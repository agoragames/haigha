'''
Copyright (c) 2011-2015, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from collections import deque
import copy

from haigha.connection import Connection
from haigha.classes.basic_class import BasicClass
from haigha.classes.exchange_class import ExchangeClass
from haigha.classes.protocol_class import ProtocolClass
from haigha.writer import Writer
from haigha.frames.method_frame import MethodFrame


class RabbitConnection(Connection):

    '''
    A connection specific to RabbitMQ that supports its extensions.
    '''

    def __init__(self, **kwargs):
        '''
        Initialize the connection
        '''
        class_map = kwargs.get('class_map', {}).copy()
        class_map.setdefault(40, RabbitExchangeClass)
        class_map.setdefault(60, RabbitBasicClass)
        class_map.setdefault(85, RabbitConfirmClass)
        kwargs['class_map'] = class_map

        # Indicate RabbitMQ-specific consumer_cancel_notify capability per
        # www.rabbitmq.com/consumer-cancel.html
        if "client_properties" in kwargs:
            client_properties = copy.deepcopy(kwargs["client_properties"])
        else:
            client_properties = dict()

        if "capabilities" not in client_properties:
            client_properties["capabilities"] = dict()

        client_capabilities = client_properties["capabilities"]

        if "consumer_cancel_notify" not in client_capabilities:
            client_capabilities["consumer_cancel_notify"] = True

        kwargs["client_properties"] = client_properties


        super(RabbitConnection, self).__init__(**kwargs)


class RabbitExchangeClass(ExchangeClass):

    '''
    Exchange class Rabbit extensions
    '''

    def __init__(self, *args, **kwargs):
        super(RabbitExchangeClass, self).__init__(*args, **kwargs)
        self.dispatch_map[31] = self._recv_bind_ok
        self.dispatch_map[51] = self._recv_unbind_ok

        self._bind_cb = deque()
        self._unbind_cb = deque()

    # I hate the code copying here. Probably a better solution, like
    # functools.
    def declare(self, exchange, type, passive=False, durable=False,
                auto_delete=True, internal=False, nowait=True,
                arguments=None, ticket=None, cb=None):
        """
        Declare the exchange.

        exchange - The name of the exchange to declare
        type - One of
        """
        nowait = nowait and self.allow_nowait() and not cb

        args = Writer()
        args.write_short(ticket or self.default_ticket).\
            write_shortstr(exchange).\
            write_shortstr(type).\
            write_bits(passive, durable, auto_delete, internal, nowait).\
            write_table(arguments or {})
        self.send_frame(MethodFrame(self.channel_id, 40, 10, args))

        if not nowait:
            self._declare_cb.append(cb)
            self.channel.add_synchronous_cb(self._recv_declare_ok)

    def bind(self, exchange, source, routing_key='', nowait=True,
             arguments={}, ticket=None, cb=None):
        '''
        Bind an exchange to another.
        '''
        nowait = nowait and self.allow_nowait() and not cb

        args = Writer()
        args.write_short(ticket or self.default_ticket).\
            write_shortstr(exchange).\
            write_shortstr(source).\
            write_shortstr(routing_key).\
            write_bit(nowait).\
            write_table(arguments or {})
        self.send_frame(MethodFrame(self.channel_id, 40, 30, args))

        if not nowait:
            self._bind_cb.append(cb)
            self.channel.add_synchronous_cb(self._recv_bind_ok)

    def _recv_bind_ok(self, _method_frame):
        '''Confirm exchange bind.'''
        cb = self._bind_cb.popleft()
        if cb:
            cb()

    def unbind(self, exchange, source, routing_key='', nowait=True,
               arguments={}, ticket=None, cb=None):
        '''
        Unbind an exchange from another.
        '''
        nowait = nowait and self.allow_nowait() and not cb

        args = Writer()
        args.write_short(ticket or self.default_ticket).\
            write_shortstr(exchange).\
            write_shortstr(source).\
            write_shortstr(routing_key).\
            write_bit(nowait).\
            write_table(arguments or {})
        self.send_frame(MethodFrame(self.channel_id, 40, 40, args))

        if not nowait:
            self._unbind_cb.append(cb)
            self.channel.add_synchronous_cb(self._recv_unbind_ok)

    def _recv_unbind_ok(self, _method_frame):
        '''Confirm exchange unbind.'''
        cb = self._unbind_cb.popleft()
        if cb:
            cb()


class RabbitBasicClass(BasicClass):

    '''
    Support Rabbit extensions to Basic class.
    '''

    def __init__(self, *args, **kwargs):
        super(RabbitBasicClass, self).__init__(*args, **kwargs)
        self.dispatch_map[30] = self._recv_cancel
        self.dispatch_map[80] = self._recv_ack
        self.dispatch_map[120] = self._recv_nack

        self._ack_listener = None
        self._nack_listener = None

        self._msg_id = 0
        self._last_ack_id = 0

        # Mapping of active consumer tags to user's consumer cancel callbacks
        self._broker_cancel_cb_map = dict()

    def _cleanup(self):
        '''
        Cleanup all the local data.
        '''
        self._ack_listener = None
        self._nack_listener = None
        self._broker_cancel_cb_map = None
        super(RabbitBasicClass, self)._cleanup()

    def set_ack_listener(self, cb):
        '''
        Set a callback for ack listening, to be used when the channel is
        in publisher confirm mode. Will be called with a single integer
        argument which is the id of the message as returned from publish().

        cb(message_id)
        '''
        self._ack_listener = cb

    def set_nack_listener(self, cb):
        '''
        Set a callbnack for nack listening, to be used when the channel is
        in publisher confirm mode. Will be called with a single integer
        argument which is the id of the message as returned from publish()
        and a boolean flag indicating if it can be requeued.

        cb(message_id, reque)
        '''
        self._nack_listener = cb

    # Probably a better solution here, like functools
    def publish(self, *args, **kwargs):
        '''
        Publish a message. Will return the id of the message if publisher
        confirmations are enabled, else will return 0.
        '''
        if self.channel.confirm._enabled:
            self._msg_id += 1
        super(RabbitBasicClass, self).publish(*args, **kwargs)
        return self._msg_id

    def _recv_ack(self, method_frame):
        '''Receive an ack from the broker.'''
        if self._ack_listener:
            delivery_tag = method_frame.args.read_longlong()
            multiple = method_frame.args.read_bit()
            if multiple:
                while self._last_ack_id < delivery_tag:
                    self._last_ack_id += 1
                    self._ack_listener(self._last_ack_id)
            else:
                self._last_ack_id = delivery_tag
                self._ack_listener(self._last_ack_id)

    def nack(self, delivery_tag, multiple=False, requeue=False):
        '''Send a nack to the broker.'''
        args = Writer()
        args.write_longlong(delivery_tag).\
            write_bits(multiple, requeue)

        self.send_frame(MethodFrame(self.channel_id, 60, 120, args))

    def _recv_nack(self, method_frame):
        '''Receive a nack from the broker.'''
        if self._nack_listener:
            delivery_tag = method_frame.args.read_longlong()
            multiple, requeue = method_frame.args.read_bits(2)
            if multiple:
                while self._last_ack_id < delivery_tag:
                    self._last_ack_id += 1
                    self._nack_listener(self._last_ack_id, requeue)
            else:
                self._last_ack_id = delivery_tag
                self._nack_listener(self._last_ack_id, requeue)

    def consume(self, *args, **kwargs):
        '''Start a queue consumer.

        Accepts the following kwarg in addition to those of BasicClass.consume:

        :param cancel_cb: a callable to be called when the broker cancels the
          consumer; e.g., when the consumer's queue is deleted. See
          www.rabbitmq.com/consumer-cancel.html.
        :type cancel_cb: callable with the signature cancel_cb(consumer_tag)
        '''
        # Register the consumer's broker-cancel-cb entry
        if "cancel_cb" in kwargs:
            cancel_cb = kwargs.pop("cancel_cb")
        else:
            cancel_cb = None

        if cancel_cb is not None:
            if not callable(cancel_cb):
                raise ValueError('cancel_cb is not callable: %r' % (cancel_cb,))

        consumer_tag = args[2] if len(args) > 2 else kwargs.get('consumer_tag')
        if not consumer_tag:
            consumer_tag = self._generate_consumer_tag()

        self._broker_cancel_cb_map[consumer_tag] = cancel_cb

        # Start consumer
        super(RabbitBasicClass, self).consume(*args, **kwargs)

    def cancel(self, *args, **kwargs):
        '''
        Cancel a consumer. Can choose to delete based on a consumer tag or
        the function which is consuming.  If deleting by function, take care
        to only use a consumer once per channel.
        '''
        consumer_tag = args[0] if len(args) > 0 else kwargs.get('consumer_tag')
        consumer = args[2] if len(args) > 2 else kwargs.get('consumer')

        # Remove the consumer's broker-cancel-cb entry
        if consumer:
            tag = self._lookup_consumer_tag_by_consumer(consumer)
            if tag:
                consumer_tag = tag

        try:
            del self._broker_cancel_cb_map[tag]
        except KeyError:
            self.logger.warning(
                'cancel: no broker-cancel-cb entry for consumer tag %r '
                '(consumer %r)', tag, consumer)

        # Cancel consumer
        super(RabbitBasicClass, self).cancel(*args, **kwargs)

    def _recv_cancel(self, method_frame):
        '''Handle Basic.Cancel from broker

        :param MethodFrame method_frame: Basic.Cancel method frame from broker
        '''
        self.logger.warning("consumer cancelled by broker: %r", method_frame)

        consumer_tag = method_frame.args.read_shortstr()
        nowait = method_frame.args.read_bit()
        if not nowait:
            # This should never happen coming from RabbitMQ broker
            self.logger.critical(
                'unexpected no-wait=False in basic.cancel from broker: %r',
                method_frame)

        # Remove consumer from this basic instance
        try:
            cancel_cb = self._broker_cancel_cb_map.pop(consumer_tag)
        except KeyError:
            # Must be a race condition between user's cancel and broker's cancel
            self.logger.warning(
                '_recv_cancel: no broker-cancel-cb entry for consumer tag %r',
                consumer_tag)
        else:
            if callable(cancel_cb):
                # Purge from base class only when user supplies cancel_cb
                self._purge_consumer_by_tag(consumer_tag)

                # Notify user
                cancel_cb(consumer_tag)


class RabbitConfirmClass(ProtocolClass):

    '''
    Implementation of Rabbit's confirm class.
    '''

    def __init__(self, *args, **kwargs):
        super(RabbitConfirmClass, self).__init__(*args, **kwargs)
        self.dispatch_map = {
            11: self._recv_select_ok,
        }

        self._enabled = False
        self._select_cb = deque()

    @property
    def name(self):
        return 'confirm'

    def select(self, nowait=True, cb=None):
        '''
        Set this channel to use publisher confirmations.
        '''
        nowait = nowait and self.allow_nowait() and not cb

        if not self._enabled:
            self._enabled = True
            self.channel.basic._msg_id = 0
            self.channel.basic._last_ack_id = 0
            args = Writer()
            args.write_bit(nowait)

            self.send_frame(MethodFrame(self.channel_id, 85, 10, args))

            if not nowait:
                self._select_cb.append(cb)
                self.channel.add_synchronous_cb(self._recv_select_ok)

    def _recv_select_ok(self, _method_frame):
        cb = self._select_cb.popleft()
        if cb:
            cb()
