'''
Copyright (c) 2011-2015, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from collections import deque

from haigha.message import Message
from haigha.writer import Writer
from haigha.frames.method_frame import MethodFrame
from haigha.frames.header_frame import HeaderFrame
from haigha.frames.content_frame import ContentFrame
from haigha.classes.protocol_class import ProtocolClass


class BasicClass(ProtocolClass):

    '''
    Implements the AMQP Basic class
    '''

    def __init__(self, *args, **kwargs):
        super(BasicClass, self).__init__(*args, **kwargs)
        self.dispatch_map = {
            11: self._recv_qos_ok,
            21: self._recv_consume_ok,
            31: self._recv_cancel_ok,
            50: self._recv_return,
            60: self._recv_deliver,
            71: self._recv_get_response,   # see impl
            72: self._recv_get_response,   # see impl
            111: self._recv_recover_ok,
        }

        self._consumer_tag_id = 0
        self._pending_consumers = deque()
        self._consumer_cb = {}
        self._get_cb = deque()
        self._recover_cb = deque()
        self._cancel_cb = deque()
        self._return_listener = None

    @property
    def name(self):
        return 'basic'

    def _cleanup(self):
        '''
        Cleanup all the local data.
        '''
        self._pending_consumers = None
        self._consumer_cb = None
        self._get_cb = None
        self._recover_cb = None
        self._cancel_cb = None
        self._return_listener = None
        super(BasicClass, self)._cleanup()

    def set_return_listener(self, cb):
        '''
        Set a callback for basic.return listening. Will be called with a single
        Message argument.

        The return_info attribute of the Message will have the following
        properties:
            'channel': Channel instance
            'reply_code': reply code (int)
            'reply_text': reply text
            'exchange': exchange name
            'routing_key': routing key

        RabbitMQ NOTE: if the channel was in confirmation mode when the message
        was published, then basic.return will still be followed by basic.ack
        later.

        :param cb: callable cb(Message); pass None to reset
        '''
        if cb is not None and not callable(cb):
            raise ValueError('return_listener callback must either be None or '
                             'a callable, but got: %r' % (cb,))
        self._return_listener = cb

    def _generate_consumer_tag(self):
        '''
        Generate the next consumer tag.

        The consumer tag is local to a channel, so two clients can use the
        same consumer tags.

        NOTE: this protected method may be called by derived classes
        '''
        self._consumer_tag_id += 1
        return "channel-%d-%d" % (self.channel_id, self._consumer_tag_id)

    def qos(self, prefetch_size=0, prefetch_count=0, is_global=False):
        '''
        Set QoS on this channel.
        '''
        args = Writer()
        args.write_long(prefetch_size).\
            write_short(prefetch_count).\
            write_bit(is_global)
        self.send_frame(MethodFrame(self.channel_id, 60, 10, args))

        self.channel.add_synchronous_cb(self._recv_qos_ok)

    def _recv_qos_ok(self, _method_frame):
        # No arguments, nothing to do
        pass

    def consume(self, queue, consumer, consumer_tag='', no_local=False,
                no_ack=True, exclusive=False, nowait=True, ticket=None,
                cb=None):
        '''
        Start a queue consumer. If `cb` is supplied, will be called when
        broker confirms that consumer is registered.
        '''
        nowait = nowait and self.allow_nowait() and not cb

        if nowait and consumer_tag == '':
            consumer_tag = self._generate_consumer_tag()

        args = Writer()
        args.write_short(ticket or self.default_ticket).\
            write_shortstr(queue).\
            write_shortstr(consumer_tag).\
            write_bits(no_local, no_ack, exclusive, nowait).\
            write_table({})  # unused according to spec
        self.send_frame(MethodFrame(self.channel_id, 60, 20, args))

        if not nowait:
            self._pending_consumers.append((consumer, cb))
            self.channel.add_synchronous_cb(self._recv_consume_ok)
        else:
            self._consumer_cb[consumer_tag] = consumer

    def _recv_consume_ok(self, method_frame):
        consumer_tag = method_frame.args.read_shortstr()
        consumer, cb = self._pending_consumers.popleft()

        self._consumer_cb[consumer_tag] = consumer
        if cb:
            cb()

    def cancel(self, consumer_tag='', nowait=True, consumer=None, cb=None):
        '''
        Cancel a consumer. Can choose to delete based on a consumer tag or
        the function which is consuming.  If deleting by function, take care
        to only use a consumer once per channel.
        '''
        if consumer:
            tag = self._lookup_consumer_tag_by_consumer(consumer)
            if tag:
                consumer_tag = tag

        nowait = nowait and self.allow_nowait() and not cb

        args = Writer()
        args.write_shortstr(consumer_tag).\
            write_bit(nowait)
        self.send_frame(MethodFrame(self.channel_id, 60, 30, args))

        if not nowait:
            self._cancel_cb.append(cb)
            self.channel.add_synchronous_cb(self._recv_cancel_ok)
        else:
            self._purge_consumer_by_tag(consumer_tag)

    def _lookup_consumer_tag_by_consumer(self, consumer):
        '''Look up consumer tag given its consumer function

        NOTE: this protected method may be called by derived classes

        :param callable consumer: consumer function

        :returns: matching consumer tag or None
        :rtype: str or None
        '''
        for (tag, func) in self._consumer_cb.iteritems():
            if func == consumer:
                return tag

    def _purge_consumer_by_tag(self, consumer_tag):
        '''Purge consumer entry from this basic instance

        NOTE: this protected method may be called by derived classes

        :param str consumer_tag:
        '''
        try:
            del self._consumer_cb[consumer_tag]
        except KeyError:
            self.logger.warning(
                'no callback registered for consumer tag " %s "', consumer_tag)
        else:
            self.logger.info('purged consumer with tag " %s "', consumer_tag)


    def _recv_cancel_ok(self, method_frame):
        consumer_tag = method_frame.args.read_shortstr()
        self._purge_consumer_by_tag(consumer_tag)

        cb = self._cancel_cb.popleft()
        if cb:
            cb()

    def publish(self, msg, exchange, routing_key, mandatory=False,
                immediate=False, ticket=None):
        '''
        publish a message.
        '''
        args = Writer()
        args.write_short(ticket or self.default_ticket).\
            write_shortstr(exchange).\
            write_shortstr(routing_key).\
            write_bits(mandatory, immediate)

        self.send_frame(MethodFrame(self.channel_id, 60, 40, args))
        self.send_frame(
            HeaderFrame(self.channel_id, 60, 0, len(msg), msg.properties))

        f_max = self.channel.connection.frame_max
        for f in ContentFrame.create_frames(self.channel_id, msg.body, f_max):
            self.send_frame(f)

    def return_msg(self, reply_code, reply_text, exchange, routing_key):
        '''
        Return a failed message.  Not named "return" because python interpreter
        can't deal with that.
        '''
        args = Writer()
        args.write_short(reply_code).\
            write_shortstr(reply_text).\
            write_shortstr(exchange).\
            write_shortstr(routing_key)

        self.send_frame(MethodFrame(self.channel_id, 60, 50, args))

    def _recv_return(self, method_frame):
        '''
        Handle basic.return method. If we have a complete message, will call the
        user's return listener callabck (if any). If there are not enough
        frames, will re-queue current frames and raise a FrameUnderflow

        NOTE: if the channel was in confirmation mode when the message was
        published, then this will still be followed by basic.ack later
        '''
        msg = self._read_returned_msg(method_frame)

        if callable(self._return_listener):
            self._return_listener(msg)
        else:
            self.logger.error(
                "Published message returned by broker: info=%s, properties=%s",
                msg.return_info, msg.properties)

    def _recv_deliver(self, method_frame):
        msg = self._read_msg(method_frame,
                             with_consumer_tag=True, with_message_count=False)

        func = self._consumer_cb.get(msg.delivery_info['consumer_tag'], None)
        if func:
            func(msg)

    def get(self, queue, consumer=None, no_ack=True, ticket=None):
        '''
        Ask to fetch a single message from a queue.  If a consumer is supplied,
        the consumer will be called with either a Message argument, or None if
        there is no message in queue. If a synchronous transport, Message or
        None is returned.
        '''
        args = Writer()
        args.write_short(ticket or self.default_ticket).\
            write_shortstr(queue).\
            write_bit(no_ack)

        self._get_cb.append(consumer)
        self.send_frame(MethodFrame(self.channel_id, 60, 70, args))
        return self.channel.add_synchronous_cb(self._recv_get_response)

    def _recv_get_response(self, method_frame):
        '''
        Handle either get_ok or get_empty. This is a hack because the
        synchronous callback stack is expecting one method to satisfy the
        expectation. To keep that loop as tight as possible, work within
        those constraints. Use of get is not recommended anyway.
        '''
        if method_frame.method_id == 71:
            return self._recv_get_ok(method_frame)
        elif method_frame.method_id == 72:
            return self._recv_get_empty(method_frame)

    def _recv_get_ok(self, method_frame):
        msg = self._read_msg(method_frame,
                             with_consumer_tag=False, with_message_count=True)
        cb = self._get_cb.popleft()
        if cb:
            cb(msg)
        return msg

    def _recv_get_empty(self, _method_frame):
        # On empty, call back with None as the argument so that user code knows
        # it's empty and can take next action
        cb = self._get_cb.popleft()
        if cb:
            cb(None)

    def ack(self, delivery_tag, multiple=False):
        '''
        Acknowledge delivery of a message.  If multiple=True, acknowledge up-to
        and including delivery_tag.
        '''
        args = Writer()
        args.write_longlong(delivery_tag).\
            write_bit(multiple)

        self.send_frame(MethodFrame(self.channel_id, 60, 80, args))

    def reject(self, delivery_tag, requeue=False):
        '''
        Reject a message.
        '''
        args = Writer()
        args.write_longlong(delivery_tag).\
            write_bit(requeue)

        self.send_frame(MethodFrame(self.channel_id, 60, 90, args))

    def recover_async(self, requeue=False):
        '''
        Redeliver all unacknowledged messages on this channel.

        This method is deprecated in favour of the synchronous
        recover/recover-ok
        '''
        args = Writer()
        args.write_bit(requeue)

        self.send_frame(MethodFrame(self.channel_id, 60, 100, args))

    def recover(self, requeue=False, cb=None):
        '''
        Ask server to redeliver all unacknowledged messages.
        '''
        args = Writer()
        args.write_bit(requeue)

        # The XML spec is incorrect; this method is always synchronous
        #  http://lists.rabbitmq.com/pipermail/rabbitmq-discuss/2011-January/010738.html
        self._recover_cb.append(cb)
        self.send_frame(MethodFrame(self.channel_id, 60, 110, args))
        self.channel.add_synchronous_cb(self._recv_recover_ok)

    def _recv_recover_ok(self, _method_frame):
        cb = self._recover_cb.popleft()
        if cb:
            cb()

    def _read_msg(self, method_frame, with_consumer_tag=False,
                  with_message_count=False):
        '''
        Support method to read a Message from the current frame buffer.
        Will return a Message, or re-queue current frames and raise a
        FrameUnderflow. Takes an optional argument on whether to read the
        consumer tag so it can be used for both deliver and get-ok.
        '''
        header_frame, body = self._reap_msg_frames(method_frame)

        if with_consumer_tag:
            consumer_tag = method_frame.args.read_shortstr()
        delivery_tag = method_frame.args.read_longlong()
        redelivered = method_frame.args.read_bit()
        exchange = method_frame.args.read_shortstr()
        routing_key = method_frame.args.read_shortstr()
        if with_message_count:
            message_count = method_frame.args.read_long()

        delivery_info = {
            'channel': self.channel,
            'delivery_tag': delivery_tag,
            'redelivered': redelivered,
            'exchange': exchange,
            'routing_key': routing_key,
        }
        if with_consumer_tag:
            delivery_info['consumer_tag'] = consumer_tag
        if with_message_count:
            delivery_info['message_count'] = message_count

        return Message(body=body, delivery_info=delivery_info,
                       **header_frame.properties)

    def _read_returned_msg(self, method_frame):
        '''
        Support method to read a returned (basic.return) Message from the
        current frame buffer. Will return a Message with return_info, or
        re-queue current frames and raise a FrameUnderflow.

        :returns: Message with the return_info attribute set, where return_info
          is a dict with the following properties:
            'channel': Channel instance
            'reply_code': reply code (int)
            'reply_text': reply text
            'exchange': exchange name
            'routing_key': routing key
        '''
        header_frame, body = self._reap_msg_frames(method_frame)

        return_info = {
            'channel': self.channel,
            'reply_code': method_frame.args.read_short(),
            'reply_text': method_frame.args.read_shortstr(),
            'exchange': method_frame.args.read_shortstr(),
            'routing_key': method_frame.args.read_shortstr()
        }

        return Message(body=body, return_info=return_info,
                       **header_frame.properties)

    def _reap_msg_frames(self, method_frame):
        '''
        Support method to reap header frame and body from current frame buffer.
        Used in processing of basic.return, basic.deliver, and basic.get_ok.
        Will return a pair (<header frame>, <body>), or re-queue current frames
        and raise a FrameUnderflow.

        :returns: pair (<header frame>, <body>)
        :rtype: tuple of (HeaderFrame, bytearray)
        '''
        # No need to assert that is instance of Header or Content frames
        # because failure to access as such will result in exception that
        # channel will pick up and handle accordingly.
        header_frame = self.channel.next_frame()
        if header_frame:
            size = header_frame.size
            body = bytearray()
            rbuf_frames = deque([header_frame, method_frame])

            while len(body) < size:
                content_frame = self.channel.next_frame()
                if content_frame:
                    rbuf_frames.appendleft(content_frame)
                    body.extend(content_frame.payload.buffer())
                else:
                    self.channel.requeue_frames(rbuf_frames)
                    raise self.FrameUnderflow()
        else:
            self.channel.requeue_frames([method_frame])
            raise self.FrameUnderflow()

        return (header_frame, body)
