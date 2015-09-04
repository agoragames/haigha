'''
Copyright (c) 2011-2015, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from collections import deque
import logging
from chai import Chai

from haigha.connections import rabbit_connection
from haigha.connections.rabbit_connection import *
from haigha.connection import Connection
from haigha.writer import Writer
from haigha.frames import *
from haigha.classes import *


class RabbitConnectionTest(Chai):

    def test_init(self):
        with expect(mock(rabbit_connection, 'super')).args(is_arg(RabbitConnection), RabbitConnection).returns(mock()) as c:
            expect(c.__init__).args(
                class_map=var('classes'),
                foo='bar',
                client_properties=var('client_props'))

        rc = RabbitConnection(foo='bar')

        assert_equals(var('classes').value, {
            40: RabbitExchangeClass,
            60: RabbitBasicClass,
            85: RabbitConfirmClass
        })

        assert_equals(
            var('client_props').value,
            {'capabilities': {'consumer_cancel_notify': True}})

    def test_init_with_user_supplied_client_capabilities(self):
        with expect(mock(rabbit_connection, 'super')).args(is_arg(RabbitConnection), RabbitConnection).returns(mock()) as c:
            expect(c.__init__).args(
                class_map=var('classes'),
                foo='bar',
                client_properties=var('client_props'))

        user_client_properties = {'my_app_version': '1.9'}

        rc = RabbitConnection(foo='bar',
                              client_properties=user_client_properties)

        assert_equals(var('classes').value, {
            40: RabbitExchangeClass,
            60: RabbitBasicClass,
            85: RabbitConfirmClass
        })

        assert_equals(
            var('client_props').value,
            {'my_app_version': '1.9',
             'capabilities': {'consumer_cancel_notify': True}})

        # Check that user's client_capabilities dict was not altered
        assert_equals({'my_app_version': '1.9'}, user_client_properties)


class RabbitExchangeClassTest(Chai):

    def setUp(self):
        super(RabbitExchangeClassTest, self).setUp()
        ch = mock()
        ch.channel_id = 42
        ch.logger = mock()
        self.klass = RabbitExchangeClass(ch)

    def test_init(self):
        assert_equals(self.klass.dispatch_map[31], self.klass._recv_bind_ok)
        assert_equals(self.klass.dispatch_map[51], self.klass._recv_unbind_ok)
        assert_equals(deque(), self.klass._bind_cb)
        assert_equals(deque(), self.klass._unbind_cb)

    def test_declare_default_args(self):
        w = mock()
        expect(self.klass.allow_nowait).returns(True)
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_short).args(self.klass.default_ticket).returns(w)
        expect(w.write_shortstr).args('exchange').returns(w)
        expect(w.write_shortstr).args('topic').returns(w)
        expect(w.write_bits).args(False, False, True, False, True).returns(w)
        expect(w.write_table).args({})
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 40, 10, w).returns('frame')
        expect(self.klass.send_frame).args('frame')
        stub(self.klass.channel.add_synchronous_cb)

        self.klass.declare('exchange', 'topic')
        assert_equals(deque(), self.klass._declare_cb)

    def test_declare_with_args(self):
        w = mock()
        stub(self.klass.allow_nowait)
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_short).args('t').returns(w)
        expect(w.write_shortstr).args('exchange').returns(w)
        expect(w.write_shortstr).args('topic').returns(w)
        expect(w.write_bits).args('p', 'd', 'ad', 'yes', False).returns(w)
        expect(w.write_table).args('table')
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 40, 10, w).returns('frame')
        expect(self.klass.send_frame).args('frame')
        expect(self.klass.channel.add_synchronous_cb).args(
            self.klass._recv_declare_ok)

        self.klass.declare('exchange', 'topic', passive='p', durable='d',
                           nowait=False, arguments='table', ticket='t',
                           auto_delete='ad', internal='yes')
        assert_equals(deque([None]), self.klass._declare_cb)

    def test_declare_with_cb(self):
        w = mock()
        expect(self.klass.allow_nowait).returns(True)
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_short).args('t').returns(w)
        expect(w.write_shortstr).args('exchange').returns(w)
        expect(w.write_shortstr).args('topic').returns(w)
        expect(w.write_bits).args('p', 'd', True, False, False).returns(w)
        expect(w.write_table).args('table')
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 40, 10, w).returns('frame')
        expect(self.klass.send_frame).args('frame')
        expect(self.klass.channel.add_synchronous_cb).args(
            self.klass._recv_declare_ok)

        self.klass.declare('exchange', 'topic', passive='p', durable='d',
                           nowait=True, arguments='table', ticket='t', cb='foo')
        assert_equals(deque(['foo']), self.klass._declare_cb)

    def test_bind_default_args(self):
        w = mock()

        expect(self.klass.allow_nowait).returns(True)
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_short).args(0).returns(w)
        expect(w.write_shortstr).args('destination').returns(w)
        expect(w.write_shortstr).args('source').returns(w)
        expect(w.write_shortstr).args('').returns(w)
        expect(w.write_bit).args(True).returns(w)
        expect(w.write_table).args({})
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 40, 30, w).returns('frame')
        expect(self.klass.send_frame).args('frame')

        self.klass.bind('destination', 'source')
        assert_equals(deque(), self.klass._bind_cb)

    def test_bind_with_args(self):
        w = mock()

        stub(self.klass.allow_nowait)
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_short).args('t').returns(w)
        expect(w.write_shortstr).args('destination').returns(w)
        expect(w.write_shortstr).args('source').returns(w)
        expect(w.write_shortstr).args('route').returns(w)
        expect(w.write_bit).args(False).returns(w)
        expect(w.write_table).args('table')
        expect(self.klass.channel.add_synchronous_cb).args(
            self.klass._recv_bind_ok)
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 40, 30, w).returns('frame')
        expect(self.klass.send_frame).args('frame')

        self.klass.bind('destination', 'source', routing_key='route',
                        ticket='t', nowait=False, arguments='table')
        assert_equals(deque([None]), self.klass._bind_cb)

    def test_bind_with_cb(self):
        w = mock()

        expect(self.klass.allow_nowait).returns(True)
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_short).args('t').returns(w)
        expect(w.write_shortstr).args('destination').returns(w)
        expect(w.write_shortstr).args('source').returns(w)
        expect(w.write_shortstr).args('route').returns(w)
        expect(w.write_bit).args(False).returns(w)
        expect(w.write_table).args('table')
        expect(self.klass.channel.add_synchronous_cb).args(
            self.klass._recv_bind_ok)
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 40, 30, w).returns('frame')
        expect(self.klass.send_frame).args('frame')

        self.klass.bind('destination', 'source', routing_key='route',
                        ticket='t', arguments='table', cb='foo')
        assert_equals(deque(['foo']), self.klass._bind_cb)

    def test_recv_bind_ok_no_cb(self):
        self.klass._bind_cb = deque([None])
        self.klass._recv_bind_ok('frame')
        assert_equals(deque(), self.klass._bind_cb)

    def test_recv_bind_ok_with_cb(self):
        cb = mock()
        self.klass._bind_cb = deque([cb])
        expect(cb)
        self.klass._recv_bind_ok('frame')
        assert_equals(deque(), self.klass._bind_cb)

    def test_unbind_default_args(self):
        w = mock()

        expect(self.klass.allow_nowait).returns(True)
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_short).args(0).returns(w)
        expect(w.write_shortstr).args('destination').returns(w)
        expect(w.write_shortstr).args('source').returns(w)
        expect(w.write_shortstr).args('').returns(w)
        expect(w.write_bit).args(True).returns(w)
        expect(w.write_table).args({})
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 40, 40, w).returns('frame')
        expect(self.klass.send_frame).args('frame')

        self.klass.unbind('destination', 'source')
        assert_equals(deque(), self.klass._unbind_cb)

    def test_unbind_with_args(self):
        w = mock()

        stub(self.klass.allow_nowait)
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_short).args('t').returns(w)
        expect(w.write_shortstr).args('destination').returns(w)
        expect(w.write_shortstr).args('source').returns(w)
        expect(w.write_shortstr).args('route').returns(w)
        expect(w.write_bit).args(False).returns(w)
        expect(w.write_table).args('table')
        expect(self.klass.channel.add_synchronous_cb).args(
            self.klass._recv_unbind_ok)
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 40, 40, w).returns('frame')
        expect(self.klass.send_frame).args('frame')

        self.klass.unbind('destination', 'source', routing_key='route',
                          ticket='t', nowait=False, arguments='table')
        assert_equals(deque([None]), self.klass._unbind_cb)

    def test_unbind_with_cb(self):
        w = mock()

        expect(self.klass.allow_nowait).returns(True)
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_short).args('t').returns(w)
        expect(w.write_shortstr).args('destination').returns(w)
        expect(w.write_shortstr).args('source').returns(w)
        expect(w.write_shortstr).args('route').returns(w)
        expect(w.write_bit).args(False).returns(w)
        expect(w.write_table).args('table')
        expect(self.klass.channel.add_synchronous_cb).args(
            self.klass._recv_unbind_ok)
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 40, 40, w).returns('frame')
        expect(self.klass.send_frame).args('frame')

        self.klass.unbind('destination', 'source', routing_key='route',
                          ticket='t', arguments='table', cb='foo')
        assert_equals(deque(['foo']), self.klass._unbind_cb)

    def test_recv_unbind_ok_no_cb(self):
        self.klass._unbind_cb = deque([None])
        self.klass._recv_unbind_ok('frame')
        assert_equals(deque(), self.klass._unbind_cb)

    def test_recv_unbind_ok_with_cb(self):
        cb = mock()
        self.klass._unbind_cb = deque([cb])
        expect(cb)
        self.klass._recv_unbind_ok('frame')
        assert_equals(deque(), self.klass._unbind_cb)


class RabbitBasicClassTest(Chai):

    def setUp(self):
        super(RabbitBasicClassTest, self).setUp()
        ch = mock()
        ch.channel_id = 42
        ch.logger = mock()
        self.klass = RabbitBasicClass(ch)

    def test_init(self):
        assert_equals(self.klass.dispatch_map[80], self.klass._recv_ack)
        assert_equals(self.klass.dispatch_map[120], self.klass._recv_nack)
        assert_equals(None, self.klass._ack_listener)
        assert_equals(None, self.klass._nack_listener)
        assert_equals(0, self.klass._msg_id)
        assert_equals(0, self.klass._last_ack_id)

    def test_cleanup(self):
        with expect(mock(rabbit_connection, 'super')).args(is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as c:
            expect(c._cleanup).args()

        self.klass._cleanup()
        assert_equals(None, self.klass._ack_listener)
        assert_equals(None, self.klass._nack_listener)
        assert_equals(None, self.klass._broker_cancel_cb_map)

    def test_set_ack_listener(self):
        self.klass.set_ack_listener('foo')
        assert_equals('foo', self.klass._ack_listener)

    def test_set_nack_listener(self):
        self.klass.set_nack_listener('foo')
        assert_equals('foo', self.klass._nack_listener)

    def test_publish_when_not_confirming(self):
        self.klass.channel.confirm._enabled = False
        with expect(mock(rabbit_connection, 'super')).args(
                is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.publish).args('a', 'b', c='d')

        assert_equals(0, self.klass.publish('a', 'b', c='d'))
        assert_equals(0, self.klass._msg_id)

    def test_publish_when_confirming(self):
        self.klass.channel.confirm._enabled = True
        with expect(mock(rabbit_connection, 'super')).args(
                is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.publish).args('a', 'b', c='d')

        assert_equals(1, self.klass.publish('a', 'b', c='d'))
        assert_equals(1, self.klass._msg_id)

    def test_recv_ack_no_listener(self):
        self.klass._recv_ack('frame')

    def test_recv_ack_with_listener_single_msg(self):
        self.klass._ack_listener = mock()
        frame = mock()
        expect(frame.args.read_longlong).returns(42)
        expect(frame.args.read_bit).returns(False)
        expect(self.klass._ack_listener).args(42)

        self.klass._recv_ack(frame)
        assert_equals(42, self.klass._last_ack_id)

    def test_recv_ack_with_listener_multiple_msg(self):
        self.klass._ack_listener = mock()
        self.klass._last_ack_id = 40
        frame = mock()
        expect(frame.args.read_longlong).returns(42)
        expect(frame.args.read_bit).returns(True)
        expect(self.klass._ack_listener).args(41)
        expect(self.klass._ack_listener).args(42)

        self.klass._recv_ack(frame)
        assert_equals(42, self.klass._last_ack_id)

    def test_nack_default_args(self):
        w = mock()
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_longlong).args(8675309).returns(w)
        expect(w.write_bits).args(False, False)
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 60, 120, w).returns('frame')
        expect(self.klass.send_frame).args('frame')

        self.klass.nack(8675309)

    def test_nack_with_args(self):
        w = mock()
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(w.write_longlong).args(8675309).returns(w)
        expect(w.write_bits).args('many', 'sure')
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 60, 120, w).returns('frame')
        expect(self.klass.send_frame).args('frame')

        self.klass.nack(8675309, multiple='many', requeue='sure')

    def test_recv_nack_no_listener(self):
        self.klass._recv_nack('frame')

    def test_recv_nack_with_listener_single_msg(self):
        self.klass._nack_listener = mock()
        frame = mock()
        expect(frame.args.read_longlong).returns(42)
        expect(frame.args.read_bits).args(2).returns((False, False))
        expect(self.klass._nack_listener).args(42, False)

        self.klass._recv_nack(frame)
        assert_equals(42, self.klass._last_ack_id)

    def test_recv_nack_with_listener_multiple_msg(self):
        self.klass._nack_listener = mock()
        self.klass._last_ack_id = 40
        frame = mock()
        expect(frame.args.read_longlong).returns(42)
        expect(frame.args.read_bits).args(2).returns((True, True))
        expect(self.klass._nack_listener).args(41, True)
        expect(self.klass._nack_listener).args(42, True)

        self.klass._recv_nack(frame)
        assert_equals(42, self.klass._last_ack_id)

    def test_consume_with_default_args(self):
        consumer = mock()
        with expect(mock(rabbit_connection, 'super')).args(
                is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.consume).args(
                'queue', consumer, 'ctag', False, True, False, True, None, None)

        expect(self.klass._generate_consumer_tag).args().returns('ctag')

        self.klass.consume('queue', consumer)
        assert_equals({'ctag': None}, self.klass._broker_cancel_cb_map)

    def test_consume_with_cancel_cb(self):
        consumer = mock()
        cancel_cb = mock()
        with expect(mock(rabbit_connection, 'super')).args(
                is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.consume).args(
                'queue', consumer, 'ctag', False, True, False, True, None, None)

        expect(self.klass._generate_consumer_tag).args().returns('ctag')

        self.klass.consume('queue', consumer, cancel_cb=cancel_cb)
        assert_equals({'ctag': cancel_cb}, self.klass._broker_cancel_cb_map)

    def test_consume_with_consumer_tag(self):
        consumer = mock()
        with expect(mock(rabbit_connection, 'super')).args(
                is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.consume).args(
                'queue', consumer, 'user-ctag',
                False, True, False, True, None, None)

        expect(self.klass._generate_consumer_tag).times(0)

        self.klass.consume('queue', consumer, 'user-ctag')

    def test_consume_with_invalid_cancel_cb(self):
        assert_raises(
            ValueError,
            self.klass.consume, 'queue', mock(), cancel_cb='not-callable')
        assert_equals({}, self.klass._broker_cancel_cb_map)

    def test_cancel_with_default_args(self):
        with expect(mock(rabbit_connection, 'super')).args(
            is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.cancel).args(
                '', True, None, None)

        expect(self.klass.logger.warning).args(
            'cancel: no broker-cancel-cb entry for consumer tag %r '
            '(consumer %r)', '', None)

        self.klass.cancel()

    def test_cancel_by_consumer_cb(self):
        consumer = mock()
        with expect(mock(rabbit_connection, 'super')).args(
            is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.cancel).args(
                'ctag', True, consumer, None)

        self.klass._broker_cancel_cb_map['ctag'] = mock(name='cancel_cb')

        expect(self.klass._lookup_consumer_tag_by_consumer).args(consumer).returns('ctag')

        self.klass.cancel(consumer=consumer)

    def test_cancel_by_consumer_cb_not_found(self):
        consumer = mock()
        with expect(mock(rabbit_connection, 'super')).args(
            is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.cancel).args(
                '', True, consumer, None)

        expect(self.klass._lookup_consumer_tag_by_consumer).args(consumer).returns(None)
        expect(self.klass.logger.warning).args(
            'cancel: no broker-cancel-cb entry for consumer tag %r '
            '(consumer %r)', '', consumer)

        self.klass.cancel(consumer=consumer)

    def test_cancel_by_consumer_tag(self):
        with expect(mock(rabbit_connection, 'super')).args(
            is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.cancel).args(
                'ctag', True, None, None)

        self.klass._broker_cancel_cb_map['ctag'] = mock(name='cancel_cb')

        expect(self.klass._lookup_consumer_tag_by_consumer).times(0)

        self.klass.cancel(consumer_tag='ctag')

    def test_cancel_by_consumer_tag_with_cancel_cb_not_found(self):
        with expect(mock(rabbit_connection, 'super')).args(
            is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
            expect(klass.cancel).args(
                'ctag', True, None, None)

        expect(self.klass._lookup_consumer_tag_by_consumer).times(0)
        expect(self.klass.logger.warning).args(
            'cancel: no broker-cancel-cb entry for consumer tag %r '
            '(consumer %r)', 'ctag', None)

        self.klass.cancel(consumer_tag='ctag')

    def test_recv_cancel(self):
        cancel_cb = mock()
        self.klass._broker_cancel_cb_map['ctag'] = cancel_cb
        frame = mock()

        expect(self.klass.logger.warning).args(
            'consumer cancelled by broker: %r', frame)

        expect(frame.args.read_shortstr).returns('ctag')

        expect(self.klass._purge_consumer_by_tag).args('ctag')

        expect(cancel_cb).args('ctag')

        self.klass._recv_cancel(frame)

        assert_equals({}, self.klass._broker_cancel_cb_map)

    def test_recv_cancel_with_cancel_cb_not_found(self):
        frame = mock()

        expect(self.klass.logger.warning).args(
            'consumer cancelled by broker: %r', frame)

        expect(frame.args.read_shortstr).returns('ctag')

        expect(self.klass.logger.warning).args(
            '_recv_cancel: no broker-cancel-cb entry for consumer tag %r',
            'ctag')

        expect(self.klass._purge_consumer_by_tag).times(0)

        self.klass._recv_cancel(frame)

    def test_recv_cancel_with_cancel_cb_not_callable(self):
        self.klass._broker_cancel_cb_map['ctag'] = None
        frame = mock()

        expect(self.klass.logger.warning).args(
            'consumer cancelled by broker: %r', frame)

        expect(frame.args.read_shortstr).returns('ctag')

        expect(self.klass._purge_consumer_by_tag).times(0)

        self.klass._recv_cancel(frame)

        assert_equals({}, self.klass._broker_cancel_cb_map)

class RabbitConfirmClassTest(Chai):

    def setUp(self):
        super(RabbitConfirmClassTest, self).setUp()
        ch = mock()
        ch.channel_id = 42
        ch.logger = mock()
        self.klass = RabbitConfirmClass(ch)

    def test_init(self):
        assert_equals(
            {11: self.klass._recv_select_ok}, self.klass.dispatch_map)
        assert_false(self.klass._enabled)
        assert_equals(deque(), self.klass._select_cb)

    def test_name(self):
        assert_equals('confirm', self.klass.name)

    def test_select_when_not_enabled_and_no_cb(self):
        self.klass._enabled = False
        w = mock()
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(self.klass.allow_nowait).returns(True)
        expect(w.write_bit).args(True)
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 85, 10, w).returns('frame')
        expect(self.klass.send_frame).args('frame')
        stub(self.klass.channel.add_synchronous_cb)

        self.klass.select()
        assert_true(self.klass._enabled)
        assert_equals(deque(), self.klass._select_cb)

    def test_select_when_not_enabled_and_no_cb_but_synchronous(self):
        self.klass._enabled = False
        w = mock()
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        stub(self.klass.allow_nowait)
        expect(w.write_bit).args(False)
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 85, 10, w).returns('frame')
        expect(self.klass.send_frame).args('frame')
        expect(self.klass.channel.add_synchronous_cb).args(
            self.klass._recv_select_ok)

        self.klass.select(nowait=False)
        assert_true(self.klass._enabled)
        assert_equals(deque([None]), self.klass._select_cb)

    def test_select_when_not_enabled_with_cb(self):
        self.klass._enabled = False
        w = mock()
        expect(mock(rabbit_connection, 'Writer')).returns(w)
        expect(self.klass.allow_nowait).returns(True)
        expect(w.write_bit).args(False)
        expect(mock(rabbit_connection, 'MethodFrame')).args(
            42, 85, 10, w).returns('frame')
        expect(self.klass.send_frame).args('frame')
        expect(self.klass.channel.add_synchronous_cb).args(
            self.klass._recv_select_ok)

        self.klass.select(cb='foo')
        assert_true(self.klass._enabled)
        assert_equals(deque(['foo']), self.klass._select_cb)

    def test_select_when_already_enabled(self):
        self.klass._enabled = True
        stub(self.klass.allow_nowait)
        stub(self.klass.send_frame)
        expect(self.klass.allow_nowait).returns(True)

        assert_equals(deque(), self.klass._select_cb)
        self.klass.select()
        assert_equals(deque(), self.klass._select_cb)

    def test_recv_select_ok_no_cb(self):
        self.klass._select_cb = deque([None])
        self.klass._recv_select_ok('frame')
        assert_equals(deque(), self.klass._select_cb)

    def test_recv_select_ok_with_cb(self):
        cb = mock()
        self.klass._select_cb = deque([cb])
        expect(cb)
        self.klass._recv_select_ok('frame')
        assert_equals(deque(), self.klass._select_cb)
