'''
Copyright (c) 2011-2015, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
from collections import deque

from haigha import channel
from haigha.channel import Channel, SyncWrapper
from haigha.exceptions import ChannelError, ChannelClosed, ConnectionClosed
from haigha.classes.basic_class import BasicClass
from haigha.classes.channel_class import ChannelClass
from haigha.classes.exchange_class import ExchangeClass
from haigha.classes.queue_class import QueueClass
from haigha.classes.transaction_class import TransactionClass
from haigha.classes.protocol_class import ProtocolClass
from haigha.frames.method_frame import MethodFrame
from haigha.frames.heartbeat_frame import HeartbeatFrame
from haigha.frames.header_frame import HeaderFrame
from haigha.frames.content_frame import ContentFrame


class SyncWrapperTest(Chai):

    def test_init(self):
        s = SyncWrapper('cb')
        assert_equals('cb', s._cb)
        assert_true(s._read)
        assert_equals(None, s._result)

    def test_eq_when_other_is_same_cb(self):
        s = SyncWrapper('cb')
        assert_equals('cb', s)
        assert_not_equals('bb', s)

    def test_eq_when_other_has_same_cb(self):
        s = SyncWrapper('cb')
        other = SyncWrapper('cb')
        another = SyncWrapper('bb')

        assert_equals(s, other)
        assert_not_equals(s, another)

    def test_call(self):
        cb = mock()
        s = SyncWrapper(cb)

        expect(cb).args('foo', 'bar', hello='mars')
        s('foo', 'bar', hello='mars')
        assert_false(s._read)


class ChannelTest(Chai):

    def test_init(self):
        connection = mock()
        c = Channel(connection, 'id', {
            20: ChannelClass,
            40: ExchangeClass,
            50: QueueClass,
            60: BasicClass,
            90: TransactionClass,
        })
        assert_equals(connection, c._connection)
        assert_equals('id', c._channel_id)
        assert_true(isinstance(c.channel, ChannelClass))
        assert_true(isinstance(c.exchange, ExchangeClass))
        assert_true(isinstance(c.queue, QueueClass))
        assert_true(isinstance(c.basic, BasicClass))
        assert_true(isinstance(c.tx, TransactionClass))
        assert_false(c._synchronous)
        assert_equals(c._class_map[20], c.channel)
        assert_equals(c._class_map[40], c.exchange)
        assert_equals(c._class_map[50], c.queue)
        assert_equals(c._class_map[60], c.basic)
        assert_equals(c._class_map[90], c.tx)
        assert_equals(deque([]), c._pending_events)
        assert_equals(deque([]), c._frame_buffer)
        assert_equals(set([]), c._open_listeners)
        assert_equals(set([]), c._close_listeners)
        assert_false(c._closed)
        assert_equals(
            {
                'reply_code': 0,
                'reply_text': 'first connect',
                'class_id': 0,
                'method_id': 0
            }, c._close_info)
        assert_true(c._active)

        c = Channel(connection, 'id', {
            20: ChannelClass,
            40: ExchangeClass,
            50: QueueClass,
            60: BasicClass,
            90: TransactionClass,
        }, synchronous=True)
        assert_true(c._synchronous)

    def test_properties(self):
        connection = mock()
        connection.logger = 'logger'
        connection.synchronous = False

        c = Channel(connection, 'id', {})
        c._closed = 'yes'
        c._close_info = 'ithappened'
        c._active = 'record'

        assert_equals(connection, c.connection)
        assert_equals('id', c.channel_id)
        assert_equals('logger', c.logger)
        assert_equals('yes', c.closed)
        assert_equals('ithappened', c.close_info)
        assert_equals('record', c.active)
        assert_false(c.synchronous)

        c._closed = False
        assert_equals(None, c.close_info)

        connection.synchronous = False
        c = Channel(connection, 'id', {}, synchronous=True)
        assert_true(c.synchronous)

        connection.synchronous = True
        c = Channel(connection, 'id', {})
        assert_true(c.synchronous)

        connection.synchronous = True
        c = Channel(connection, 'id', {}, synchronous=False)
        assert_true(c.synchronous)

    def test_add_open_listener(self):
        c = Channel(mock(), None, {})
        c.add_open_listener('foo')
        assert_equals(set(['foo']), c._open_listeners)

    def test_remove_open_listener(self):
        c = Channel(mock(), None, {})
        c.add_open_listener('foo')
        c.remove_open_listener('foo')
        c.remove_open_listener('bar')
        assert_equals(set([]), c._open_listeners)

    def test_notify_open_listeners(self):
        c = Channel(mock(), None, {})
        cb1 = mock()
        cb2 = mock()
        c._open_listeners = set([cb1, cb2])
        expect(cb1).args(c)
        expect(cb2).args(c)
        c._notify_open_listeners()

    def test_add_close_listener(self):
        c = Channel(mock(), None, {})
        c.add_close_listener('foo')
        assert_equals(set(['foo']), c._close_listeners)

    def test_remove_close_listener(self):
        c = Channel(mock(), None, {})
        c.add_close_listener('foo')
        c.remove_close_listener('foo')
        c.remove_close_listener('bar')
        assert_equals(set([]), c._close_listeners)

    def test_process_frames_passes_through_exception_from_close_listener(self):
        class MyError(Exception):
            pass

        connection = mock()
        ch = Channel(connection, channel_id=1, class_map={20: ChannelClass})
        rframe = mock(channel_id=ch.channel_id, class_id=20, method_id=40)
        ch._frame_buffer = deque([rframe])
        on_channel_closed = mock()
        ch.add_close_listener(on_channel_closed)

        expect(rframe.args.read_short).returns('rcode')
        expect(rframe.args.read_shortstr).returns('reason')
        expect(rframe.args.read_short).returns('cid')
        expect(rframe.args.read_short).returns('mid')

        expect(connection.send_frame).once()

        expect(on_channel_closed).args(ch).raises(MyError)

        expect(ch.logger.exception).args(
            'Closing on failed dispatch of frame %.255s', rframe)

        with assert_raises(MyError):
            ch.process_frames()


    def test_notify_close_listeners(self):
        c = Channel(mock(), None, {})
        cb1 = mock()
        cb2 = mock()
        c._close_listeners = set([cb1, cb2])
        expect(cb1).args(c)
        expect(cb2).args(c)
        c._notify_close_listeners()

    def test_open(self):
        c = Channel(mock(), None, {})
        expect(mock(c, 'channel').open)
        c.open()

    def test_active(self):
        c = Channel(mock(), None, {})
        expect(mock(c, 'channel').open)
        c.open()
        assertTrue(c.active)

    def test_close_with_no_args(self):
        c = Channel(mock(), None, {})
        expect(mock(c, 'channel').close).args(0, '', 0, 0)
        c.close()

    def test_close_with_args(self):
        c = Channel(mock(), None, {})
        expect(mock(c, 'channel').close).args(1, 'two', 3, 4)
        expect(c.channel.close).args(1, 'two', 3, 4)

        c.close(1, 'two', 3, 4)
        c.close(reply_code=1, reply_text='two', class_id=3, method_id=4)

    def test_close_when_channel_attr_cleared(self):
        c = Channel(mock(), None, {})
        assert_false(hasattr(c, 'channel'))
        c.close()

    def test_publish(self):
        c = Channel(mock(), None, {})
        expect(mock(c, 'basic').publish).args('arg1', 'arg2', foo='bar')
        c.publish('arg1', 'arg2', foo='bar')

    def test_publish_synchronous(self):
        c = Channel(mock(), None, {})
        expect(mock(c, 'tx').select)
        expect(mock(c, 'basic').publish).args('arg1', 'arg2', foo='bar')
        expect(c.tx.commit).args(cb='a_cb')

        c.publish_synchronous('arg1', 'arg2', foo='bar', cb='a_cb')

    def test_dispatch(self):
        c = Channel(mock(), None, {})
        frame = mock()
        frame.class_id = 32
        klass = mock()

        c._class_map[32] = klass
        expect(klass.dispatch).args(frame)
        c.dispatch(frame)

        frame.class_id = 33
        assert_raises(Channel.InvalidClass, c.dispatch, frame)

    def test_buffer_frame(self):
        c = Channel(mock(), None, {})
        c.buffer_frame('f1')
        c.buffer_frame('f2')
        assert_equals(deque(['f1', 'f2']), c._frame_buffer)

    def test_process_frames_when_no_frames(self):
        # Not that this should ever happen, but to be sure
        c = Channel(mock(), None, {})
        expect(c.dispatch).times(0)
        c.process_frames()

    def test_process_frames_stops_when_buffer_is_empty(self):
        c = Channel(mock(), None, {})
        f0 = MethodFrame('ch_id', 'c_id', 'm_id')
        f1 = MethodFrame('ch_id', 'c_id', 'm_id')
        c._frame_buffer = deque([f0, f1])

        expect(c.dispatch).args(f0)
        expect(c.dispatch).args(f1)

        c.process_frames()
        assert_equals(deque(), c._frame_buffer)

    def test_process_frames_stops_when_frameunderflow_raised(self):
        c = Channel(mock(), None, {})
        f0 = MethodFrame('ch_id', 'c_id', 'm_id')
        f1 = MethodFrame('ch_id', 'c_id', 'm_id')
        c._frame_buffer = deque([f0, f1])

        expect(c.dispatch).args(f0).raises(ProtocolClass.FrameUnderflow)

        c.process_frames()
        assert_equals(f1, c._frame_buffer[0])

    def test_process_frames_when_connectionclosed_on_dispatch(self):
        c = Channel(mock(), None, {})
        c._connection = mock()
        c._connection.logger = mock()

        f0 = MethodFrame(20, 30, 40)
        f1 = MethodFrame('ch_id', 'c_id', 'm_id')
        c._frame_buffer = deque([f0, f1])

        expect(c.dispatch).args(f0).raises(
            ConnectionClosed('something darkside'))
        expect(c.close).times(0)

        assert_raises(ConnectionClosed, c.process_frames)

    def test_process_frames_logs_and_closes_when_dispatch_error_raised(self):
        c = Channel(mock(), None, {})
        c._connection = mock()
        c._connection.logger = mock()

        f0 = MethodFrame(20, 30, 40)
        f1 = MethodFrame('ch_id', 'c_id', 'm_id')
        c._frame_buffer = deque([f0, f1])

        expect(c.dispatch).args(f0).raises(RuntimeError("zomg it broked"))
        expect(c.logger.exception).args(
            'Closing on failed dispatch of frame %.255s', f0)
        expect(c.close).args(500, 'Failed to dispatch %s' % (str(f0)))

        assert_raises(RuntimeError, c.process_frames)
        assert_equals(f1, c._frame_buffer[0])

    def test_process_frames_logs_and_preserves_original_exception_when_dispatch_and_close_fail(self):
        # fix it, too.
        c = Channel(mock(), 20, {})
        c._connection = mock()
        c._connection.logger = mock()

        f0 = MethodFrame(20, 30, 40)
        f1 = MethodFrame('ch_id', 'c_id', 'm_id')
        c._frame_buffer = deque([f0, f1])

        expect(c.dispatch).args(f0).raises(RuntimeError("zomg it broked"))
        expect(c.logger.exception).args(
            'Closing on failed dispatch of frame %.255s', f0)
        expect(c.close).raises(ValueError())
        expect(c.logger.exception).args('Channel close failed')

        assert_raises(RuntimeError, c.process_frames)
        assert_equals(f1, c._frame_buffer[0])

    def test_process_frames_raises_systemexit_when_close_raises_systemexit(self):
        c = Channel(mock(), 20, {})
        c._connection = mock()
        c._connection.logger = mock()

        f0 = MethodFrame(20, 30, 40)
        f1 = MethodFrame('ch_id', 'c_id', 'm_id')
        c._frame_buffer = deque([f0, f1])

        expect(c.dispatch).args(f0).raises(RuntimeError("zomg it broked"))
        expect(c.logger.exception).args(
            'Closing on failed dispatch of frame %.255s', f0)
        expect(c.close).raises(SystemExit())

        assert_raises(SystemExit, c.process_frames)
        assert_equals(f1, c._frame_buffer[0])


    def test_process_frames_does_not_close_and_raises_systemexit_when_dispatch_raises_systemexit(self):
        c = Channel(mock(), None, {})
        c._connection = mock()
        c._connection.logger = mock()

        f0 = MethodFrame(20, 30, 40)
        f1 = MethodFrame('ch_id', 'c_id', 'm_id')
        c._frame_buffer = deque([f0, f1])

        expect(c.dispatch).args(f0).raises(SystemExit())
        expect(c.close).times(0)

        assert_raises(SystemExit, c.process_frames)
        assert_equals(f1, c._frame_buffer[0])

    def test_process_frames_drops_non_close_methods_when_emergency_closing(self):
        c = Channel(mock(), None, {})
        c._emergency_close_pending = True
        c._connection = mock()
        c._connection.logger = mock()

        f0 = MethodFrame(1, 30, 40)
        f1 = HeaderFrame(1, 30, 0, 0)
        f2 = ContentFrame(1, "payload")
        f3_basic_close = MethodFrame(1, 20, 40)
        f4_basic_close_ok = MethodFrame(1, 20, 41)
        f5 = MethodFrame('ch_id', 'c_id', 'm_id')
        c._frame_buffer = deque(
            [f0, f1, f2, f3_basic_close, f4_basic_close_ok, f5])

        expect(c.dispatch).args(f0).times(0)
        expect(c.dispatch).args(f1).times(0)
        expect(c.dispatch).args(f2).times(0)
        expect(c.dispatch).args(f3_basic_close).times(1)
        expect(c.dispatch).args(f4_basic_close_ok).times(1)
        expect(c.dispatch).args(f5).times(0)
        expect(c.logger.warn).times(4)

        c.process_frames()
        assert_equals(0, len(c._frame_buffer))

    def test_next_frame_with_a_frame(self):
        c = Channel(mock(), None, {})
        ch_id, c_id, m_id = 0, 1, 2
        f0 = MethodFrame(ch_id, c_id, m_id)
        f1 = MethodFrame(ch_id, c_id, m_id)
        c._frame_buffer = deque([f0, f1])
        assert_equals(c.next_frame(), f0)

    def test_next_frame_with_no_frames(self):
        c = Channel(mock(), None, {})
        c._frame_buffer = deque()
        assert_equals(c.next_frame(), None)

    def test_requeue_frames(self):
        c = Channel(mock(), None, {})
        ch_id, c_id, m_id = 0, 1, 2
        f = [MethodFrame(ch_id, c_id, m_id) for i in xrange(4)]
        c._frame_buffer = deque(f[:2])

        c.requeue_frames(f[2:])
        assert_equals(c._frame_buffer, deque([f[i] for i in [3, 2, 0, 1]]))

    def test_send_frame_when_not_closed_no_flow_control_no_pending_events(self):
        conn = mock()
        c = Channel(conn, 32, {})

        expect(conn.send_frame).args('frame')

        c.send_frame('frame')

    def test_send_frame_when_not_closed_no_flow_control_pending_event(self):
        conn = mock()
        c = Channel(conn, 32, {})
        c._pending_events.append('cb')

        c.send_frame('frame')
        assert_equals(deque(['cb', 'frame']), c._pending_events)

    def test_send_frame_when_not_closed_and_flow_control(self):
        conn = mock()
        c = Channel(conn, 32, {})
        c._active = False

        method = MethodFrame(1, 2, 3)
        heartbeat = HeartbeatFrame()
        header = HeaderFrame(1, 2, 3, 4)
        content = ContentFrame(1, 'foo')

        expect(conn.send_frame).args(method)
        expect(conn.send_frame).args(heartbeat)

        c.send_frame(method)
        c.send_frame(heartbeat)
        assert_raises(Channel.Inactive, c.send_frame, header)
        assert_raises(Channel.Inactive, c.send_frame, content)

    def test_send_frame_when_closed_for_a_reason(self):
        conn = mock()
        c = Channel(conn, 32, {})
        c._closed = True
        c._close_info = {'reply_code': 42, 'reply_text': 'bad'}

        assert_raises(ChannelClosed, c.send_frame, 'frame')

    def test_send_frame_when_closed_for_no_reason(self):
        conn = mock()
        c = Channel(conn, 32, {})
        c._closed = True
        c._close_info = {'reply_code': 42, 'reply_text': ''}

        assert_raises(ChannelClosed, c.send_frame, 'frame')

    def test_add_synchronous_cb_when_transport_asynchronous(self):
        conn = mock()
        conn.synchronous = False
        c = Channel(conn, None, {})

        assert_equals(deque([]), c._pending_events)
        c.add_synchronous_cb('foo')
        assert_equals(deque(['foo']), c._pending_events)

    def test_add_synchronous_cb_when_transport_asynchronous_but_channel_synchronous(self):
        conn = mock()
        conn.synchronous = False
        c = Channel(conn, None, {}, synchronous=True)

        wrapper = mock()
        wrapper._read = True
        wrapper._result = 'done'

        expect(channel.SyncWrapper).args('foo').returns(wrapper)
        expect(conn.read_frames)
        expect(conn.read_frames).side_effect(
            lambda: setattr(wrapper, '_read', False))

        assert_equals(deque([]), c._pending_events)
        assert_equals('done', c.add_synchronous_cb('foo'))

        # This is technically cleared in runtime, but assert that it's not cleared
        # in this method
        assert_equals(deque([wrapper]), c._pending_events)

    def test_add_synchronous_cb_when_transport_synchronous(self):
        conn = mock()
        conn.synchronous = True
        c = Channel(conn, None, {})

        wrapper = mock()
        wrapper._read = True
        wrapper._result = 'done'

        expect(channel.SyncWrapper).args('foo').returns(wrapper)
        expect(conn.read_frames)
        expect(conn.read_frames).side_effect(
            lambda: setattr(wrapper, '_read', False))

        assert_equals(deque([]), c._pending_events)
        assert_equals('done', c.add_synchronous_cb('foo'))

        # This is technically cleared in runtime, but assert that it's not cleared
        # in this method
        assert_equals(deque([wrapper]), c._pending_events)

    def test_add_synchronous_cb_when_transport_synchronous_and_channel_closes(self):
        conn = mock()
        conn.synchronous = True
        c = Channel(conn, None, {})

        wrapper = mock()
        wrapper._read = True
        wrapper._result = 'done'

        expect(channel.SyncWrapper).args('foo').returns(wrapper)
        expect(conn.read_frames)
        expect(conn.read_frames).side_effect(
            lambda: setattr(c, '_closed', True))

        with assert_raises(ChannelClosed):
            c.add_synchronous_cb('foo')

    def test_clear_synchronous_cb_when_no_pending(self):
        c = Channel(mock(), None, {})
        stub(c._flush_pending_events)

        assert_equals(deque([]), c._pending_events)
        assert_equals('foo', c.clear_synchronous_cb('foo'))

    def test_clear_synchronous_cb_when_pending_cb_matches(self):
        c = Channel(mock(), None, {})
        c._pending_events = deque(['foo'])

        expect(c._flush_pending_events)

        assert_equals('foo', c.clear_synchronous_cb('foo'))
        assert_equals(deque([]), c._pending_events)

    def test_clear_synchronous_cb_when_pending_cb_doesnt_match_but_isnt_in_list(self):
        c = Channel(mock(), None, {})
        c._pending_events = deque(['foo'])

        expect(c._flush_pending_events).times(0)

        assert_equals('bar', c.clear_synchronous_cb('bar'))
        assert_equals(deque(['foo']), c._pending_events)

    def test_clear_synchronous_cb_raises_when_pending_cb_doesnt_match_but_is_in_list(self):
        c = Channel(mock(), None, {})
        stub(c._flush_pending_events)
        c._pending_events = deque(['foo', 'bar'])

        assert_raises(ChannelError, c.clear_synchronous_cb, 'bar')
        assert_equals(deque(['foo', 'bar']), c._pending_events)

    def test_flush_pending_events_flushes_all_leading_frames(self):
        conn = mock()
        c = Channel(conn, 42, {})
        f1 = MethodFrame(42, 2, 3)
        f2 = MethodFrame(42, 2, 3)
        f3 = MethodFrame(42, 2, 3)
        c._pending_events = deque([f1, f2, 'cb', f3])

        expect(conn.send_frame).args(f1)
        expect(conn.send_frame).args(f2)

        c._flush_pending_events()
        assert_equals(deque(['cb', f3]), c._pending_events)

    def test_closed_cb_without_final_frame(self):
        c = Channel(mock(), None, {
            20: ChannelClass,
            40: ExchangeClass,
            50: QueueClass,
            60: BasicClass,
            90: TransactionClass,
        })
        c._pending_events = 'foo'
        c._frame_buffer = 'foo'

        for val in c._class_map.values():
            expect(val._cleanup)
        expect(c._notify_close_listeners)

        c._closed_cb()
        assert_equals(deque([]), c._pending_events)
        assert_equals(deque([]), c._frame_buffer)
        assert_equals(None, c._connection)
        assert_false(hasattr(c, 'channel'))
        assert_false(hasattr(c, 'exchange'))
        assert_false(hasattr(c, 'queue'))
        assert_false(hasattr(c, 'basic'))
        assert_false(hasattr(c, 'tx'))
        assert_equals(None, c._class_map)
        assert_equals(set(), c._close_listeners)

    def test_closed_cb_with_final_frame(self):
        conn = mock()
        c = Channel(conn, None, {})

        expect(conn.send_frame).args('final')
        for val in c._class_map.values():
            expect(val._cleanup)

        c._closed_cb('final')
