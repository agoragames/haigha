'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from collections import deque

from chai import Chai

from haigha.channel_pool import ChannelPool

class ChannelPoolTest(Chai):

  def test_init(self):
    c = ChannelPool('connection')
    self.assertEquals('connection', c._connection)
    self.assertEquals(set(), c._free_channels)
    assert_equals( None, c._size )
    assert_equals( 0, c._channels )
    assert_equals( deque(), c._queue )
    
    c = ChannelPool('connection', size=50)
    self.assertEquals('connection', c._connection)
    self.assertEquals(set(), c._free_channels)
    assert_equals( 50, c._size )
    assert_equals( 0, c._channels )
    assert_equals( deque(), c._queue )

  def test_publish_without_user_cb(self):
    ch = mock()
    cp = ChannelPool(None)

    expect(cp._get_channel).returns( ch )
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=var('cb'), doit='harder' )

    cp.publish( 'arg1', 'arg2', doit='harder' )
    assert_equals( set(), cp._free_channels )
    
    # run committed callback
    var('cb').value()
    assert_equals( set([ch]), cp._free_channels )

  def test_publish_with_user_cb(self):
    ch = mock()
    cp = ChannelPool(None)
    user_cb = mock()

    expect(cp._get_channel).returns( ch )
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=var('cb'), doit='harder' )

    cp.publish( 'arg1', 'arg2', cb=user_cb, doit='harder' )
    assert_equals( set(), cp._free_channels )

    expect(user_cb)
    var('cb').value()
    assert_equals( set([ch]), cp._free_channels )

  def test_publish_resends_queued_messages_if_channel_is_active(self):
    ch = mock()
    cp = ChannelPool(None)
    user_cb = mock()
    ch.active = True
    ch.closed = False
    cp._queue.append( (('a1', 'a2'), {'cb':'foo', 'yo':'dawg'}) )

    expect(cp._get_channel).returns( ch )
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=var('cb'), doit='harder' )

    cp.publish( 'arg1', 'arg2', cb=user_cb, doit='harder' )
    assert_equals( set(), cp._free_channels )
    assert_equals( 1, len(cp._queue) )

    expect(cp._process_queue)
    expect(user_cb)
    var('cb').value()
    assert_equals( set([ch]), cp._free_channels )

  def test_publish_does_not_resend_queued_messages_if_channel_is_inactive(self):
    ch = mock()
    cp = ChannelPool(None)
    user_cb = mock()
    ch.active = True
    ch.closed = False
    cp._queue.append( (('a1', 'a2'), {'cb':'foo', 'yo':'dawg'}) )

    expect(cp._get_channel).returns( ch )
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=var('cb'), doit='harder' )

    cp.publish( 'arg1', 'arg2', cb=user_cb, doit='harder' )
    assert_equals( set(), cp._free_channels )
    assert_equals( 1, len(cp._queue) )

    ch.active = False
    
    stub(cp._process_queue)
    expect(user_cb)
    var('cb').value()
    assert_equals( set([ch]), cp._free_channels )
    assert_equals( 1, len(cp._queue) )

  def test_publish_does_not_resend_queued_messages_if_channel_is_closed(self):
    ch = mock()
    cp = ChannelPool(None)
    user_cb = mock()
    ch.active = True
    ch.closed = False
    cp._queue.append( (('a1', 'a2'), {'cb':'foo', 'yo':'dawg'}) )

    expect(cp._get_channel).returns( ch )
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=var('cb'), doit='harder' )

    cp.publish( 'arg1', 'arg2', cb=user_cb, doit='harder' )
    assert_equals( set(), cp._free_channels )
    assert_equals( 1, len(cp._queue) )

    ch.closed = True
    stub(cp._process_queue)
    expect(user_cb)
    var('cb').value()
    assert_equals( set([ch]), cp._free_channels )
    assert_equals( 1, len(cp._queue) )

  def test_publish_searches_for_active_channel(self):
    ch1 = mock()
    ch2 = mock()
    ch3 = mock()
    ch1.active = ch2.active = False
    ch3.active = True
    cp = ChannelPool(None)

    expect(cp._get_channel).returns(ch1)
    expect(cp._get_channel).returns(ch2)
    expect(cp._get_channel).returns(ch3)
    expect(ch3.publish_synchronous).args( 'arg1', 'arg2', cb=ignore() )

    cp.publish( 'arg1', 'arg2' )
    self.assertEquals( set([ch1,ch2]), cp._free_channels )

  def test_publish_appends_to_queue_when_no_ready_channels(self):
    cp = ChannelPool(None)

    expect(cp._get_channel).returns(None)

    cp.publish( 'arg1', 'arg2', arg3='foo', cb='usercb' )
    self.assertEquals( set(), cp._free_channels )
    assert_equals( deque([ (('arg1','arg2'), {'arg3':'foo','cb':'usercb'})]), 
      cp._queue )

  def test_publish_appends_to_queue_when_no_ready_channels_out_of_several(self):
    ch1 = mock()
    cp = ChannelPool(None)
    ch1.active = False

    expect(cp._get_channel).returns(ch1)
    expect(cp._get_channel).returns(None)

    cp.publish( 'arg1', 'arg2', arg3='foo', cb='usercb' )
    self.assertEquals( set([ch1]), cp._free_channels )
    assert_equals( deque([ (('arg1','arg2'), {'arg3':'foo','cb':'usercb'})]), 
      cp._queue )

  def test_process_queue(self):
    cp = ChannelPool(None)
    cp._queue = deque([
      (('foo',),{'a':1}),
      (('bar',),{'b':2}),
    ])
    expect( cp.publish ).args('foo', a=1)
    expect( cp.publish ).args('bar', b=2)

    cp._process_queue()
    cp._process_queue()
    cp._process_queue()

  def test_get_channel_returns_new_when_none_free_and_not_at_limit(self):
    conn = mock()
    cp = ChannelPool(conn)
    cp._channels = 1

    with expect(conn.channel).returns(mock()) as newchannel:
      expect( newchannel.add_close_listener ).args( cp._channel_closed_cb )
      self.assertEquals( newchannel, cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )
    assert_equals( 2, cp._channels )

  def test_get_channel_returns_new_when_none_free_and_at_limit(self):
    conn = mock()
    cp = ChannelPool(conn, 1)
    cp._channels = 1

    stub(conn.channel)
    
    self.assertEquals( None, cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )

  def test_get_channel_when_one_free_and_not_closed(self):
    conn = mock()
    ch = mock()
    ch.closed = False
    cp = ChannelPool(conn)
    cp._free_channels = set([ch])

    self.assertEquals( ch, cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )

  def test_get_channel_when_two_free_and_one_closed(self):
    # Because we can't mock builtins ....
    class Set(set):
      def pop(self): pass

    conn = mock()
    ch1 = mock()
    ch1.closed = True
    ch2 = mock()
    ch2.closed = False
    cp = ChannelPool(conn)
    cp._free_channels = Set([ch1,ch2])
    cp._channels = 2

    # Because we want them in order
    expect( cp._free_channels.pop ).returns( ch1 ).side_effect( super(Set,cp._free_channels).pop )
    expect( cp._free_channels.pop ).returns( ch2 ).side_effect( super(Set,cp._free_channels).pop )

    self.assertEquals( ch2, cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )
    assert_equals( 2, cp._channels )

  def test_get_channel_when_two_free_and_all_closed(self):
    conn = mock()
    ch1 = mock()
    ch1.closed = True
    ch2 = mock()
    ch2.closed = True
    cp = ChannelPool(conn)
    cp._free_channels = set([ch1,ch2])
    cp._channels = 2

    with expect(conn.channel).returns(mock()) as newchannel:
      expect( newchannel.add_close_listener ).args( cp._channel_closed_cb )
      self.assertEquals( newchannel, cp._get_channel() )

    self.assertEquals( set(), cp._free_channels )
    assert_equals( 3, cp._channels )

  def test_channel_closed_cb(self):
    cp = ChannelPool(None)
    cp._channels = 32

    expect( cp._process_queue )
    cp._channel_closed_cb('channel')
    assert_equals( 31, cp._channels )
