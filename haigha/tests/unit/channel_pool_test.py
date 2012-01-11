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

    def test_committed_cb(cb):
      # Because using this for side effects is kinda fugly, protect it
      if not getattr(cb,'_called_yet',False):
        cb()
        setattr(cb, '_called_yet', True)
      return True

    expect(cp._get_channel).returns( ch )
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=func(test_committed_cb), doit='harder' )

    self.assertEquals( set(), cp._free_channels )
    cp.publish( 'arg1', 'arg2', doit='harder' )
    self.assertEquals( set([ch]), cp._free_channels )

  def test_publish_with_user_cb(self):
    ch = mock()
    cp = ChannelPool(None)
    user_cb = mock()

    def test_committed_cb(cb):
      # Because using this for side effects is kinda fugly, protect it
      if not getattr(cb,'_called_yet',False):
        expect(user_cb)
        cb()
        setattr(cb, '_called_yet', True)
      return True

    expect(cp._get_channel).returns( ch )
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=func(test_committed_cb), doit='harder' )

    self.assertEquals( set(), cp._free_channels )
    cp.publish( 'arg1', 'arg2', cb=user_cb, doit='harder' )
    self.assertEquals( set([ch]), cp._free_channels )

  def test_publish_resends_queued_messages_if_channel_is_active(self):
    ch = mock()
    cp = ChannelPool(None)
    user_cb = mock()
    ch.active = True
    cp._queue.append( (('a1', 'a2'), {'cb':'foo', 'yo':'dawg'}) )

    def test_committed_cb(cb):
      # Because using this for side effects is kinda fugly, protect it
      if not getattr(cb,'_called_yet',False):
        expect( cp.publish ).args( 'a1', 'a2', cb='foo', yo='dawg' )
        expect(user_cb)
        cb()
        setattr(cb, '_called_yet', True)
      
      return True

    expect(cp._get_channel).returns( ch )
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=func(test_committed_cb), doit='harder' )

    self.assertEquals( set(), cp._free_channels )
    cp.publish( 'arg1', 'arg2', cb=user_cb, doit='harder' )
    self.assertEquals( set([ch]), cp._free_channels )
    assert_equals( deque(), cp._queue )

  def test_publish_does_not_resend_queued_messages_if_channel_is_inactive(self):
    ch = mock()
    cp = ChannelPool(None)
    user_cb = mock()
    ch.active = True
    cp._queue.append( (('a1', 'a2'), {'cb':'foo', 'yo':'dawg'}) )

    def test_committed_cb(cb):
      # Because using this for side effects is kinda fugly, protect it
      if not getattr(cb,'_called_yet',False):
        ch.active = False
        expect(user_cb)
        cb()
        setattr(cb, '_called_yet', True)
      
      return True

    expect(cp._get_channel).returns( ch )
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=func(test_committed_cb), doit='harder' )

    self.assertEquals( set(), cp._free_channels )
    cp.publish( 'arg1', 'arg2', cb=user_cb, doit='harder' )
    self.assertEquals( set([ch]), cp._free_channels )
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

  def test_get_channel_returns_new_when_none_free_and_not_at_limit(self):
    conn = mock()
    cp = ChannelPool(conn)
    cp._channels = 1

    expect(conn.channel).returns( 'channel' )
    
    self.assertEquals( 'channel', cp._get_channel() )
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
    assert_equals( 1, cp._channels )

  def test_get_channel_when_two_free_and_all_closed(self):
    conn = mock()
    ch1 = mock()
    ch1.closed = True
    ch2 = mock()
    ch2.closed = True
    cp = ChannelPool(conn)
    cp._free_channels = set([ch1,ch2])
    cp._channels = 2

    expect(conn.channel).returns('channel')

    self.assertEquals( 'channel', cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )
    assert_equals( 1, cp._channels )
