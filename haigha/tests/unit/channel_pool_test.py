from chai import Chai

from haigha.channel_pool import ChannelPool

class ChannelPoolTest(Chai):

  def test_init(self):
    c = ChannelPool('connection')
    self.assertEquals('connection', c._connection)
    self.assertEquals(set(), c._free_channels)

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
        cb()
        setattr(cb, '_called_yet', True)
      return True

    expect(cp._get_channel).returns( ch )
    expect(user_cb).any_order()
    expect(ch.publish_synchronous).args( 'arg1', 'arg2', cb=func(test_committed_cb), doit='harder' )

    self.assertEquals( set(), cp._free_channels )
    cp.publish( 'arg1', 'arg2', cb=user_cb, doit='harder' )
    self.assertEquals( set([ch]), cp._free_channels )

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

  def test_get_channel_when_none_free(self):
    conn = mock()
    cp = ChannelPool(conn)

    expect(conn.channel).returns( 'channel' )
    
    self.assertEquals( 'channel', cp._get_channel() )
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

    # Because we want them in order
    expect( cp._free_channels.pop ).returns( ch1 ).side_effect( super(Set,cp._free_channels).pop )
    expect( cp._free_channels.pop ).returns( ch2 ).side_effect( super(Set,cp._free_channels).pop )

    self.assertEquals( ch2, cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )

  def test_get_channel_when_two_free_and_all_closed(self):
    conn = mock()
    ch1 = mock()
    ch1.closed = True
    ch2 = mock()
    ch2.closed = True
    cp = ChannelPool(conn)
    cp._free_channels = set([ch1,ch2])

    expect(conn.channel).returns('channel')

    self.assertEquals( 'channel', cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )
