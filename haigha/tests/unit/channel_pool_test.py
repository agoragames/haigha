from chai import Chai

from haigha.channel_pool import ChannelPool

class ChannelPoolTest(Chai):

  def test_init(self):
    c = ChannelPool('connection')
    self.assertEquals('connection', c._connection)
    self.assertEquals(set(), c._free_channels)

  def test_publish_without_user_cb(self):
    ch = mock()
    ch.__hash__ = lambda: 42
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
    ch.__hash__ = lambda: 42
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

  def test_get_channel_when_none_free(self):
    conn = mock()
    cp = ChannelPool(conn)

    expect(conn.channel).returns( 'channel' )
    
    self.assertEquals( 'channel', cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )

  def test_get_channel_when_one_free(self):
    conn = self.mock()
    cp = ChannelPool(conn)
    cp._free_channels = set(['channel'])

    self.assertEquals( 'channel', cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )
