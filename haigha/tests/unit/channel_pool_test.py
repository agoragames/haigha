import mox

from haigha.channel_pool import ChannelPool

class ChannelPoolTest(mox.MoxTestBase):

  def test_init(self):
    c = ChannelPool('connection')
    self.assertEquals('connection', c._connection)
    self.assertEquals(set(), c._free_channels)

  def test_publish_without_user_cb(self):
    ch = self.create_mock_anything()
    ch.__hash__ = lambda: 42
    cp = ChannelPool(None)
    self.mock( cp, '_get_channel' )

    def test_committed_cb(cb):
      cb()
      return True

    cp._get_channel().AndReturn( ch )
    ch.publish_synchronous( 'arg1', 'arg2', cb=mox.Func(test_committed_cb), doit='harder' )

    self.replay_all()
    self.assertEquals( set(), cp._free_channels )
    cp.publish( 'arg1', 'arg2', doit='harder' )
    self.assertEquals( set([ch]), cp._free_channels )

  def test_publish_with_user_cb(self):
    ch = self.create_mock_anything()
    ch.__hash__ = lambda: 42
    cp = ChannelPool(None)
    self.mock( cp, '_get_channel' )
    user_cb = self.create_mock_anything()

    def test_committed_cb(cb):
      cb()
      return True

    cp._get_channel().AndReturn( ch )
    ch.publish_synchronous( 'arg1', 'arg2', cb=mox.Func(test_committed_cb), doit='harder' )
    user_cb()

    self.replay_all()
    self.assertEquals( set(), cp._free_channels )
    cp.publish( 'arg1', 'arg2', cb=user_cb, doit='harder' )
    self.assertEquals( set([ch]), cp._free_channels )

  def test_get_channel_when_none_free(self):
    conn = self.create_mock_anything()
    cp = ChannelPool(conn)

    conn.channel().AndReturn( 'channel' )
    
    self.replay_all()
    self.assertEquals( 'channel', cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )

  def test_get_channel_when_one_free(self):
    conn = self.create_mock_anything()
    cp = ChannelPool(conn)
    cp._free_channels = set(['channel'])

    self.assertEquals( 'channel', cp._get_channel() )
    self.assertEquals( set(), cp._free_channels )
