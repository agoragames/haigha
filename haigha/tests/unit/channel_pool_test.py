import mox

from haigha.channel_pool import ChannelPool

class ChannelPoolTest(mox.MoxTestBase):

  def test_init(self):
    c = ChannelPool('connection')
    self.assertEquals('connection', c._connection)
    self.assertEquals(set(), c._free_channels)
