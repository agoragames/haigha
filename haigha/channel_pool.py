
class ChannelPool(object):
  '''
  Manages a pool of channels for transaction-based publishing.  This allows a
  client to use as many channels as are necessary to publish while not creating
  a backlog of transactions that slows throughput and consumes memory.

  There is currently no soft limit placed on the size of the pool, so it will
  continue to allocate channels as needed. The user should be wary to monitor
  the number of channels if there is a concern that throughput will cause the
  client to exceed the number of channels supported by the broker.
  '''
  
  def __init__(self, connection):
    '''Initialize the channel on a connection.'''
    self._connection = connection
    self._free_channels = set()

  def publish(self, *args, **kwargs):
    '''
    Publish a message. Caller can supply an optional callback which will
    be fired when the transaction is committed.
    '''
    user_cb = kwargs.pop('cb', None)
    channel = self._get_channel()

    def committed():
      self._free_channels.add( channel )
      if user_cb is not None: user_cb()

    channel.publish_synchronous( *args, cb=committed, **kwargs )

  def _get_channel(self):
    '''
    Fetch a channel from the pool. Will return a new one if necessary.
    '''
    if len(self._free_channels):
      return self._free_channels.pop()
    return self._connection.channel()
