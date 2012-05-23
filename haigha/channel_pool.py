'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from collections import deque

class ChannelPool(object):
  '''
  Manages a pool of channels for transaction-based publishing.  This allows a
  client to use as many channels as are necessary to publish while not creating
  a backlog of transactions that slows throughput and consumes memory.

  The pool can accept an optional `size` argument in the ctor, which caps the
  number of channels which the pool will allocate. If no channels are available
  on `publish()`, the message will be locally queued and sent as soon as a 
  channel is available. It is recommended that you use the pool with a max
  size, as each channel consumes memory on the broker and it is possible to
  exercise memory limit protection seems on the broker due to number of
  channels.
  '''
  
  def __init__(self, connection, size=None):
    '''Initialize the channel on a connection.'''
    self._connection = connection
    self._free_channels = set()
    self._size = size
    self._queue = deque()
    self._channels = 0

  def publish(self, *args, **kwargs):
    '''
    Publish a message. Caller can supply an optional callback which will
    be fired when the transaction is committed. Tries very hard to avoid
    closed and inactive channels, but a ChannelError or ConnectionError
    may still be raised.
    '''
    user_cb = kwargs.pop('cb', None)

    # If the first channel we grab is inactive, continue fetching until
    # we get an active channel, then put the inactive channels back in
    # the pool. Try to keep the overhead to a minimum.
    channel = self._get_channel()

    if channel and not channel.active:
      inactive_channels = set()
      while channel and not channel.active:
        inactive_channels.add( channel )
        channel = self._get_channel()
      self._free_channels.update( inactive_channels )

    # When the transaction is committed, add the channel back to the pool and
    # call any user-defined callbacks. If there is anything in queue, pop it
    # and call back to publish(). Only do so if the channel is still active
    # though, because otherwise the message will end up at the back of the
    # queue, breaking the original order.
    def committed():
      self._free_channels.add( channel )
      if channel.active and not channel.closed:
        self._process_queue()
      if user_cb is not None: user_cb()

    if channel:
      channel.publish_synchronous( *args, cb=committed, **kwargs )
    else:
      kwargs['cb'] = user_cb
      self._queue.append( (args,kwargs) )
    
  def _process_queue(self):
    '''
    If there are any message in the queue, process one of them.
    '''
    if len(self._queue):
      args, kwargs = self._queue.popleft()
      self.publish( *args, **kwargs )

  def _get_channel(self):
    '''
    Fetch a channel from the pool. Will return a new one if necessary. If
    a channel in the free pool is closed, will remove it. Will return None
    if we hit the cap. Will clean up any channels that were published to but
    closed due to error.
    '''
    while len(self._free_channels):
      rval = self._free_channels.pop()
      if not rval.closed: 
        return rval
      # don't adjust _channels value because the callback will do that and
      # we don't want to double count it.

    if not self._size or self._channels < self._size:
      rval = self._connection.channel()
      self._channels += 1
      rval.add_close_listener( self._channel_closed_cb )
      return rval

  def _channel_closed_cb(self, channel):
    '''
    Callback when channel closes.
    '''
    self._channels -= 1
    self._process_queue()
