'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

class ProtocolClass(object):
  '''
  The base class of all protocol classes.
  '''
  
  class ProtocolError(Exception): pass
  class InvalidMethod(ProtocolError): pass
  class FrameUnderflow(ProtocolError): pass
  
  dispatch_map = {}

  def __init__(self, channel):
    '''
    Construct this protocol class on a channel.
    '''
    # Cache the channel id so that cleanup can remove the circular channel
    # reference but id is still accessible (it's useful!)
    self._channel = channel
    self._channel_id = channel.channel_id

  @property
  def channel(self):
    return self._channel

  @property
  def channel_id(self):
    return self._channel_id

  @property
  def logger(self):
    return self._channel.logger

  @property
  def default_ticket(self):
    return 0

  def _cleanup(self):
    '''
    "Private" call from Channel when it's shutting down so that local
    data can be cleaned up and references closed out. It's strongly
    recommended that subclasses call this /after/ doing their own cleanup .
    Note that this removes reference to both the channel and the dispatch
    map.
    '''
    self._channel = None
    self.dispatch_map = None

  def dispatch(self, method_frame):
    '''
    Dispatch a method for this protocol.
    '''
    method = self.dispatch_map.get( method_frame.method_id )
    if method:
      self.channel.clear_synchronous_cb( method )
      method(method_frame)
    else:
      raise self.InvalidMethod("no method is registered with id: %d" % method_frame.method_id)

  def send_frame(self, frame):
    '''
    Send a frame
    '''
    self.channel.send_frame( frame )
