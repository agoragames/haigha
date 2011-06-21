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
    self._channel = channel

  @property
  def channel(self):
    return self._channel

  @property
  def channel_id(self):
    return self._channel.channel_id

  @property
  def logger(self):
    return self._channel.logger

  @property
  def default_ticket(self):
    return 0

  def dispatch(self, method_frame):
    '''
    Dispatch a method for this protocol.
    '''
    # HACK: because the synch callback stack will be based on instance methods,
    # we need to take what's currently registered and turn that into an instance
    # attr.
    method = self.dispatch_map.get( method_frame.method_id )
    if method:
      method = getattr(self, method.im_func.__name__)
      
      self.channel.clear_synchronous_cb( method )
      method(method_frame)
    else:
      raise self.InvalidMethod("no method is registered with id: %d" % method_frame.method_id)

  def send_frame(self, frame):
    '''
    Send a frame
    '''
    self.channel.send_frame( frame )
