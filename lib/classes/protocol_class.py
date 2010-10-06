
class ProtocolClass(object):
  '''
  The base class of all protocol classes.
  '''
  
  class ProtocolError(Exception): pass
  class InvalidMethod(ProtocolError): pass
  
  # decorator to registor dispatch
  class register(object):
    def __init__(self, id):
      self._id = id
    
    def __call__(self, function):
      ProtocolClass._bind_method(self._id, function)
      return function

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

  def dispatch(self, method_frame, *content_frames):
    '''
    Dispatch a method for this protocol.
    '''
    try:
      # TODO: get rid of this try-catch.  The stack inside the method callback
      # could be quite large, as it might callback to a user's application stack.
      self.channel.logger.info("Dispatching to method_id : %s", method_frame.method_id)
      self.dispatch_map[method_frame.method_id](self)
    except KeyError:
      raise self.InvalidMethod("no method is registered with id: %d" % method_frame.method_id)

  @classmethod
  def _bind_method(cls, id, function):
#    if cls.dispatch_map.has_key(id):
#      raise cls.MethodBindingError("a method is alread bound to id: %d" % id)
#    cls.dispatch_map[id] = function
    pass

  def send_frame(self, frame):
    '''
    Send a frame
    '''
    # TODO: actually implement this.
    self.channel.send_frame( frame )
