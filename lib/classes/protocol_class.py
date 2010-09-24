
class ProtocolClass(object):
  '''
  The base class of all protocol classes.
  '''
  
  class ProtocolError(Exception): pass
  class MethodBindingError(ProtocolError): pass
  
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

  def dispatch(self, method_frame, *content_frames):
    '''
    Dispatch a method for this protocol.
    '''
    try:
      self.dispatch_map[method_frame.method_id](self)
    except IndexError:
      raise self.NoMethodRegistered("no method is registered with id: %d" % method_frame.method_id)

  @classmethod
  def _bind_method(cls, id, function):
#    if cls.dispatch_map.has_key(id):
#      raise cls.MethodBindingError("a method is alread bound to id: %d" % id)
#    cls.dispatch_map[id] = function
    pass
