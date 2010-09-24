
class ProtocolClass(object):
  '''
  The base class of all protocol classes.
  '''

  # subclasses should re-define this
  # TODO: do something awesome with decorators?
  dispatch_map = {}

  def __init__(self, channel):
    '''
    Construct this protocol class on a channel.
    '''
    self._channel = channel
    self._method_map = {}

  @property
  def channel(self):
    return self._channel

  @property
  def channel_id(self):
    return self._channel.channel_id

  def dispatch(self, method_frame, *content_frames):
    '''
    Dispatch a method for this protocol.
    '''
    # TODO: Can we do an automatic scheme, or do we have to leave this
    # up to each subclass?  Automatic would be soooo much nicer
