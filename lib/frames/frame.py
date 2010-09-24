
class Frame(object):
  '''
  Base class for a frame.
  '''

  # Exceptions
  class FrameError(Exception): '''Base class for all frame errors'''
  class MissingFooter(FrameError): '''Could not find the footer to the frame.'''

  # Class data
  _frame_type_map = {}
  
  # Class methods
  @classmethod
  def register(cls):
    '''
    Register a frame type.
    '''
    # TODO is there a better way to do this?  Order of class declaration matters
    # here.  There's probably a way to tap into when a subclass is declared.  Could
    # write a decorator 'register_type()` that subclasses classmethod?  Would like
    # to clean up initialization pattern
    _frame_type_map[ cls.type() ] = cls

  @classmethod
  def read_frames(cls, buffer):
    '''
    Read one or more frames from an IO buffer.  Buffer must support file object
    interface.

    After reading, caller will need to check if there are bytes remaining in the
    buffer.  If there are, then that implies that there is one or more incomplete
    frames and more data needs to be read.  The position of the cursor in the
    frame buffer will mark the point at which the last good frame was read.  If
    the caller is expecting a sequence of frames and only received a part of that
    sequence, they are responsible for buffering those frames until the rest of
    the frames in the sequence have arrived.

    Note: no effort is made in this function to determine 
    '''
    rval = []

    while True:
      frame_start_pos = buffer.tell()

      frame = _read_frame(buffer)
      if frame is None: 
        buffer.seek( frame_start_pos )
        break

      rval.add( frame )

    return rval

  @classmethod
  def _read_frame(cls, buffer):
    '''
    Read a single frame from a buffer.  Will return None if there is an incomplete
    frame in the buffer.

    Raise MissingFooter if there's a problem reading the footer byte.
    '''
    # TODO: Do we implement a reader here, or stick all the writing and reading
    # interfaces right into this class?
    frame_type = reader.read_octet()
    channel = reader.read_short()
    size = reader.read_long()
    payload = reader.read( size )

    if len(payload) != size:
      #raise AMQPIncompletePayloadError('Payload length %d did not match expected size %d' % \
      #  (len(payload), size) )
      return None

    # TODO: In the edge case where we're missing just this one byte, return None
    ch = reader.read_octet()  # footer
    if ch != 0xce:
      raise MissingFooter('Framing error, unexpected byte: %x.  frame type %x. channel %d, payload size %d' % \
        (ch, frame_type, channel, size) )

  
  @classmethod
  def read_octet(cls, buffer):
    pass


  # Instance methods
  def type(self):
    '''
    Fetch the type of this frame.  Should be an octet.
    '''
    raise NotImplementedError()

