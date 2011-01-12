import struct
from haigha.reader import Reader

class Frame(object):
  '''
  Base class for a frame.
  '''

  # Exceptions
  class FrameError(Exception): '''Base class for all frame errors'''
  class InvalidFrameType(FrameError): '''The frame type is unknown.'''
  class MissingFooter(FrameError): '''Could not find the footer to the frame.'''

  # Class data
  _frame_type_map = {}
  
  # Class methods
  @classmethod
  def register(cls):
    '''
    Register a frame type.
    '''
    cls._frame_type_map[ cls.type() ] = cls

  @classmethod
  def type(self):
    '''
    Fetch the type of this frame.  Should be an octet.
    '''
    raise NotImplementedError()

  @classmethod
  def read_frames(cls, stream):
    '''
    Read one or more frames from an IO stream.  Buffer must support file object
    interface.

    After reading, caller will need to check if there are bytes remaining in the
    stream.  If there are, then that implies that there is one or more incomplete
    frames and more data needs to be read.  The position of the cursor in the
    frame stream will mark the point at which the last good frame was read.  If
    the caller is expecting a sequence of frames and only received a part of that
    sequence, they are responsible for buffering those frames until the rest of
    the frames in the sequence have arrived.
    '''
    rval = []

    while True:
      frame_start_pos = stream.tell()
      try:
        frame = cls._read_frame(stream)
      except struct.error, e:
        # No more data in the stream
        frame = None
      if frame is None: 
        stream.seek( frame_start_pos )
        break

      rval.append( frame )

    return rval

  @classmethod
  def _read_frame(cls, stream):
    '''
    Read a single frame from a stream.  Will return None if there is an incomplete
    frame in the stream.

    Raise MissingFooter if there's a problem reading the footer byte.
    '''
    reader = Reader(stream)
    frame_type = reader.read_octet()
    channel_id = reader.read_short()
    size = reader.read_long()
    payload = reader.read( size )

    if len(payload) != size:
      return None

    ch = reader.read_octet()  # footer
    if ch != 0xce:
      raise Frame.MissingFooter(
        'Framing error, unexpected byte: %x.  frame type %x. channel %d, payload size %d',
        ch, frame_type, channel, size )

    frame_class = cls._frame_type_map.get( frame_type )
    if not frame_class:
      raise Frame.InvalidFrameType("Unknown frame type %x", frame_type)
    return frame_class.parse( channel_id, payload )



  # Instance methods
  def __init__(self, channel_id=-1):
    self._channel_id = channel_id

  @classmethod
  def parse(cls, channel_id, payload):
    '''
    Subclasses need to implement parsing of their frames.  Should return a new
    instance of their type.
    '''
    raise NotImplementedError()

  @property
  def channel_id(self):
    return self._channel_id

  def __str__(self):
    return "%s[channel: %d]"%( self.__class__.__name__, self.channel_id )

  def write_frame(self, stream):
    '''
    Write this frame.
    '''
    raise NotImplementedError()
