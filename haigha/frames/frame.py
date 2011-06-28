'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

import struct
import sys
from collections import deque
from haigha.reader import Reader

class Frame(object):
  '''
  Base class for a frame.
  '''

  # Exceptions
  class FrameError(Exception): '''Base class for all frame errors'''
  class FormatError(FrameError): '''The frame was mal-formed.'''
  class InvalidFrameType(FrameError): '''The frame type is unknown.'''

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
  def read_frames(cls, reader):
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
    rval = deque()

    while True:
      frame_start_pos = reader.tell()
      try:
        frame = Frame._read_frame( reader )
      except Reader.BufferUnderflow:
        # No more data in the stream
        frame = None
      except Reader.ReaderError as e:
        # Some other format error
        raise Frame.FormatError, str(e), sys.exc_info()[-1]
      except struct.error as e:
        raise Frame.FormatError, str(e), sys.exc_info()[-1]

      if frame is None: 
        reader.seek( frame_start_pos )
        break

      rval.append( frame )

    return rval

  @classmethod
  def _read_frame(cls, reader):
    '''
    Read a single frame from a Reader.  Will return None if there is an incomplete
    frame in the stream.

    Raise MissingFooter if there's a problem reading the footer byte.
    '''
    frame_type = reader.read_octet()
    channel_id = reader.read_short()
    size = reader.read_long()
    
    payload = Reader(reader, reader.tell(), size)

    # Seek to end of payload
    reader.seek( size, 1 )

    ch = reader.read_octet()  # footer
    if ch != 0xce:
      raise Frame.FormatError(
        'Framing error, unexpected byte: %x.  frame type %x. channel %d, payload size %d',
        ch, frame_type, channel_id, size )

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

  def __repr__(self):
    # Have to actually call the method rather than __repr__==__str__ because
    # subclasses overload __str__
    return str(self)

  def write_frame(self, stream):
    '''
    Write this frame.
    '''
    raise NotImplementedError()
