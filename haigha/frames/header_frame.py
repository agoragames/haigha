'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

import struct
from collections import deque

from haigha.writer import Writer
from haigha.reader import Reader
from haigha.frames.frame import Frame

class HeaderFrame(Frame):
  '''
  Header frame for content.
  '''
  PROPERTIES = [
    ('content_type', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<15),
    ('content_encoding', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<14),
    ('application_headers', 'table', Reader.read_table, Writer.write_table, 1<<13),
    ('delivery_mode', 'octet', Reader.read_octet, Writer.write_octet, 1<<12),
    ('priority', 'octet', Reader.read_octet, Writer.write_octet, 1<<11),
    ('correlation_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<10),
    ('reply_to', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<9),
    ('expiration', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<8),
    ('message_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<7),
    ('timestamp', 'timestamp', Reader.read_timestamp, Writer.write_timestamp, 1<<6),
    ('type', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<5),
    ('user_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<4),
    ('app_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<3),
    ('cluster_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr, 1<<2)
  ]
  DEFAULT_PROPERTIES = True


  @classmethod
  def type(cls):
    return 2
  
  @property
  def class_id(self):
    return self._class_id
  
  @property
  def weight(self):
    return self._weight
  
  @property
  def size(self):
    return self._size

  @property
  def properties(self):
    return self._properties
  
  @classmethod
  def parse(self, channel_id, payload):
    '''
    Parse a header frame for a channel given a Reader payload.
    '''
    class_id = payload.read_short()
    weight = payload.read_short()
    size = payload.read_longlong()
    properties = {}

    # The AMQP spec is overly-complex when it comes to handling header frames.
    # The spec says that in addition to the first 16bit field, additional ones
    # can follow which /may/ then be in the property list (because bit flags
    # aren't in the list).  Properly implementing custom values requires the
    # ability change the properties and their types, which someone is welcome
    # to do, but seriously, what's the point? Because the complexity of parsing
    # and writing this frame directly impacts the speed at which messages can
    # be processed, there are two branches for both a fast parse which assumes
    # no changes to the properties and a slow parse. For now it's up to someone
    # using custom headers to flip the flag.
    if self.DEFAULT_PROPERTIES:
      flag_bits = payload.read_short()
      for key, proptype, rfunc, wfunc, mask in self.PROPERTIES:
        if flag_bits & mask:
          properties[ key ] = rfunc( payload )
    else:
      flags = []
      while True:
        flag_bits = payload.read_short()
        flags.append(flag_bits)
        if flag_bits & 1 == 0:
          break

      shift = 0
      for key, proptype, rfunc, wfunc, mask in self.PROPERTIES:
        if shift == 0:
          if not flags:
            break
          flag_bits, flags = flags[0], flags[1:]
          shift = 15
        if flag_bits & (1 << shift):
          properties[ key ] = rfunc( payload )
        shift -= 1

    return HeaderFrame( channel_id, class_id, weight, size, properties)
    
  def __init__(self, channel_id, class_id, weight, size, properties={}):
    Frame.__init__(self, channel_id)
    self._class_id = class_id
    self._weight = weight
    self._size = size
    self._properties = properties

  def __str__(self):
    return "%s[channel: %d, class_id: %d, weight: %d, size: %d, properties: %s]"%( self.__class__.__name__, self.channel_id, self._class_id, self._weight, self._size, self._properties )

  def write_frame(self, buf):
    '''
    Write the frame into an existing buffer.
    '''
    writer = Writer(buf)
    writer.write_octet( self.type() )
    writer.write_short( self.channel_id )
  
    # Track the position where we're going to write the total length
    # of the frame arguments.
    stream_args_len_pos = len(buf)
    writer.write_long(0)
    
    stream_method_pos = len(buf)

    writer.write_short( self._class_id )
    writer.write_short( self._weight )
    writer.write_longlong( self._size )

    # Like frame parsing, branch to faster code for default properties
    if self.DEFAULT_PROPERTIES:
      # Track the position where we're going to write the flags.
      flags_pos = len(buf)
      writer.write_short(0)
      flag_bits = 0
      for key, proptype, rfunc, wfunc, mask in self.PROPERTIES:
        val = self._properties.get(key, None)
        if val is not None:
          flag_bits |= mask
          wfunc(writer, val)
      writer.write_short_at( flag_bits, flags_pos )
    else:    
      shift = 15
      flag_bits = 0
      flags = []
      stack = deque()
      for key, proptype, rfunc, wfunc, mask in self.PROPERTIES:
        val = self._properties.get(key, None)
        if val is not None:
          if shift == 0:
            flags.append(flag_bits)
            flag_bits = 0
            shift = 15

          flag_bits |= (1 << shift)
          stack.append( (wfunc, val) )

        shift -= 1

      flags.append(flag_bits)
      for flag_bits in flags:
        writer.write_short(flag_bits)
      for method,val in stack:
        method(writer, val)

    # Write the total length back at the beginning of the frame
    stream_len = len(buf) - stream_method_pos
    writer.write_long_at( stream_len, stream_args_len_pos )

    writer.write_octet( 0xce )

HeaderFrame.register()
