
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
    ('content_type', 'shortstr', Reader.read_shortstr, Writer.write_shortstr),
    ('content_encoding', 'shortstr', Reader.read_shortstr, Writer.write_shortstr),
    ('application_headers', 'table', Reader.read_table, Writer.write_table),
    ('delivery_mode', 'octet', Reader.read_octet, Writer.write_octet),
    ('priority', 'octet', Reader.read_octet, Writer.write_octet),
    ('correlation_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr),
    ('reply_to', 'shortstr', Reader.read_shortstr, Writer.write_shortstr),
    ('expiration', 'shortstr', Reader.read_shortstr, Writer.write_shortstr),
    ('message_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr),
    ('timestamp', 'timestamp', Reader.read_timestamp, Writer.write_timestamp),
    ('type', 'shortstr', Reader.read_shortstr, Writer.write_shortstr),
    ('user_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr),
    ('app_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr),
    ('cluster_id', 'shortstr', Reader.read_shortstr, Writer.write_shortstr)
  ]


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
    #class_id, weight, size = struct.unpack( '>HHQ', payload[:12] )
    class_id = payload.read_short()
    weight = payload.read_short()
    size = payload.read_longlong()

    #r = Reader(payload[12:])

    # The AMQP spec is overly-complex when it comes to handling header frames.
    # The spec says that in addition to the first 16bit field, additional ones
    # can follow which /may/ then be in the property list (because bit flags
    # aren't in the list).  Properly implementing custom values requires the
    # ability 

    flags = []
    while True:
      flag_bits = payload.read_short()
      flags.append(flag_bits)
      if flag_bits & 1 == 0:
        break

    shift = 0
    d = {}
    for key, proptype, rfunc, wfunc in self.PROPERTIES:
    #for prop in self.PROPERTIES:
      if shift == 0:
        if not flags:
          break
        flag_bits, flags = flags[0], flags[1:]
        shift = 15
      if flag_bits & (1 << shift):
        #d[key] = getattr(payload, 'read_' + proptype)()
        d[ key ] = rfunc( payload )
      shift -= 1

    return HeaderFrame( channel_id, class_id, weight, size, d)
    
  def __init__(self, channel_id, class_id, weight, size, properties={}):
    Frame.__init__(self, channel_id)
    self._class_id = class_id
    self._weight = weight
    self._size = size
    self._properties = properties

  def __str__(self):
    return "%s[channel: %d, class_id: %d, weight: %d, size: %d, properties: %s]"%( self.__class__.__name__, self.channel_id, self._class_id, self._weight, self._size, self._properties )

  def write_frame(self, buf):
    writer = Writer(buf)
    writer.write_octet( self.type() )
    writer.write_short( self.channel_id )
    #writer.flush( stream )
  
    #stream_args_len_pos = stream.tell()
    stream_args_len_pos = len(buf)
    #writer = Writer()
    writer.write_long(0)
    #writer.flush( stream )
    
    #stream_method_pos = stream.tell()
    stream_method_pos = len(buf)

    #writer = Writer()
    writer.write_short( self._class_id )
    writer.write_short( self._weight )
    writer.write_longlong( self._size )
    #writer.flush(stream)
    #stream_end_args_pos = stream.tell()
    stream_end_args_pos = len(buf)

    shift = 15
    flag_bits = 0
    flags = []
    stack = deque()
    for key, proptype, rfunc, wfunc in self.PROPERTIES:
    #for prop in self.PROPERTIES:
      val = self._properties.get(key, None)
      if val is not None:
        if shift == 0:
          flags.append(flag_bits)
          flag_bits = 0
          shift = 15

        flag_bits |= (1 << shift)
        #if proptype != 'bit':
        #  #getattr(raw_bytes, 'write_' + proptype)(val)
        #  stack.append( ('write_%s'%(proptype), val) )
        stack.append( (wfunc, val) )

      shift -= 1

    flags.append(flag_bits)
    #writer = Writer()
    for flag_bits in flags:
      #writer.write_short(flag_bits)
      writer.write_short(flag_bits)
    for method,val in stack:
      #getattr(writer, method)( val )
      method(writer, val)
    #writer.flush( stream )
    #stream_end_args_pos = stream.tell()
    stream_end_args_pos = len(buf)
    # END msg.serialize_props

    # Now go back and write the current length
    stream_len = stream_end_args_pos - stream_method_pos

    # Seek all the way back to when we started writing the arguments and
    # write the total length of the bytes we wrote.
    #writer = Writer()
    #writer.write_long( stream_len )
    #stream.seek( stream_args_len_pos )
    #writer.flush( stream )
    writer.write_long_at( stream_len, stream_args_len_pos )

    # Seek to end and write the footer
    #stream.seek( 0, 2 )
    #stream.write('\xce')
    writer.write_octet( 0xce )

HeaderFrame.register()
