
import struct
from haigha.lib.writer import Writer
from haigha.lib.reader import Reader
from haigha.lib.frames.frame import Frame

class HeaderFrame(Frame):
  '''
  Header frame for content.
  '''
  PROPERTIES = [                      \
    ('content_type', 'shortstr'),     \
    ('content_encoding', 'shortstr'), \
    ('application_headers', 'table'), \
    ('delivery_mode', 'octet'),       \
    ('priority', 'octet'),            \
    ('correlation_id', 'shortstr'),   \
    ('reply_to', 'shortstr'),         \
    ('expiration', 'shortstr'),       \
    ('message_id', 'shortstr'),       \
    ('timestamp', 'timestamp'),       \
    ('type', 'shortstr'),             \
    ('user_id', 'shortstr'),          \
    ('app_id', 'shortstr'),           \
    ('cluster_id', 'shortstr')        \
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
    class_id, weight, size = struct.unpack( '>HHQ', payload[:12] )

    r = Reader(payload[12:])

    #
    # Read 16-bit shorts until we get one with a low bit set to zero
    # TODO: decipher this and clean it up
    #
    flags = []
    while True:
      flag_bits = r.read_short()
      flags.append(flag_bits)
      if flag_bits & 1 == 0:
        break

    shift = 0
    d = {}
    for key, proptype in self.PROPERTIES:
      if shift == 0:
        if not flags:
          break
        flag_bits, flags = flags[0], flags[1:]
        shift = 15
      if flag_bits & (1 << shift):
        d[key] = getattr(r, 'read_' + proptype)()
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

  def write_frame(self, stream):
    writer = Writer()
    writer.write_octet( 2 )
    writer.write_short( self.channel_id )
    writer.flush( stream )
  
    stream_args_len_pos = stream.tell()
    writer = Writer()
    writer.write_long(0)
    writer.flush( stream )
    
    stream_method_pos = stream.tell()

    writer = Writer()
    writer.write_short( self._class_id )
    writer.write_short( self._weight )
    writer.write_longlong( self._size )
    writer.flush(stream)
    stream_end_args_pos = stream.tell()

    ### msg._serialzie_props
    # TODO: decipher this and clean it up
    # NOTE: As near as I can tell, we could loop through and write all the bits
    # directly, queing up the list of types and values as we do, and then write
    # them raw.  This bit about "15" is really because there are two expected
    # bitfields to start a message.
    shift = 15
    flag_bits = 0
    flags = []
    stack = []
    for key, proptype in self.PROPERTIES:
      val = self._properties.get(key, None)
      if val is not None:
        if shift == 0:
          flags.append(flag_bits)
          flag_bits = 0
          shift = 15

        # NOTE: I saw this error at TM, need to investigate @AW
        # /usr/local/lib/python2.6/site-packages/Hydra-1.5.18-py2.6.egg/hydra/core/amqp/evamqp/message.py:120:              DeprecationWarning: 'i' format requires -2147483648 <= number <= 2147483647
        flag_bits |= (1 << shift)
        if proptype != 'bit':
          #getattr(raw_bytes, 'write_' + proptype)(val)
          stack.append( ('write_%s'%(proptype), val) )

      shift -= 1

    flags.append(flag_bits)
    writer = Writer()
    for flag_bits in flags:
      writer.write_short(flag_bits)
    for method,val in stack:
      getattr(writer, method)( val )
    writer.flush( stream )
    stream_end_args_pos = stream.tell()
    # END msg.serialize_props

    # Now go back and write the current length
    stream_len = stream_end_args_pos - stream_method_pos

    # Seek all the way back to when we started writing the arguments and
    # write the total length of the bytes we wrote.
    writer = Writer()
    writer.write_long( stream_len )
    stream.seek( stream_args_len_pos )
    writer.flush( stream )

    # Seek to end and write the footer
    stream.seek( 0, 2 )
    stream.write('\xce')

HeaderFrame.register()
