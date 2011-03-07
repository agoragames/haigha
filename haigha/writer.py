"""
Definition of the Writer class.
"""

from struct import pack, pack_into, Struct
from time import mktime
from datetime import datetime
from decimal import Decimal
from operator import xor
    
class Writer(object):
  """
  Implements writing of structured data.  Operates as in 2 passes; first a write
  which buffers the objects and their format, and a second which flushes the
  structured data to a stream.  This setup allows for classes to make use of the
  Writer without needing to have a direct handle to an output buffer, or in cases
  where it's necessary to have finite control over the stream cursor.
  """

  def __init__(self, buf=None):
    if buf is not None: 
      self._output_buffer = buf
    else:
      self._output_buffer = bytearray()

  def __str__(self):
    return ''.join( ['\\x%s'%(chr(c).encode('hex')) for c in self._output_buffer] )

  def buffer(self):
    '''
    Get the buffer that this has written to. Returns bytearray.
    '''
    return self._output_buffer

  def write(self, s):
    """
    Write a plain Python string, with no special encoding.
    """
    self._output_buffer.extend( s )

  def write_bits(self, *args):
    # Would be nice to make this a bit smarter
    if len(args) > 8:
      raise ValueError("Can only write 8 bits at a time")

    # It would be awesome if pack_into would understand how to extend
    self._output_buffer.append( pack('B',
      reduce(lambda x,y: xor(x,args[y]<<y), xrange(len(args)), 0)) )

  def write_bit(self, b, packer=Struct('B')):
    '''
    Write a single bit. Convenience method for single bit args.
    '''
    self._output_buffer.append( packer.pack(b) )

  def write_octet(self, n, packer=Struct('B')):
    """
    Write an integer as an unsigned 8-bit value.
    """
    if 0 <= n <= 255:
      self._output_buffer.append( packer.pack(n) )
    else:
      raise ValueError('Octet %d out of range 0..255', n)

  def write_short(self, n, packer=Struct('>H')):
    """
    Write an integer as an unsigned 16-bit value.
    """
    if 0 <= n <= 0xFFFF:
      self._output_buffer.extend( packer.pack(n) )
    else:
      raise ValueError('Octet %d out of range 0..0xFFFF', n)

  def write_long(self, n, packer=Struct('>I')):
    """
    Write an integer as an unsigned 32-bit value.
    """
    if 0 <= n <= 0xFFFFFFFF:
      self._output_buffer.extend( packer.pack(n) )
    else:
      raise ValueError('Octet %d out of range 0..0xFFFFFFFF', n)

  def write_long_at(self, n, pos, packer=Struct('>I')):
    '''
    Write an unsigned 32bit value at a specific position in the buffer.
    Used for writing tables and frames.
    '''
    if 0 <= n <= 0xFFFFFFFF:
      packer.pack_into(self._output_buffer, pos, n)
    else:
      raise ValueError('Octet %d out of range 0..0xFFFFFFFF', n)

  def write_longlong(self, n, packer=Struct('>Q')):
    """
    Write an integer as an unsigned 64-bit value.
    """
    if 0 <= n <= 0xFFFFFFFFFFFFFFFF:
      self._output_buffer.extend( packer.pack(n) )
    else:
      raise ValueError('Octet %d out of range 0..0xFFFFFFFFFFFFFFFF', n)

  def write_shortstr(self, s):
    """
    Write a string up to 255 bytes long after encoding.  If passed
    a unicode string, encode as UTF-8.
    """
    if isinstance(s, unicode):
      s = s.encode('utf-8')
    self.write_octet( len(s) )
    self.write( s )

  def write_longstr(self, s):
    """
    Write a string up to 2**32 bytes long after encoding.  If passed
    a unicode string, encode as UTF-8.
    """
    if isinstance(s, unicode):
      s = s.encode('utf-8')
    self.write_long( len(s) )
    self.write( s )

  def write_table(self, d):
    """
    Write out a Python dictionary made of up string keys, and values
    that are strings, signed integers, Decimal, datetime.datetime, or
    sub-dictionaries following the same constraints.
    """
    # HACK: encoding of AMQP tables is broken because it requires the length of
    # the /encoded/ data instead of the number of items.  To support streaming,
    # fiddle with cursor position, rewinding to write the real length of the
    # data.  Generally speaking, I'm not a fan of the AMQP encoding scheme, it
    # could be much faster.
    table_len_pos = len( self._output_buffer )
    self.write_long( 0 )
    table_data_pos = len(self._output_buffer )
    
    for k, v in d.iteritems():
      # 6 April 09 aaron - Don't send table key unless the data type is
      # supported.
      if isinstance(v, basestring):
        if isinstance(v, unicode):
          v = v.encode('utf-8')
        self.write_shortstr(k)
        self._output_buffer.append('S')
        self.write_longstr(v)
      elif isinstance(v, (int, long)):
        self.write_shortstr(k)
        self._output_buffer.append('I')
        self._output_buffer.extend(pack('>i', v))
      elif isinstance(v, Decimal):
        self.write_shortstr(k)
        self._output_buffer.append('D')
        sign, digits, exponent = v.as_tuple()
        v = 0
        for d in digits:
          v = (v * 10) + d
        if sign:
          v = -v
        self.write_octet(-exponent)
        self._output_buffer.extend(pack('>i', v))
      elif isinstance(v, datetime):
        self.write_shortstr(k)
        self._output_buffer.append('T')
        self.write_timestamp(v)
      elif isinstance(v, dict):
        self.write_shortstr(k)
        self._output_buffer.append('F')
        self.write_table(v)
    table_end_pos = len(self._output_buffer)
    table_len = table_end_pos - table_data_pos
    
    self.write_long_at( table_len, table_len_pos )

  def write_timestamp(self, t):
    """
    Write out a Python datetime.datetime object as a 64-bit integer
    representing seconds since the Unix epoch.
    """
    # Double check timestamp, can't imagine why it would be signed
    self._output_buffer.extend( pack('>Q', long(mktime(t.timetuple()))) )
