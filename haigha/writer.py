"""
Definition of the Writer class.
"""

from struct import pack
from time import mktime
from datetime import datetime
from decimal import Decimal

from StringIO import StringIO

class Writer(object):
  """
  Implements writing of structured data.  Operates as in 2 passes; first a write
  which buffers the objects and their format, and a second which flushes the
  structured data to a stream.  This setup allows for classes to make use of the
  Writer without needing to have a direct handle to an output buffer, or in cases
  where it's necessary to have finite control over the stream cursor.
  """

  def __init__(self):
    self._output_buffer = []

  def flush(self, stream):
    '''Flush the output to the stream. Stream must be a file object.'''
    self._bitcount = 0
    self._bitfield = 0
    for cb,data in self._output_buffer:
      cb( data, stream )

    self._flush_bits( stream )

  # NOTE: This is no longer needed, but we need to rethink how Message._serialize_properties
  # and Channel frame/method/content sending is done.  If necessary:
  #   stream = StringIO()
  #   flush(stream)
  #   return stream.getvalue()
  def __str__(self):
    stream = StringIO()
    self.flush( stream )
    return stream.getvalue()


  def write(self, s):
    """
    Write a plain Python string, with no special encoding.
    """
    self._output_buffer.append( (self._write_str, s) )

  def _write_str(self, s, stream):
    '''Write the string to a stream.'''
    self._flush_bits( stream )
    stream.write(s)

  def write_bit(self, b):
    """
    Write a boolean value.
    """
    self._output_buffer.append( (self._write_bit, b) )

  def _write_bit(self, b, stream):
    '''Write a bit to the output stream.  Will actually pool all the bits into
    a byte and then flush when that's full.  More common case is that we pack a
    few bits, then the next data type will flush that byte.'''
    if b:
      b = 1
    else:
      b = 0
    shift = self._bitcount % 8
    if shift == 0 and self._bitcount > 0:
      self._flush_bits( stream )

    self._bitfield |= (b << shift)
    self._bitcount += 1

  def _flush_bits(self, stream):
    '''Flush the bits that have been packed into the current byte and reset state.'''
    # Because this can be called many times when writing a table, be safe
    if self._bitcount > 0:
      stream.write(pack('B', self._bitfield))
      self._bitfield = 0
      self._bitcount = 0

  def write_octet(self, n):
    """
    Write an integer as an unsigned 8-bit value.
    """
    if (n < 0) or (n > 255):
      raise ValueError('Octet %d out of range 0..255', n)
    self._output_buffer.append( (self._write_octet, n) )

  def _write_octet(self, n, stream):
    '''Write an octet to the output stream.'''
    if (n < 0) or (n > 255):
      raise ValueError('Octet %d out of range 0..255', n)
    self._flush_bits( stream )
    stream.write(pack('B', n))

  def write_short(self, n):
    """
    Write an integer as an unsigned 16-bit value.
    """
    if (n < 0) or (n > 65535):
      raise ValueError('Octet %d out of range 0..65535', n)
    self._output_buffer.append( (self._write_short, n) )

  def _write_short(self, n, stream):
    '''Write the short to the output stream.'''
    if (n < 0) or (n > 65535):
      raise ValueError('Octet %d out of range 0..65535', n)
    self._flush_bits( stream )
    stream.write(pack('>H', n))

  def write_long(self, n):
    """
    Write an integer as an unsigned2 32-bit value.
    """
    if (n < 0) or (n >= (2**32)):
      raise ValueError('Octet %d out of range 0..2**31-1', n)
    self._output_buffer.append( (self._write_long, n) )

  def _write_long(self, n, stream):
    '''Write the long to a stream.'''    
    if (n < 0) or (n >= (2**32)):
      raise ValueError('Octet %d out of range 0..2**31-1', n)
    self._flush_bits( stream )
    stream.write(pack('>I', n))

  def write_longlong(self, n):
    """
    Write an integer as an unsigned 64-bit value.
    """
    if (n < 0) or (n >= (2**64)):
      raise ValueError('Octet %d out of range 0..2**64-1', n)
    self._output_buffer.append( (self._write_longlong, n) )

  def _write_longlong(self, n, stream):
    '''Write the longlong to a stream.'''
    if (n < 0) or (n >= (2**64)):
      raise ValueError('Octet %d out of range 0..2**64-1', n)
    self._flush_bits( stream )
    stream.write(pack('>Q', n))

  def write_shortstr(self, s):
    """
    Write a string up to 255 bytes long after encoding.  If passed
    a unicode string, encode as UTF-8.
    """
    if isinstance(s, unicode):
      s = s.encode('utf-8')
    if len(s) > 255:
      raise ValueError('String too long')
    self._output_buffer.append( (self._write_shortstr, s) )

  def _write_shortstr(self, s, stream):
    '''Write the string to the stream.'''
    if len(s) > 255:
      raise ValueError('String too long')
    self._flush_bits( stream )
    self._write_octet(len(s), stream) # also flushes bits
    stream.write(s)

  def write_longstr(self, s):
    """
    Write a string up to 2**32 bytes long after encoding.  If passed
    a unicode string, encode as UTF-8.
    """
    if isinstance(s, unicode):
      s = s.encode('utf-8')
    self._output_buffer.append( (self._write_longstr, s) )

  def _write_longstr(self, s, stream):
    '''Write the long string to the stream.'''
    self._flush_bits( stream )
    self._write_long(len(s), stream) # also flushes bits
    stream.write(s)

  def write_table(self, d):
    """
    Write out a Python dictionary made of up string keys, and values
    that are strings, signed integers, Decimal, datetime.datetime, or
    sub-dictionaries following the same constraints.
    """
    self._output_buffer.append( (self._write_table, d) )

  def _write_table(self, d, stream):
    '''Write the table to an output stream.'''
    # HACK: encoding of AMQP tables is broken because it requires the length of
    # the /encoded/ data instead of the number of items.  To support streaming,
    # fiddle with cursor position, rewinding to write the real length of the
    # data.  Generally speaking, I'm not a fan of the AMQP encoding scheme, it
    # could be much faster.
    self._flush_bits( stream )
    table_len_pos = stream.tell()
    self._write_long( 0, stream )
    table_data_pos = stream.tell()
    
    for k, v in d.items():
      # 6 April 09 aaron - Don't send table key unless the data type is
      # supported.
      if isinstance(v, basestring):
        if isinstance(v, unicode):
          v = v.encode('utf-8')
        self._write_shortstr(k, stream)
        stream.write('S')
        self._write_longstr(v, stream)
      elif isinstance(v, (int, long)):
        self._write_shortstr(k, stream)
        stream.write('I')
        stream.write(pack('>i', v))
      elif isinstance(v, Decimal):
        self._write_shortstr(k, stream)
        stream.write('D')
        sign, digits, exponent = v.as_tuple()
        v = 0
        for d in digits:
          v = (v * 10) + d
        if sign:
          v = -v
        self._write_octet(-exponent, stream)
        stream.write(pack('>i', v))
      elif isinstance(v, datetime):
        self._write_shortstr(k, stream)
        stream.write('T')
        self._write_timestamp(v, stream)
      elif isinstance(v, dict):
        self._write_shortstr(k, stream)
        stream.write('F')
        self._write_table(v, stream)
    table_end_pos = stream.tell()
    table_len = table_end_pos - table_data_pos
    stream.seek( table_len_pos )

    self._write_long( table_len, stream )
    stream.seek( 0, 2 )

  def write_timestamp(self, t):
    """
    Write out a Python datetime.datetime object as a 64-bit integer
    representing seconds since the Unix epoch.
    """
    self._output_buffer.append( (self._write_timestamp, t) )

  def _write_timestamp(self, v, stream):
    '''Write the timestamp to a stream.'''
    stream.write(pack('>q', long(mktime(v.timetuple()))))
