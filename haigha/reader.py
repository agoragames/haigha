"""
Defines the Reader class.
"""

from cStringIO import StringIO
from struct import unpack
from datetime import datetime
from decimal import Decimal

class Reader(object):
  """
  Parse data from AMQP
  """

  class ReaderError(Exception): '''Base class for all reader errors.'''
  class BufferUnderflow(ReaderError): '''Not enough bytes to satisfy the request.'''
  class TableError(ReaderError): '''Unsupported table field type was read.'''

  def __init__(self, source):
    """
    source should be either a file-like object with a read() and tell() method, 
    a plain or unicode string.
    """
    if hasattr(source, 'read'):
      self.input = source
    elif isinstance(source, str):
      self.input = StringIO(source)
    elif isinstance(source, unicode):
      self.input = StringIO(source.encode('utf8'))
    else:
      raise ValueError('Reader needs a file-like object or plain string')

    self.bitcount = self.bits = 0

  def __str__(self):
    return ''.join( ['\\x%s'%(c.encode('hex')) for c in self.input.getvalue()] )

  def close(self):
    self.input.close()

  def read(self, n):
    """
    Read n bytes.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    """
    self.bitcount = self.bits = 0
    rval = self.input.read(n)
    if len(rval) != n:
      raise self.BufferUnderflow()
    return rval

  def read_bit(self):
    """
    Read a single boolean value, returns 0 or 1.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    """
    if not self.bitcount:
      self.bits = ord( self.read(1) )
      self.bitcount = 0xFF
    result = self.bits & 1
    self.bits >>= 1
    self.bitcount >>= 1
    return result

  def read_octet(self):
    """
    Read one byte, return as an integer

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    return unpack('B', self.read(1))[0]

  def read_short(self):
    """
    Read an unsigned 16-bit integer

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    return unpack('>H', self.read(2))[0]

  def read_long(self):
    """
    Read an unsigned 32-bit integer

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    return unpack('>I', self.read(4))[0]

  def read_longlong(self):
    """
    Read an unsigned 64-bit integer

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    return unpack('>Q', self.read(8))[0]

  def read_shortstr(self):
    """
    Read a utf-8 encoded string that's stored in up to
    255 bytes.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise UnicodeDecodeError if the text is mal-formed.
    Will raise struct.error if the data is malformed
    """
    slen = unpack('B', self.read(1))[0]
    return self.read(slen)

  def read_longstr(self):
    """
    Read a string that's up to 2**32 bytes, the encoding
    isn't specified in the AMQP spec, so just return it as
    a plain Python string.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    slen = unpack('>I', self.read(4))[0]
    return self.read(slen)

  def read_table(self):
    """
    Read an AMQP table, and return as a Python dictionary.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise UnicodeDecodeError if the text is mal-formed.
    Will raise struct.error if the data is malformed
    """
    tlen = unpack('>I', self.read(4))[0]
    end_pos = self.input.tell() + tlen
    result = {}
    while self.input.tell() < end_pos:
      name = self.read_shortstr()
      ftype = self.read(1)
      if ftype == 'S':
        val = self.read_longstr()
      elif ftype == 'I':
        val = unpack('>i', self.read(4))[0]
      elif ftype == 'D':
        d = self.read_octet()
        n = unpack('>i', self.read(4))[0]
        val = Decimal(n) / Decimal(10 ** d)
      elif ftype == 'T':
        val = self.read_timestamp()
      elif ftype == 'F':
        val = self.read_table()
      else:
        raise Reader.TableError('Unknown field type %s', ftype)
      result[name] = val
    return result

  def read_timestamp(self):
    """
    Read and AMQP timestamp, which is a 64-bit integer representing
    seconds since the Unix epoch in 1-second resolution.  Return as
    a Python datetime.datetime object, expressed as localtime.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    return datetime.fromtimestamp(self.read_longlong())
