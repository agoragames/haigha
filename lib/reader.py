"""
Defines the Reader class.
"""

from cStringIO import StringIO  # TODO: make 2.6 and 3.0 compatible
from struct import unpack
from datetime import datetime
from decimal import Decimal

class Reader(object):
  """
  Parse data from AMQP
  """

  def __init__(self, source):
    """
    source should be either a file-like object with a read() method, or
    a plain (non-unicode) string.

    """
    if isinstance(source, str):
      self.input = StringIO(source)
    elif hasattr(source, 'read'):
      self.input = source
    else:
      raise ValueError('Reader needs a file-like object or plain string')

    self.bitcount = self.bits = 0

  def __str__(self):
    # TODO: make this portable if someone passed a non-StringIO into the ctor
    return ''.join( ['\\x%s'%(c.encode('hex')) for c in self.input.getvalue()] )

  def close(self):
    self.input.close()

  def read(self, n):
    """
    Read n bytes.

    """
    self.bitcount = self.bits = 0
    return self.input.read(n)

  def read_bit(self):
    """
    Read a single boolean value.

    """
    if not self.bitcount:
      self.bits = ord(self.input.read(1))
      self.bitcount = 8
    result = (self.bits & 1) == 1
    self.bits >>= 1
    self.bitcount -= 1
    return result

  def read_octet(self):
    """
    Read one byte, return as an integer

    """
    self.bitcount = self.bits = 0
    return unpack('B', self.input.read(1))[0]

  def read_short(self):
    """
    Read an unsigned 16-bit integer

    """
    self.bitcount = self.bits = 0
    return unpack('>H', self.input.read(2))[0]

  def read_long(self):
    """
    Read an unsigned 32-bit integer

    """
    self.bitcount = self.bits = 0
    return unpack('>I', self.input.read(4))[0]

  def read_longlong(self):
    """
    Read an unsigned 64-bit integer

    """
    self.bitcount = self.bits = 0
    return unpack('>Q', self.input.read(8))[0]

  def read_shortstr(self):
    """
    Read a utf-8 encoded string that's stored in up to
    255 bytes.  Return it decoded as a Python unicode object.

    """
    self.bitcount = self.bits = 0
    slen = unpack('B', self.input.read(1))[0]
    return self.input.read(slen).decode('utf-8')

  def read_longstr(self):
    """
    Read a string that's up to 2**32 bytes, the encoding
    isn't specified in the AMQP spec, so just return it as
    a plain Python string.

    """
    self.bitcount = self.bits = 0
    slen = unpack('>I', self.input.read(4))[0]
    return self.input.read(slen)

  def read_table(self):
    """
    Read an AMQP table, and return as a Python dictionary.

    """
    self.bitcount = self.bits = 0
    tlen = unpack('>I', self.input.read(4))[0]
    table_data = Reader(self.input.read(tlen))
    result = {}
    while table_data.input.tell() < tlen:
      name = table_data.read_shortstr()
      ftype = table_data.input.read(1)
      if ftype == 'S':
        val = table_data.read_longstr()
      elif ftype == 'I':
        val = unpack('>i', table_data.input.read(4))[0]
      elif ftype == 'D':
        d = table_data.read_octet()
        n = unpack('>i', table_data.input.read(4))[0]
        val = Decimal(n) / Decimal(10 ** d)
      elif ftype == 'T':
        val = table_data.read_timestamp()
      elif ftype == 'F':
        val = table_data.read_table() # recurse
      result[name] = val
    return result

  def read_timestamp(self):
    """
    Read and AMQP timestamp, which is a 64-bit integer representing
    seconds since the Unix epoch in 1-second resolution.  Return as
    a Python datetime.datetime object, expressed as localtime.

    """
    return datetime.fromtimestamp(self.read_longlong())
