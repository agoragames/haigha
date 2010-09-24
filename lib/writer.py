"""
Definition of the Writer class.
"""

from cStringIO import StringIO  # TODO: make 2.6 and 3.0 compatible
from struct import pack
from time import mktime
from datetime import datetime
from decimal import Decimal

class Writer(object):
  """
  A StringIO like object for building up encoded AMQP data.
  """

  def __init__(self):
    self.out = StringIO()
    self.bits = []
    self.bitcount = 0

  def _flushbits(self):
    if self.bits:
      for b in self.bits:
        self.out.write(pack('B', b))
      self.bits = []
      self.bitcount = 0

  def getvalue(self):
    """
    Get what's been encoded so far.

    """
    self._flushbits()
    return self.out.getvalue()

  def write(self, s):
    """
    Write a plain Python string, with no special encoding.

    """
    self._flushbits()
    self.out.write(s)

  def write_bit(self, b):
    """
    Write a boolean value.
    """
    if b:
      b = 1
    else:
      b = 0
    shift = self.bitcount % 8
    if shift == 0:
      self.bits.append(0)
    self.bits[-1] |= (b << shift)
    self.bitcount += 1

  def write_octet(self, n):
    """
    Write an integer as an unsigned 8-bit value.
    """
    if (n < 0) or (n > 255):
      raise ValueError('Octet out of range 0..255')
    self._flushbits()
    self.out.write(pack('B', n))

  def write_short(self, n):
    """
    Write an integer as an unsigned 16-bit value.
    """
    if (n < 0) or (n > 65535):
      raise ValueError('Octet out of range 0..65535')
    self._flushbits()
    self.out.write(pack('>H', n))

  def write_long(self, n):
    """
    Write an integer as an unsigned2 32-bit value.

    """
    if (n < 0) or (n >= (2**32)):
      raise ValueError('Octet out of range 0..2**31-1')
    self._flushbits()
    self.out.write(pack('>I', n))

  def write_longlong(self, n):
    """
    Write an integer as an unsigned 64-bit value.

    """
    if (n < 0) or (n >= (2**64)):
      raise ValueError('Octet out of range 0..2**64-1')
    self._flushbits()
    self.out.write(pack('>Q', n))

  def write_shortstr(self, s):
    """
    Write a string up to 255 bytes long after encoding.  If passed
    a unicode string, encode as UTF-8.

    """
    self._flushbits()
    if isinstance(s, unicode):
      s = s.encode('utf-8')
    if len(s) > 255:
      raise ValueError('String too long')
    self.write_octet(len(s))
    self.out.write(s)

  def write_longstr(self, s):
    """
    Write a string up to 2**32 bytes long after encoding.  If passed
    a unicode string, encode as UTF-8.

    """
    self._flushbits()
    if isinstance(s, unicode):
      s = s.encode('utf-8')
    self.write_long(len(s))
    self.out.write(s)

  def write_table(self, d):
    """
    Write out a Python dictionary made of up string keys, and values
    that are strings, signed integers, Decimal, datetime.datetime, or
    sub-dictionaries following the same constraints.

    """
    self._flushbits()
    table_data = Writer()
    for k, v in d.items():
      # 6 April 09 aaron - Don't send table key unless the data type is
      # supported. TODO: an OO way of doing this.
      if isinstance(v, basestring):
        if isinstance(v, unicode):
          v = v.encode('utf-8')
        table_data.write_shortstr(k)
        table_data.write('S')
        table_data.write_longstr(v)
      elif isinstance(v, (int, long)):
        table_data.write_shortstr(k)
        table_data.write('I')
        table_data.write(pack('>i', v))
      elif isinstance(v, Decimal):
        table_data.write_shortstr(k)
        table_data.write('D')
        sign, digits, exponent = v.as_tuple()
        v = 0
        for d in digits:
          v = (v * 10) + d
        if sign:
          v = -v
        table_data.write_octet(-exponent)
        table_data.write(pack('>i', v))
      elif isinstance(v, datetime):
        table_data.write_shortstr(k)
        table_data.write('T')
        table_data.write_timestamp(v)
        ## FIXME: timezone ?
      elif isinstance(v, dict):
        table_data.write_shortstr(k)
        table_data.write('F')
        table_data.write_table(v)
    table_data = table_data.getvalue()
    self.write_long(len(table_data))
    self.out.write(table_data)

  def write_timestamp(self, v):
    """
    Write out a Python datetime.datetime object as a 64-bit integer
    representing seconds since the Unix epoch.

    """
    self.out.write(pack('>q', long(mktime(v.timetuple()))))
