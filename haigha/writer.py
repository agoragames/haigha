'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from struct import Struct, pack
from time import mktime
from datetime import datetime
from decimal import Decimal
from operator import xor
    
class Writer(object):
  """
  Implements writing of structured AMQP data. Buffers data directly to a 
  bytearray or a buffer supplied in the constructor. The buffer must
  supply append, extend and struct.pack_into semantics.
  """

  def __init__(self, buf=None):
    if buf is not None: 
      self._output_buffer = buf
    else:
      self._output_buffer = bytearray()

  def __str__(self):
    return ''.join( ['\\x%s'%(chr(c).encode('hex')) for c in self._output_buffer] )

  __repr__ = __str__

  def __eq__(self, other):
    if isinstance(other, Writer):
      return self._output_buffer == other._output_buffer
    return False

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
    return self

  def write_bits(self, *args):
    '''
    Write multiple bits in a single byte field. The bits will be written in
    little-endian order, but should be supplied in big endian order. Will raise
    ValueError when more than 8 arguments are supplied.

    write_bits(True, False) => 0x02
    '''
    # Would be nice to make this a bit smarter
    if len(args) > 8:
      raise ValueError("Can only write 8 bits at a time")

    self._output_buffer.append( chr(
      reduce(lambda x,y: xor(x,args[y]<<y), xrange(len(args)), 0)) )

    return self

  def write_bit(self, b, pack=Struct('B').pack):
    '''
    Write a single bit. Convenience method for single bit args.
    '''
    self._output_buffer.append( pack(True if b else False) )
    return self

  def write_octet(self, n, pack=Struct('B').pack):
    """
    Write an integer as an unsigned 8-bit value.
    """
    if 0 <= n <= 255:
      self._output_buffer.append( pack(n) )
    else:
      raise ValueError('Octet %d out of range 0..255', n)
    return self

  def write_short(self, n, pack=Struct('>H').pack):
    """
    Write an integer as an unsigned 16-bit value.
    """
    if 0 <= n <= 0xFFFF:
      self._output_buffer.extend( pack(n) )
    else:
      raise ValueError('Short %d out of range 0..0xFFFF', n)
    return self

  def write_short_at(self, n, pos, pack_into=Struct('>H').pack_into):
    '''
    Write an unsigned 16bit value at a specific position in the buffer.
    Used for writing tables and frames.
    '''
    if 0 <= n <= 0xFFFF:
      pack_into(self._output_buffer, pos, n)
    else:
      raise ValueError('Short %d out of range 0..0xFFFF', n)
    return self

  def write_long(self, n, pack=Struct('>I').pack):
    """
    Write an integer as an unsigned 32-bit value.
    """
    if 0 <= n <= 0xFFFFFFFF:
      self._output_buffer.extend( pack(n) )
    else:
      raise ValueError('Long %d out of range 0..0xFFFFFFFF', n)
    return self

  def write_long_at(self, n, pos, pack_into=Struct('>I').pack_into):
    '''
    Write an unsigned 32bit value at a specific position in the buffer.
    Used for writing tables and frames.
    '''
    if 0 <= n <= 0xFFFFFFFF:
      pack_into(self._output_buffer, pos, n)
    else:
      raise ValueError('Long %d out of range 0..0xFFFFFFFF', n)
    return self

  def write_longlong(self, n, pack=Struct('>Q').pack):
    """
    Write an integer as an unsigned 64-bit value.
    """
    if 0 <= n <= 0xFFFFFFFFFFFFFFFF:
      self._output_buffer.extend( pack(n) )
    else:
      raise ValueError('Longlong %d out of range 0..0xFFFFFFFFFFFFFFFF', n)
    return self

  def write_shortstr(self, s):
    """
    Write a string up to 255 bytes long after encoding.  If passed
    a unicode string, encode as UTF-8.
    """
    if isinstance(s, unicode):
      s = s.encode('utf-8')
    self.write_octet( len(s) )
    self.write( s )
    return self

  def write_longstr(self, s):
    """
    Write a string up to 2**32 bytes long after encoding.  If passed
    a unicode string, encode as UTF-8.
    """
    if isinstance(s, unicode):
      s = s.encode('utf-8')
    self.write_long( len(s) )
    self.write( s )
    return self

  def write_timestamp(self, t, pack=Struct('>Q').pack):
    """
    Write out a Python datetime.datetime object as a 64-bit integer
    representing seconds since the Unix epoch.
    """
    # Double check timestamp, can't imagine why it would be signed
    self._output_buffer.extend( pack(long(mktime(t.timetuple())) ))
    return self

  # NOTE: coding this to http://dev.rabbitmq.com/wiki/Amqp091Errata#section_3 and
  # NOT spec 0.9.1. It seems that Rabbit and other brokers disagree on this
  # section for now.
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

    for key,value in d.iteritems():
      self._write_item( key, value )
    
    table_end_pos = len(self._output_buffer)
    table_len = table_end_pos - table_data_pos
    
    self.write_long_at( table_len, table_len_pos )
    return self

  def _write_item(self, key, value ):
    self.write_shortstr(key)
    self._write_field( value )

  def _write_field(self, value ):
    writer = self.field_type_map.get( type(value) )
    if writer:
      writer(self, value)
    else:
      # Write a None because we've already written a key
      self._field_none( value )

  def _field_bool(self, val, pack=Struct('B').pack):
    self._output_buffer.append( 't' )
    self._output_buffer.append( pack( True if val else False ) )

  def _field_int(self, val, short_pack=Struct('>h').pack, \
  int_pack=Struct('>i').pack, long_pack=Struct('>q').pack):
    if -2**15 <= val < 2**15:
      self._output_buffer.append( 's' )
      self._output_buffer.extend( short_pack(val) )
    elif -2**31 <= val < 2**31:
      self._output_buffer.append( 'I' )
      self._output_buffer.extend( int_pack(val) )
    else:
      self._output_buffer.append( 'l' )
      self._output_buffer.extend( long_pack(val) )

  def _field_double(self, val, pack=Struct('>d').pack):
    self._output_buffer.append( 'd' )
    self._output_buffer.extend( pack(val) )

  # Coding to http://dev.rabbitmq.com/wiki/Amqp091Errata#section_3 which
  # differs from spec in that the value is signed.
  def _field_decimal(self, val, exp_pack=Struct('B').pack, dig_pack=Struct('>i').pack):
    self._output_buffer.append('D')
    sign, digits, exponent = val.as_tuple()
    v = 0
    for d in digits:
      v = (v * 10) + d
    if sign:
      v = -v
    self._output_buffer.append( exp_pack(-exponent) )
    self._output_buffer.extend( dig_pack(v) )

  def _field_str(self, val):
    self._output_buffer.append('S')
    self.write_longstr(val)

  def _field_unicode(self, val):
    val = val.encode('utf-8')
    self._output_buffer.append('S')
    self.write_longstr( val )

  def _field_timestamp(self, val):
    self._output_buffer.append( 'T' )
    self.write_timestamp( val )

  def _field_table(self, val):
    self._output_buffer.append('F')
    self.write_table( val )

  def _field_none(self, val):
    self._output_buffer.append('V')

  def _field_bytearray(self, val):
    self._output_buffer.append('x')
    self.write_longstr( val )

  def _field_iterable(self, val):
    self._output_buffer.append( 'A' )
    for x in val:
      self._write_field( x )
  
  field_type_map = {
    bool      : _field_bool,
    int       : _field_int,
    long      : _field_int,
    float     : _field_double,
    Decimal   : _field_decimal,
    str       : _field_str,
    unicode   : _field_unicode,
    datetime  : _field_timestamp,
    dict      : _field_table,
    None      : _field_none,
    bytearray : _field_bytearray,
  }

  # 0.9.1 spec mapping
  # field_type_map = {
  #   bool      : _field_bool,
  #   int       : _field_int,
  #   long      : _field_int,
  #   float     : _field_double,
  #   Decimal   : _field_decimal,
  #   str       : _field_str,
  #   unicode   : _field_unicode,
  #   datetime  : _field_timestamp,
  #   dict      : _field_table,
  #   None      : _field_none,
  #   bytearray : _field_bytearray,
  #   list      : _field_iterable,
  #   tuple     : _field_iterable,
  #   set       : _field_iterable,
  # }
