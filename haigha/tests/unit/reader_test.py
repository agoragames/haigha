'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
from datetime import datetime
from io import BytesIO
from decimal import Decimal

from haigha.reader import Reader
import struct
import operator

class ReaderTest(Chai):

  def test_init(self):
    ba = Reader(bytearray('foo'))
    assert_true( isinstance(ba._input, buffer) )
    assert_equals( 'foo', str(ba._input) )
    assert_equals( 0, ba._start_pos )
    assert_equals( 0, ba._pos )
    assert_equals( 3, ba._end_pos )

    s = Reader('foo')
    assert_true( isinstance(s._input, buffer) )
    assert_equals( 'foo', str(s._input) )

    u = Reader(u'D\xfcsseldorf')
    assert_true( isinstance(u._input, buffer) )
    assert_equals( 'D\xc3\xbcsseldorf', str(u._input) )

    b = BytesIO('foo')
    i = Reader(b)
    assert_true( isinstance(i._input, buffer) )
    assert_equals( 'foo', str(i._input) )

    src = Reader( 'foo' )
    r = Reader( src )
    assert_true( isinstance(r._input, buffer) )
    assert_equals( id(src._input), id(r._input) )
    assert_equals( 0, r._start_pos )
    assert_equals( 3, r._end_pos )

    src = Reader( 'hello world' )
    r = Reader( src, 3, 5 )
    assert_true( isinstance(r._input, buffer) )
    assert_equals( id(src._input), id(r._input) )
    assert_equals( 3, r._start_pos )
    assert_equals( 8, r._end_pos )
    assert_equals( 3, r._pos )

    assert_raises( ValueError, Reader, 1 )

  def test_str(self):
    assert_equals( '\\x66\\x6f\\x6f', str(Reader('foo')) )

  def test_tell(self):
    r = Reader('')
    r._pos = 'foo'
    assert_equals( 'foo', r.tell() )

  def test_seek_whence_zero(self):
    r = Reader('', 3)
    assert_equals( 3, r._pos )
    r.seek( 5 )
    assert_equals( 8, r._pos )

  def test_seek_whence_one(self):
    r = Reader('')
    r._pos = 2
    r.seek( 5, 1 )
    assert_equals( 7, r._pos )

  def test_seek_whence_two(self):
    r = Reader('foo bar')
    r.seek( -3, 2 )
    assert_equals( 3, r._pos )

  def test_check_underflow(self):
    r = Reader('')
    r._pos = 0
    r._end_pos = 5
    r._check_underflow( 3 )
    assert_raises( Reader.BufferUnderflow, r._check_underflow, 8 )

  def test_check_len(self):
    r = Reader('foo bar')
    self.assert_equals( 7, len(r) )
    r = Reader('foo bar', 3)
    self.assert_equals( 4, len(r) )

  def test_buffer(self):
    r = Reader('hello world', 3, 5)
    self.assert_equals( buffer('lo wo'), r.buffer() )

  def test_read(self):
    b = Reader('foo')
    assert_equals('foo', b.read(3) )
    
    b = Reader('foo')
    assert_equals('fo', b.read(2) )

    b = Reader('foo')
    assert_raises( Reader.BufferUnderflow, b.read, 4 )

  def test_read_bit(self):
    b = Reader('\x01')
    assert_true( b.read_bit() )
    
    b = Reader('\x00')
    assert_false( b.read_bit() )
    
    b = Reader('\x02')
    assert_false( b.read_bit() )

    b = Reader('')
    assert_raises( Reader.BufferUnderflow, b.read_bit )
  
  def test_read_bits(self):
    b = Reader('\x01')
    assert_equals( [True], b.read_bits(1) )
    
    b = Reader('\x00')
    assert_equals( [False], b.read_bits(1) )
    
    b = Reader('\x02')
    assert_equals( [False,True], b.read_bits(2) )
    
    b = Reader('\x02')
    assert_equals( [False,True,False,False,False,False,False,False], b.read_bits(8) )

    b = Reader('\x00')
    assert_raises( ValueError, b.read_bits, 9 )
    assert_raises( ValueError, b.read_bits, -1 )
    assert_equals( [], b.read_bits(0) )

    b = Reader('')
    assert_raises( Reader.BufferUnderflow, b.read_bits, 2 )

  def test_read_octet(self):
    b = Reader('\xff')
    assert_equals( 255, b.read_octet() )
    assert_raises( Reader.BufferUnderflow, b.read_octet )

  def test_read_short(self):
    b = Reader('\xff\x00')
    assert_equals( 65280, b.read_short() )
    assert_raises( Reader.BufferUnderflow, b.read_short )
  
  def test_read_long(self):
    b = Reader('\xff\x00\xff\x00')
    assert_equals( 4278255360, b.read_long() )
    assert_raises( Reader.BufferUnderflow, b.read_long )
  
  def test_read_longlong(self):
    b = Reader('\xff\x00\xff\x00\xff\x00\xff\x00')
    assert_equals( 18374966859414961920L, b.read_longlong() )
    assert_raises( Reader.BufferUnderflow, b.read_longlong )

  def test_read_shortstr(self):
    b = Reader('\x05hello')
    assert_equals( 'hello', b.read_shortstr() )
    assert_raises( Reader.BufferUnderflow, b.read_shortstr )
    
    b = Reader('\x0bD\xc3\xbcsseldorf')
    assert_equals( 'D\xc3\xbcsseldorf', b.read_shortstr() )
    
    b = Reader('\x05hell')
    assert_raises( Reader.BufferUnderflow, b.read_shortstr )

  def test_read_longstr(self):
    b = Reader('\x00\x00\x01\x00'+('a'*256))
    assert_equals( 'a'*256, b.read_longstr() )
    
    b = Reader('\x00\x00\x01\x00'+('a'*255))
    assert_raises( Reader.BufferUnderflow, b.read_longstr )

  def test_read_timestamp(self):
    b = Reader('\x00\x00\x00\x00\x4d\x34\xc4\x71')
    d = datetime(2011, 1, 17, 17, 36, 33)

    assert_equals( d, b.read_timestamp() )

  def test_read_table(self):
    # mock everything to keep this simple
    r = Reader('')
    expect( r.read_long ).returns( 42 )
    expect( r._check_underflow ).args( 42 )
    expect( r._field_shortstr ).returns( 'a' )
    expect( r._read_field ).returns( 3.14 ).side_effect( lambda: setattr(r, '_pos', 20) )
    expect( r._field_shortstr ).returns( 'b' )
    expect( r._read_field ).returns( 'pi' ).side_effect( lambda: setattr(r, '_pos', 42) )
    
    assert_equals( {'a':3.14,'b':'pi'}, r.read_table() )

  def test_read_field(self):
    r = Reader('Z')
    r.field_type_map['Z'] = mock()
    expect( r.field_type_map['Z'] ).args( r )
    
    r._read_field()

  def test_read_field_raises_fielderror_on_unknown_type(self):
    r = Reader('X')
    assert_raises( Reader.FieldError, r._read_field )

  def test_field_bool(self):
    r = Reader('\x00\x01\xf5')
    assert_false( r._field_bool() )
    assert_true( r._field_bool() )
    assert_true( r._field_bool() )
    assert_equals( 3, r._pos )

  def test_field_short_short_int(self):
    r = Reader( struct.pack('bb', 5, -5) )
    assert_equals( 5, r._field_short_short_int() )
    assert_equals( -5, r._field_short_short_int() )
    assert_equals( 2, r._pos )
  
  def test_field_short_short_uint(self):
    r = Reader( struct.pack('BB', 5, 255) )
    assert_equals( 5, r._field_short_short_uint() )
    assert_equals( 255, r._field_short_short_uint() )
    assert_equals( 2, r._pos )

  def test_field_short_int(self):
    r = Reader( struct.pack('>hh', 256, -256) )
    assert_equals( 256, r._field_short_int() )
    assert_equals( -256, r._field_short_int() )
    assert_equals( 4, r._pos )

  def test_field_short_uint(self):
    r = Reader( struct.pack('>HH', 256, 512) )
    assert_equals( 256, r._field_short_uint() )
    assert_equals( 512, r._field_short_uint() )
    assert_equals( 4, r._pos )

  def test_field_long_int(self):
    r = Reader( struct.pack('>ii', 2**16, -2**16) )
    assert_equals( 2**16, r._field_long_int() )
    assert_equals( -2**16, r._field_long_int() )
    assert_equals( 8, r._pos )

  def test_field_long_uint(self):
    r = Reader( struct.pack('>I', 2**32-1) )
    assert_equals( 2**32-1, r._field_long_uint() )
    assert_equals( 4, r._pos )

  def test_field_long_long_int(self):
    r = Reader( struct.pack('>qq', 2**32, -2**32) )
    assert_equals( 2**32, r._field_long_long_int() )
    assert_equals( -2**32, r._field_long_long_int() )
    assert_equals( 16, r._pos )

  def test_field_long_long_uint(self):
    r = Reader( struct.pack('>Q', 2**64-1) )
    assert_equals( 2**64-1, r._field_long_long_uint() )
    assert_equals( 8, r._pos )

  def test_field_float(self):
    r = Reader( struct.pack('>f', 3.1421) )
    assert_almost_equals( 3.1421, r._field_float(), 4 )
    assert_equals( 4, r._pos )

  def test_field_double(self):
    r = Reader( struct.pack('>d', 8675309.1138) )
    assert_almost_equals( 8675309.1138, r._field_double(), 4 )
    assert_equals( 8, r._pos )

  def test_field_decimal(self):
    r = Reader( struct.pack('>Bi', 2, 5) )
    assert_equals( Decimal('0.05'), r._field_decimal() )
    assert_equals( 5, r._pos )
    
    r = Reader( struct.pack('>Bi', 2, -5) )
    assert_equals( Decimal('-0.05'), r._field_decimal() )
    assert_equals( 5, r._pos )

  def test_field_shortstr(self):
    r = Reader( '\x05hello' )
    assert_equals( 'hello', r._field_shortstr() )
    assert_equals( 6, r._pos )

  def test_field_longstr(self):
    r = Reader( '\x00\x00\x01\x00'+('a'*256) )
    assert_equals( 'a'*256, r._field_longstr() )
    assert_equals( 260, r._pos )

  def test_field_array(self):
    # easier to mock the behavior here
    r = Reader('')
    expect( r.read_long ).returns( 42 )
    expect( r._read_field ).returns( 3.14 ).side_effect( lambda: setattr(r, '_pos', 20) )
    expect( r._read_field ).returns( 'pi' ).side_effect( lambda: setattr(r, '_pos', 42) )
    
    assert_equals( [3.14,'pi'], r._field_array() )
  
  def test_field_timestamp(self):
    b = Reader('\x00\x00\x00\x00\x4d\x34\xc4\x71')
    d = datetime(2011, 1, 17, 17, 36, 33)

    assert_equals( d, b._field_timestamp() )

  def test_field_bytearray(self):
    b = Reader('\x00\x00\x00\x03\x04\x05\x06')
    assert_equals( bytearray('\x04\x05\x06'), b._field_bytearray() )

  def test_field_none(self):
    b = Reader('')
    assert_equals( None, b._field_none() )
  
  def test_field_type_map_rabbit_errata(self):
    # http://dev.rabbitmq.com/wiki/Amqp091Errata#section_3
    assert_equals(
      {
        't' : Reader._field_bool.im_func,
        'b' : Reader._field_short_short_int.im_func,
        's' : Reader._field_short_int.im_func,
        'I' : Reader._field_long_int.im_func,
        'l' : Reader._field_long_long_int.im_func,
        'f' : Reader._field_float.im_func,
        'd' : Reader._field_double.im_func,
        'D' : Reader._field_decimal.im_func,
        'S' : Reader._field_longstr.im_func,
        'T' : Reader._field_timestamp.im_func,
        'F' : Reader.read_table.im_func,
        'V' : Reader._field_none.im_func,
        'x' : Reader._field_bytearray.im_func,
      }, Reader.field_type_map )

  #def test_field_type_map_091_spec(self):
  #  assert_equals(
  #    {
  #      't' : Reader._field_bool.im_func,
  #      'b' : Reader._field_short_short_int.im_func,
  #      'B' : Reader._field_short_short_uint.im_func,
  #      'U' : Reader._field_short_int.im_func,
  #      'u' : Reader._field_short_uint.im_func,
  #      'I' : Reader._field_long_int.im_func,
  #      'i' : Reader._field_long_uint.im_func,
  #      'L' : Reader._field_long_long_int.im_func,
  #      'l' : Reader._field_long_long_uint.im_func,
  #      'f' : Reader._field_float.im_func,
  #      'd' : Reader._field_double.im_func,
  #      'D' : Reader._field_decimal.im_func,
  #      's' : Reader._field_shortstr.im_func,
  #      'S' : Reader._field_longstr.im_func,
  #      'A' : Reader._field_array.im_func,
  #      'T' : Reader._field_timestamp.im_func,
  #      'F' : Reader.read_table.im_func,
  #    }, Reader.field_type_map )

