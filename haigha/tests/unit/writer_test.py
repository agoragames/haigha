'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
from datetime import datetime
from decimal import Decimal

from haigha.writer import Writer


class WriterTest(Chai):

  def test_init(self):
    w = Writer()
    assert_equals( bytearray(), w._output_buffer )

    w = Writer(buf='foo')
    assert_equals( 'foo', w._output_buffer )

  def test_str(self):
    w = Writer(bytearray('\x03\xfb\x4d'))
    assert_equals( '\\x03\\xfb\\x4d', str(w) )
    assert_equals( '\\x03\\xfb\\x4d', repr(w) )

  def test_eq(self):
    assert_equals( Writer(bytearray('foo')), Writer(bytearray('foo')) )
    assert_not_equals( Writer(bytearray('foo')), Writer(bytearray('bar')) )

  def test_buffer(self):
    b = bytearray('pbt')
    assert_true( b is Writer(b).buffer() )

  def test_write(self):
    w = Writer()
    assert_true( w is w.write( 'foo' ) )
    assert_equals( bytearray('foo'), w._output_buffer )

  def test_write_bits(self):
    w = Writer()
    assert_true( w is w.write_bits(False,True) )
    assert_equals( bytearray('\x02'), w._output_buffer )
    w = Writer()
    assert_true( w is w.write_bits(False,False,True,True,True) )
    assert_equals( bytearray('\x1c'), w._output_buffer )

    assert_raises( ValueError, w.write_bits, *((True,)*9) )

  def test_write_bit(self):
    w = Writer()
    assert_true( w is w.write_bit(True) )
    assert_equals( bytearray('\x01'), w._output_buffer )

  def test_write_octet(self):
    w = Writer()
    assert_true( w is w.write_octet(0) )
    assert_true( w is w.write_octet(255) )
    assert_equals( bytearray('\x00\xff'), w._output_buffer )

    assert_raises( ValueError, w.write_octet, -1 )
    assert_raises( ValueError, w.write_octet, 2**8 )

  def test_write_short(self):
    w = Writer()
    assert_true( w is w.write_short(0) )
    assert_true( w is w.write_short(2**16-2) )
    assert_equals( bytearray('\x00\x00\xff\xfe'), w._output_buffer )

    assert_raises( ValueError, w.write_short, -1 )
    assert_raises( ValueError, w.write_short, 2**16 )

  def test_write_short_at(self):
    w = Writer( bytearray('\x00'*6) )
    assert_true( w is w.write_short_at(2**16-1,2) )
    assert_equals( bytearray('\x00\x00\xff\xff\x00\x00'), w._output_buffer )
    
    assert_raises( ValueError, w.write_short_at, -1, 2 )
    assert_raises( ValueError, w.write_short_at, 2**16, 3 )

  def test_write_long(self):
    w = Writer()
    assert_true( w is w.write_long(0) )
    assert_true( w is w.write_long(2**32-2) )
    assert_equals( bytearray('\x00\x00\x00\x00\xff\xff\xff\xfe'), w._output_buffer )
    
    assert_raises( ValueError, w.write_long, -1 )
    assert_raises( ValueError, w.write_long, 2**32 )

  def test_write_long_at(self):
    w = Writer( bytearray('\x00'*8) )
    assert_true( w is w.write_long_at(2**32-1,2) )
    assert_equals( bytearray('\x00\x00\xff\xff\xff\xff\x00\x00'), w._output_buffer )
    
    assert_raises( ValueError, w.write_long_at, -1, 2 )
    assert_raises( ValueError, w.write_long_at, 2**32, 3 )

  def test_write_long_long(self):
    w = Writer()
    assert_true( w is w.write_longlong(0) )
    assert_true( w is w.write_longlong(2**64-2) )
    assert_equals( 
      bytearray('\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xfe'),
      w._output_buffer )
    
    assert_raises( ValueError, w.write_longlong, -1 )
    assert_raises( ValueError, w.write_longlong, 2**64 )

  def test_write_shortstr(self):
    w = Writer()
    assert_true( w is w.write_shortstr('') )
    assert_equals( bytearray('\x00'), w._output_buffer )
    w = Writer()
    assert_true( w is w.write_shortstr('a'*255) )
    assert_equals( bytearray('\xff'+('a'*255)), w._output_buffer )
    w = Writer()
    assert_true( w is w.write_shortstr( 'Au\xc3\x9ferdem'.decode('utf8') ) )
    assert_equals( bytearray('\x09Au\xc3\x9ferdem'), w._output_buffer )

    assert_raises( ValueError, w.write_shortstr, 'a'*256 )

  def test_write_longstr(self):
    # We can't actually build a string 2**32 long, so can't also test the
    # valueerror without mocking
    w = Writer()
    assert_true( w is w.write_longstr('') )
    assert_equals( bytearray('\x00\x00\x00\x00'), w._output_buffer )
    w = Writer()
    assert_true( w is w.write_longstr('a' * (2**16)) )
    assert_equals( bytearray('\x00\x01\x00\x00'+('a' * 2**16)), w._output_buffer )
    w = Writer()
    assert_true( w is w.write_longstr( 'Au\xc3\x9ferdem'.decode('utf8') ) )
    assert_equals( bytearray('\x00\x00\x00\x09Au\xc3\x9ferdem'), w._output_buffer )

    # TODO: mock valueerror when chai fixes the '__len__' problems with Mocks
    # since we can't actually create a long-enough string

  def test_write_timestamp(self):
    w = Writer()
    w.write_timestamp( datetime(2011, 1, 17, 17, 36, 33) )

    assert_equals( '\x00\x00\x00\x00\x4d\x34\xc4\x71', w._output_buffer )

  def test_write_table(self):
    w = Writer()
    expect( w._write_item ).args( 'a', 'foo' ).any_order().side_effect( 
      lambda: (setattr(w,'_pos',20), w._output_buffer.extend('afoo')) )
    expect( w._write_item ).args( 'b', 'bar' ).any_order().side_effect( 
      lambda: (setattr(w,'_pos',20), w._output_buffer.extend('bbar')) )

    assert_true( w is w.write_table({'a':'foo','b':'bar'}) )
    assert_equals( '\x00\x00\x00\x08', w._output_buffer[:4] )
    assert_equals( 12, len(w._output_buffer) )

  def test_write_item(self):
    w = Writer()
    expect( w.write_shortstr ).args( 'key' )
    expect( w._write_field ).args( 'value' )
    w._write_item( 'key', 'value' )

  def test_write_field(self):
    w = Writer()
    unknown = mock()
    expect( w._field_none ).args( unknown )
    w._write_field( unknown )
    
    Writer.field_type_map[type(unknown)] = unknown
    expect( unknown ).args( w, unknown )
    w._write_field( unknown )

  def test_field_bool(self):
    w = Writer()
    w._field_bool( True )
    w._field_bool( False )
    assert_equals( 't\x01t\x00', w._output_buffer )

  def test_field_int(self):
    w = Writer()
    w._field_int( -2**15 )
    w._field_int( 2**15-1 )
    assert_equals( 's\x80\x00s\x7f\xff', w._output_buffer )
    
    w = Writer()
    w._field_int( -2**31 )
    w._field_int( 2**31-1 )
    assert_equals( 'I\x80\x00\x00\x00I\x7f\xff\xff\xff', w._output_buffer )
    
    w = Writer()
    w._field_int( -2**63 )
    w._field_int( 2**63-1 )
    assert_equals( 
      'l\x80\x00\x00\x00\x00\x00\x00\x00l\x7f\xff\xff\xff\xff\xff\xff\xff', 
      w._output_buffer )

  def test_field_double(self):
    w = Writer()
    w._field_double( 3.1457923 )
    assert_equals( 'd\x40\x09\x2a\x95\x27\x44\x11\xa8', w._output_buffer )

  def test_field_decimal(self):
    w = Writer()
    w._field_decimal( Decimal('1.50') )
    assert_equals( 'D\x02\x00\x00\x00\x96', w._output_buffer )
    
    w = Writer()
    w._field_decimal( Decimal('-1.50') )
    assert_equals( 'D\x02\xff\xff\xff\x6a', w._output_buffer )

  def test_field_str(self):
    w = Writer()
    w._field_str( 'foo' )
    assert_equals( 'S\x00\x00\x00\x03foo', w._output_buffer )

  def test_field_unicode(self):
    w = Writer()
    w._field_unicode( 'Au\xc3\x9ferdem'.decode('utf8') )
    assert_equals( 'S\x00\x00\x00\x09Au\xc3\x9ferdem', w._output_buffer )

  def test_field_timestamp(self):
    w = Writer()
    w._field_timestamp( datetime(2011, 1, 17, 17, 36, 33) )

    assert_equals( 'T\x00\x00\x00\x00\x4d\x34\xc4\x71', w._output_buffer )

  def test_field_table(self):
    w = Writer()
    expect( w.write_table ).args( {'foo':'bar'} ).side_effect(
      lambda: w._output_buffer.extend('tdata') )
    w._field_table( {'foo':'bar'} )

    assert_equals( 'Ftdata', w._output_buffer )

  def test_field_none(self):
    w = Writer()
    w._field_none( None )
    w._field_none( 'zomg' )
    assert_equals( 'VV', w._output_buffer )
  
  def test_field_bytearray(self):
    w = Writer()
    w._field_bytearray( bytearray('foo') )
    assert_equals( 'x\x00\x00\x00\x03foo', w._output_buffer )

  def test_field_iterable(self):
    w = Writer()
    expect( w._write_field ).args('la').side_effect( 
      lambda: w._output_buffer.append('a') )
    expect( w._write_field ).args('lb').side_effect(
      lambda: w._output_buffer.append('b') )
    w._field_iterable( ['la','lb'] )
    
    expect( w._write_field ).args('ta').side_effect( 
      lambda: w._output_buffer.append('a') )
    expect( w._write_field ).args('tb').side_effect(
      lambda: w._output_buffer.append('b') )
    w._field_iterable( ('ta','tb') )
    
    expect( w._write_field ).args('sa').any_order().side_effect( 
      lambda: w._output_buffer.append('s') )
    expect( w._write_field ).args('sb').any_order().side_effect(
      lambda: w._output_buffer.append('s') )
    w._field_iterable( set(('sa','sb')) )

    assert_equals( 'AabAabAss', w._output_buffer )

  def test_field_type_map(self):
    assert_equals(
      {
        bool      : Writer._field_bool.im_func,
        int       : Writer._field_int.im_func,
        long      : Writer._field_int.im_func,
        float     : Writer._field_double.im_func,
        Decimal   : Writer._field_decimal.im_func,
        str       : Writer._field_str.im_func,
        unicode   : Writer._field_unicode.im_func,
        datetime  : Writer._field_timestamp.im_func,
        dict      : Writer._field_table.im_func,
        None      : Writer._field_none.im_func,
        bytearray : Writer._field_bytearray.im_func,
      }, Writer.field_type_map )

