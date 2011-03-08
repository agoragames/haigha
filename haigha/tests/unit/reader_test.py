from chai import Chai
from datetime import datetime
from cStringIO import StringIO

from haigha.reader import Reader
import struct

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

    b = StringIO('foo')
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

  
  # def test_read_table

  def test_readtimestamp(self):
    b = Reader('\x00\x00\x00\x00\x4d\x34\xc4\x71')
    d = datetime(2011, 1, 17, 17, 36, 33)

    assert_equals( d, b.read_timestamp() )
