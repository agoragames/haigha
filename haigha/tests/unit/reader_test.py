import mox
from datetime import datetime
from cStringIO import StringIO

from haigha.reader import Reader
import struct

class ReaderTest(mox.MoxTestBase):

  def test_init(self):
    ba = Reader(bytearray('foo'))
    self.assertTrue( isinstance(ba._input, buffer) )
    self.assertEquals( 'foo', ba._input )
    self.assertEquals( 0, ba._start_pos )
    self.assertEquals( 0, ba._pos )
    self.assertEquals( 3, ba._end_pos )

    s = Reader('foo')
    self.assertEquals( 'foo', s._input )
    self.assertEquals( 0, s.bitcount )
    self.assertEquals( 0, s.bits )

    u = Reader(u'D\xfcsseldorf')
    self.assertEquals( 'D\xc3\xbcsseldorf', u._input )

    b = StringIO('foo')
    i = Reader(b)
    self.assertEquals( 'foo', i._input )

    src = Reader( 'foo' )
    r = Reader( src )
    self.assertEquals( id(src._input), id(r._input) )
    self.assertEquals( 0, r._start_pos )
    self.assertEquals( 3, r._end_pos )

    src = Reader( 'hello world' )
    r = Reader( src, 3, 5 )
    self.assertEquals( id(src._input), id(r._input) )
    self.assertEquals( 3, r._start_pos )
    self.assertEquals( 8, r._end_pos )
    self.assertEquals( 3, r._pos )

    self.assertRaises( ValueError, Reader, 1 )

  def test_str(self):
    self.assertEquals( '\\x66\\x6f\\x6f', str(Reader('foo')) )

  def test_read(self):
    b = Reader('foo')
    b.bitcount = b.bits = 42
    self.assertEquals('foo', b.read(3) )
    self.assertEquals(0, b.bitcount )
    self.assertEquals(0, b.bits )
    
    b = Reader('foo')
    self.assertEquals('fo', b.read(2) )

    b = Reader('foo')
    self.assertRaises( Reader.BufferUnderflow, b.read, 4 )

  def test_read_bit(self):
    b = Reader('\x01')
    self.assertTrue( b.read_bit() )
    
    b = Reader('\x00')
    self.assertFalse( b.read_bit() )
    
    b = Reader('\x02')
    self.assertFalse( b.read_bit() )


  def test_read_octet(self):
    b = Reader('\xff')
    self.assertEquals( 255, b.read_octet() )
    self.assertRaises( Reader.BufferUnderflow, b.read_octet )

  def test_read_short(self):
    b = Reader('\xff\x00')
    self.assertEquals( 65280, b.read_short() )
    self.assertRaises( Reader.BufferUnderflow, b.read_short )
  
  def test_read_long(self):
    b = Reader('\xff\x00\xff\x00')
    self.assertEquals( 4278255360, b.read_long() )
    self.assertRaises( Reader.BufferUnderflow, b.read_long )
  
  def test_read_longlong(self):
    b = Reader('\xff\x00\xff\x00\xff\x00\xff\x00')
    self.assertEquals( 18374966859414961920L, b.read_longlong() )
    self.assertRaises( Reader.BufferUnderflow, b.read_longlong )

  def test_read_shortstr(self):
    b = Reader('\x05hello')
    self.assertEquals( u'hello', b.read_shortstr() )
    self.assertRaises( Reader.BufferUnderflow, b.read_shortstr )
    
    b = Reader('\x0bD\xc3\xbcsseldorf')
    self.assertEquals( u'D\xfcsseldorf', b.read_shortstr() )
    
    b = Reader('\x05hell')
    self.assertRaises( Reader.BufferUnderflow, b.read_shortstr )

  def test_read_longstr(self):
    b = Reader('\x00\x00\x01\x00'+('a'*256))
    self.assertEquals( 'a'*256, b.read_longstr() )
    
    b = Reader('\x00\x00\x01\x00'+('a'*255))
    self.assertRaises( Reader.BufferUnderflow, b.read_longstr )

  
  # def test_read_table

  def test_readtimestamp(self):
    b = Reader('\x00\x00\x00\x00\x4d\x34\xc4\x71')
    d = datetime(2011, 1, 17, 17, 36, 33)

    self.assertEquals( d, b.read_timestamp() )
