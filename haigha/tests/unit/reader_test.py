import mox
from datetime import datetime
from cStringIO import StringIO

from haigha.reader import Reader
import struct

class ReaderTest(mox.MoxTestBase):

  def test_init(self):
    s = Reader('foo')
    self.assertEquals( 'foo', s.input.getvalue() )
    self.assertEquals( 0, s.bitcount )
    self.assertEquals( 0, s.bits )

    u = Reader(u'D\xfcsseldorf')
    self.assertEquals( 'D\xc3\xbcsseldorf', u.input.getvalue() )

    b = StringIO('foo')
    i = Reader(b)
    self.assertEquals( b, i.input )

    self.assertRaises( ValueError, Reader, 1 )

  def test_str(self):
    self.assertEquals( '\\x66\\x6f\\x6f', str(Reader('foo')) )

  def test_close(self):
    b = Reader('foo')
    b.input = self.create_mock_anything()
    b.input.close()

    self.replay_all()
    b.close()

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
    b = Reader('\xff')
    for x in xrange(8):
      self.assertTrue( b.read_bit() )
    self.assertRaises( Reader.BufferUnderflow, b.read_bit )
    
    b = Reader('\x00')
    for x in xrange(8):
      self.assertFalse( b.read_bit() )

    b = Reader('\x0f')
    for x in xrange(4):
      self.assertTrue( b.read_bit() )
    for x in xrange(4):
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
