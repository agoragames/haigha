from chai import Chai
from datetime import datetime
from cStringIO import StringIO

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

  def test_write_table(self):
    pass
