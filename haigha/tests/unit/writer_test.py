from chai import Chai
from datetime import datetime
from cStringIO import StringIO

from haigha.writer import Writer


class WriterTest(Chai):

  # Tests commented out because they don't really apply, but there's a lot that
  # can be copied
  '''
  def test_write_methods(self):
    writer = Writer()
    writer.write( 'foo' )
    writer.write_bit( 1 )
    writer.write_octet( 5 )
    writer.write_short( 42 )
    writer.write_long( 12345 )
    writer.write_longlong( 123456789 )
    writer.write_shortstr( "bar" )
    writer.write_longstr( "hellowurld" )
    writer.write_table( {'cats':'dogs'} )
    writer.write_timestamp( 'now' )
    
    self.assertEquals( 10, len(writer._output_buffer) )
    self.assertEquals( (writer._write_str, 'foo'), writer._output_buffer[0] )
    self.assertEquals( (writer._write_bit, 1), writer._output_buffer[1] )
    self.assertEquals( (writer._write_octet, 5), writer._output_buffer[2] )
    self.assertEquals( (writer._write_short, 42), writer._output_buffer[3] )
    self.assertEquals( (writer._write_long, 12345), writer._output_buffer[4] )
    self.assertEquals( (writer._write_longlong, 123456789), writer._output_buffer[5] )
    self.assertEquals( (writer._write_shortstr, 'bar'), writer._output_buffer[6] )
    self.assertEquals( (writer._write_longstr, 'hellowurld'), writer._output_buffer[7] )
    self.assertEquals( (writer._write_table, {'cats':'dogs'}), writer._output_buffer[8] )
    self.assertEquals( (writer._write_timestamp, 'now'), writer._output_buffer[9] )

  def test_writing_bits(self):
    writer = Writer(); stream = StringIO()
    writer.write_bit(True)
    writer.flush( stream )
    self.assertEquals( '\x01', stream.getvalue() )
    
    writer = Writer(); stream = StringIO()
    [ writer.write_bit(True) for x in xrange(4) ]
    writer.flush( stream )
    self.assertEquals( '\x0f', stream.getvalue() )
    
    writer = Writer(); stream = StringIO()
    [ writer.write_bit(True) for x in xrange(5) ]
    writer.flush( stream )
    self.assertEquals( '\x1f', stream.getvalue() )
    
    writer = Writer(); stream = StringIO()
    [ writer.write_bit(True) for x in xrange(8) ]
    writer.flush( stream )
    self.assertEquals( '\xff', stream.getvalue() )
    
    writer = Writer(); stream = StringIO()
    writer.write_bit(True)
    writer.write_bit(False)
    writer.write_bit(True)
    writer.write_bit(False)
    writer.flush( stream )
    self.assertEquals( '\x05', stream.getvalue() )
    
    writer = Writer(); stream = StringIO()
    writer.write_bit(True)
    writer.write_bit(False)
    writer.write_bit(True)
    writer.write_bit(False)
    writer.write_bit(True)
    writer.flush( stream )
    self.assertEquals( '\x15', stream.getvalue() )
    
    writer = Writer(); stream = StringIO()
    writer.write_shortstr('foo')
    [ writer.write_bit(True) for x in xrange(4) ]
    writer.write_shortstr('bar')
    writer.flush( stream )
    self.assertEquals( '\x03foo\x0f\x03bar', stream.getvalue() )
    
    writer = Writer(); stream = StringIO()
    writer.write_shortstr('foo')
    writer.write_bit(True)
    writer.write_bit(False)
    writer.write_bit(True)
    writer.write_bit(False)
    writer.write_bit(True)
    writer.write_shortstr('bar')
    writer.flush( stream )
    self.assertEquals( '\x03foo\x15\x03bar', stream.getvalue() )
    '''
