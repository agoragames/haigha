
import mox
from datetime import datetime
from cStringIO import StringIO

from haigha.lib.writer import Writer


class WriterTest(mox.MoxTestBase):

  def setUp(self):
    mox.MoxTestBase.setUp( self )

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

  def test_flush(self):
    # TODO: test the datatypes in more depth
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
    writer.write_timestamp( datetime.now() )

    stream = StringIO()
    writer.flush( stream )
