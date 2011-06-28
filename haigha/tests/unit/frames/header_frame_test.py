'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
import struct
import time
from datetime import datetime

from haigha.frames import header_frame
from haigha.frames.header_frame import HeaderFrame
from haigha.reader import Reader
from haigha.writer import Writer

class HeaderFrameTest(Chai):

  def test_type(self):
    assert_equals( 2, HeaderFrame.type() )

  def test_properties(self):
    frame = HeaderFrame(42, 'class_id', 'weight', 'size', 'props')
    assert_equals( 42, frame.channel_id )
    assert_equals( 'class_id', frame.class_id )
    assert_equals( 'weight', frame.weight )
    assert_equals( 'size', frame.size )
    assert_equals( 'props', frame.properties )

  def test_str(self):
    # Don't bother checking the copy
    frame = HeaderFrame(42, 5, 6, 7, 'props')
    assert_equals('HeaderFrame[channel: 42, class_id: 5, weight: 6, size: 7, properties: props]',
      str(frame) )

  def test_parse_fast_for_standard_properties(self):
    bit_writer = Writer()
    val_writer = Writer()

    # strip ms because amqp doesn't include it
    now = datetime.fromtimestamp( long(time.mktime(datetime.now().timetuple())) )

    bit_field = 0
    for pname, ptype, reader, writer, mask in HeaderFrame.PROPERTIES:
      bit_field = (bit_field << 1) | 1

      if ptype=='shortstr':
        val_writer.write_shortstr( pname )
      elif ptype=='octet':
        val_writer.write_octet( 42 )
      elif ptype=='timestamp':
        val_writer.write_timestamp( now )
      elif ptype=='table':
        val_writer.write_table( {'foo':'bar'} )

    bit_field <<= (16- len(HeaderFrame.PROPERTIES))
    bit_writer.write_short( bit_field )
    
    header_writer = Writer()
    header_writer.write_short( 5 )
    header_writer.write_short( 6 )
    header_writer.write_longlong( 7 )
    payload = header_writer.buffer()
    payload += bit_writer.buffer()
    payload += val_writer.buffer()

    reader = Reader(payload)
    frame = HeaderFrame.parse(4, reader)

    for pname, ptype, reader, writer, mask in HeaderFrame.PROPERTIES:
      if ptype=='shortstr':
        self.assertEquals( pname, frame.properties[pname] )
      elif ptype=='octet':
        self.assertEquals( 42, frame.properties[pname] )
      elif ptype=='timestamp':
        self.assertEquals( now, frame.properties[pname] )
      elif ptype=='table':
        self.assertEquals( {'foo':'bar'}, frame.properties[pname] )

    assert_equals( 4, frame.channel_id )
    assert_equals( 5, frame._class_id )
    assert_equals( 6, frame._weight )
    assert_equals( 7, frame._size )
  
  def test_parse_slow_for_standard_properties(self):
    HeaderFrame.DEFAULT_PROPERTIES = False
    bit_writer = Writer()
    val_writer = Writer()

    # strip ms because amqp doesn't include it
    now = datetime.fromtimestamp( long(time.mktime(datetime.now().timetuple())) )

    bit_field = 0
    for pname, ptype, reader, writer, mask in HeaderFrame.PROPERTIES:
      bit_field = (bit_field << 1) | 1

      if ptype=='shortstr':
        val_writer.write_shortstr( pname )
      elif ptype=='octet':
        val_writer.write_octet( 42 )
      elif ptype=='timestamp':
        val_writer.write_timestamp( now )
      elif ptype=='table':
        val_writer.write_table( {'foo':'bar'} )

    bit_field <<= (16- len(HeaderFrame.PROPERTIES))
    bit_writer.write_short( bit_field )
    
    header_writer = Writer()
    header_writer.write_short( 5 )
    header_writer.write_short( 6 )
    header_writer.write_longlong( 7 )
    payload = header_writer.buffer()
    payload += bit_writer.buffer()
    payload += val_writer.buffer()

    reader = Reader(payload)
    frame = HeaderFrame.parse(4, reader)
    HeaderFrame.DEFAULT_PROPERTIES = True

    for pname, ptype, reader, writer, mask in HeaderFrame.PROPERTIES:
      if ptype=='shortstr':
        self.assertEquals( pname, frame.properties[pname] )
      elif ptype=='octet':
        self.assertEquals( 42, frame.properties[pname] )
      elif ptype=='timestamp':
        self.assertEquals( now, frame.properties[pname] )
      elif ptype=='table':
        self.assertEquals( {'foo':'bar'}, frame.properties[pname] )

    assert_equals( 4, frame.channel_id )
    assert_equals( 5, frame._class_id )
    assert_equals( 6, frame._weight )
    assert_equals( 7, frame._size )

  def test_write_frame_fast_for_standard_properties(self):
    bit_field = 0
    properties = {}
    now = datetime.fromtimestamp( long(time.mktime(datetime.now().timetuple())) )
    for pname, ptype, reader, writer, mask in HeaderFrame.PROPERTIES:
      bit_field |= mask

      if ptype=='shortstr':
        properties[pname] = pname
      elif ptype=='octet':
        properties[pname] = 42
      elif ptype=='timestamp':
        properties[pname] = now
      elif ptype=='table':
        properties[pname] = {'foo':'bar'}

    frame = HeaderFrame(42, 5,6,7, properties)
    buf = bytearray()
    frame.write_frame(buf)

    reader = Reader(buf)
    assert_equals( 2, reader.read_octet() )
    assert_equals( 42, reader.read_short() )
    size = reader.read_long()
    start_pos = reader.tell()
    assert_equals( 5, reader.read_short() )
    assert_equals( 6, reader.read_short() )
    assert_equals( 7, reader.read_longlong() )
    assert_equals( 0b1111111111111100, reader.read_short() )

    for pname, ptype, rfunc, wfunc, mask in HeaderFrame.PROPERTIES:
      if ptype=='shortstr':
        assertEquals( pname, reader.read_shortstr() )
      elif ptype=='octet':
        assertEquals( 42, reader.read_octet() )
      elif ptype=='timestamp':
        assertEquals( now, reader.read_timestamp() )
      elif ptype=='table':
        assertEquals( {'foo':'bar'}, reader.read_table() )

    end_pos = reader.tell()
    assert_equals( size, end_pos-start_pos )
    assert_equals( 0xce, reader.read_octet() )
  
  def test_write_frame_slow_for_standard_properties(self):
    HeaderFrame.DEFAULT_PROPERTIES = False
    bit_field = 0
    properties = {}
    now = datetime.fromtimestamp( long(time.mktime(datetime.now().timetuple())) )
    for pname, ptype, reader, writer, mask in HeaderFrame.PROPERTIES:
      bit_field |= mask

      if ptype=='shortstr':
        properties[pname] = pname
      elif ptype=='octet':
        properties[pname] = 42
      elif ptype=='timestamp':
        properties[pname] = now
      elif ptype=='table':
        properties[pname] = {'foo':'bar'}

    frame = HeaderFrame(42, 5,6,7, properties)
    buf = bytearray()
    frame.write_frame(buf)
    HeaderFrame.DEFAULT_PROPERTIES = True

    reader = Reader(buf)
    assert_equals( 2, reader.read_octet() )
    assert_equals( 42, reader.read_short() )
    size = reader.read_long()
    start_pos = reader.tell()
    assert_equals( 5, reader.read_short() )
    assert_equals( 6, reader.read_short() )
    assert_equals( 7, reader.read_longlong() )
    assert_equals( 0b1111111111111100, reader.read_short() )

    for pname, ptype, rfunc, wfunc, mask in HeaderFrame.PROPERTIES:
      if ptype=='shortstr':
        assertEquals( pname, reader.read_shortstr() )
      elif ptype=='octet':
        assertEquals( 42, reader.read_octet() )
      elif ptype=='timestamp':
        assertEquals( now, reader.read_timestamp() )
      elif ptype=='table':
        assertEquals( {'foo':'bar'}, reader.read_table() )

    end_pos = reader.tell()
    assert_equals( size, end_pos-start_pos )
    assert_equals( 0xce, reader.read_octet() )
