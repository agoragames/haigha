"""
Unit tests for the header frame
"""

import mox
import struct
import time
from datetime import datetime
from cStringIO import StringIO

from haigha.frames import header_frame
from haigha.frames.header_frame import HeaderFrame
from haigha.reader import Reader
from haigha.writer import Writer

class HeaderFrameTest(mox.MoxTestBase):

  def test_parse_for_standard_properties(self):
    bit_writer = Writer()
    val_writer = Writer()
    stream = StringIO()

    # strip ms because amqp doesn't include it
    now = datetime.fromtimestamp( long(time.mktime(datetime.now().timetuple())) )

    bit_field = 0
    for pname, ptype in HeaderFrame.PROPERTIES:
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
    bit_writer.flush( stream )

    val_writer.flush( stream )
    
    payload = struct.pack( '>HHQ', 5, 6, 7 )
    payload += stream.getvalue()

    frame = HeaderFrame.parse(4, payload)

    for pname,ptype in HeaderFrame.PROPERTIES:
      if ptype=='shortstr':
        self.assertEquals( pname, frame.properties[pname] )
      elif ptype=='octet':
        self.assertEquals( 42, frame.properties[pname] )
      elif ptype=='timestamp':
        self.assertEquals( now, frame.properties[pname] )
      elif ptype=='table':
        self.assertEquals( {'foo':'bar'}, frame.properties[pname] )
