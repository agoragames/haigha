'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.frames.frame import Frame
from haigha.writer import Writer

class HeartbeatFrame(Frame):
  '''
  Frame for heartbeats.
  '''

  @classmethod
  def type(cls):
    # NOTE: The PDF spec say this should be 4 but the xml spec say it should be 8
    #       RabbitMQ seems to implement this as 8, but maybe that's a difference
    #       between 0.8 and 0.9 protocols
    # Using Rabbit 2.1.1 and protocol 0.9.1 it seems that 8 is indeed the correct type @AW
    # PDF spec: http://www.amqp.org/confluence/download/attachments/720900/amqp0-9-1.pdf?version=1&modificationDate=1227526523000
    # XML spec: http://www.amqp.org/confluence/download/attachments/720900/amqp0-9-1.xml?version=1&modificationDate=1227526672000
    # This is addressed in http://dev.rabbitmq.com/wiki/Amqp091Errata#section_29
    return 8
  
  @classmethod
  def parse(self, channel_id, payload):
    return HeartbeatFrame( channel_id )

  def write_frame(self, buf):
    writer = Writer(buf)
    writer.write_octet( self.type() )
    writer.write_short( self.channel_id )
    writer.write_long( 0 )
    writer.write_octet( 0xce )

HeartbeatFrame.register()
