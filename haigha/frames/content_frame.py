from haigha.writer import Writer
from haigha.frames.frame import Frame

class ContentFrame(Frame):
  '''
  Frame for reading in content.
  '''

  @classmethod
  def type(cls):
    return 3

  @property
  def payload(self):
    return self._payload

  @classmethod
  def parse(self, channel_id, payload):
    return ContentFrame( channel_id, payload )

  @classmethod
  def create_frames(self, channel_id, buf, frame_max):
    '''
    A generator which will create frames from a buffer given a max
    frame size.
    '''
    size = frame_max - 8   # 8 bytes overhead for frame header and footer
    offset = 0
    while True:
      payload = buf[offset:(offset+size)]
      if len(payload)==0: break
      offset += size
      
      yield ContentFrame(channel_id, payload)
      if offset >= len(buf): break
    
  def __init__(self, channel_id, payload):
    Frame.__init__(self, channel_id)
    self._payload = payload
  
  def __str__(self):
    return "%s[channel: %d, payload: %s]"%( self.__class__.__name__, self.channel_id, self._payload )

  def write_frame(self, buf):
    writer = Writer( buf )

    writer.write_octet( self.type() )
    writer.write_short(self.channel_id)
    writer.write_long( len(self._payload) )

    writer.write( self._payload )

    writer.write_octet( 0xce )
    #writer.flush( stream )


ContentFrame.register()

