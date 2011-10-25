'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

class Transport(object):
  '''
  Base class and API for Transports
  '''

  def __init__(self, connection):
    '''
    Initialize a transport on a haigha.Connection instance.
    '''
    self._connection = connection

  def process_channels(self, channels):
    '''
    Process a set of channels by calling Channel.process_frames() on each. 
    Some transports may choose to do this in unique ways, such as through 
    a pool of threads.

    The default implementation will simply iterate over them and call 
    process_frames() on each.
    '''
    for channel in channels:
      channel.process_frames()

  def read(self):
    '''
    Read from the transport. If no data is available, should return None.
    '''
    return None

  def buffer(self, data):
    '''
    Buffer unused bytes from the input stream.
    '''

  def write(self, data):
    '''
    Write some bytes to the transport.
    '''
    
  def disconnect(self):
    '''
    Disconnect from the transport. Typically socket.close(). This call is 
    welcome to raise exceptions, which the Connection will catch.
    '''

  
