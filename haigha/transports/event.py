'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.transports import Transport

from eventsocket import EventSocket
import event

class Event(object):
  '''
  Base class and API for Transports
  '''

  #def __init__(self, connection):
  #  '''
  #  Initialize a transport on a haigha.Connection instance.
  #  '''
  #  self._connection = connection

  def _sock_close_cb(self, *args):
    self._connection.transport_closed_unexpected()

  # def _sock_error_cb(self, sock, msg, exception=None):
  def _sock_error_cb(self, *args):
    self._connection.transport_closed_unexpected()
  
  def connect(self, (host,port)):
    '''
    Connect assuming a host and port tuple.
    '''
    self._sock = EventSocket(
      read_cb=self._sock_read_cb,
      close_cb=self._sock_close_cb, 
      error_cb=self._sock_error_cb,
      debug=self._debug, 
      logger=self._connection.logger )
    self._sock.settimeout( self._connect_timeout )
    if self._sock_opts:
      for k,v in self._sock_opts.iteritems():
        family,type = k
        self._sock.setsockopt(family, type, v)
    self._sock.connect( (host,port) )
    self._sock.setblocking( False )


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
    self._sock.write( data )
    
  def disconnect(self):
    '''
    Disconnect from the transport. Typically socket.close(). This call is 
    welcome to raise exceptions, which the Connection will catch.
    '''
    self._sock.close_cb = None
    self._sock.close()
