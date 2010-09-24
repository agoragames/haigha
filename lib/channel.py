
from haigha.classes import *

class Channel(object):
  '''
  Define a channel
  '''

  class ChannelError(Exception): '''Base class for all channel errors'''
  class InvalidClass(ChannelException): '''The method frame referenced an invalid class.  Non-fatal.'''
  class InvalidMethod(ChannelException): '''The method frame referenced an invalid method.  Non-fatal.'''

  # TODO: If there is such a thing as extended classes for method frames, then
  # allow user to pass in a mapping.
  def __init__(self, connection, channel_id):
    '''Initialize with a handle to the connection and an id.'''
    self._connection = connection
    self._channel_id = channel_id

    self._class_map = {
      20 : ChannelClass( self ),
      40 : ExchangeClass( self ),
      50 : QueueClass( self ),
      60 : BasicClass( self ),
      90 : TransactionClass( self ),
    }
    if( channel_id==0 ):
      self._class_map[ 10 ] = ConnectionClass( self )

  @property
  def connection(self):
    return self._connection

  @property
  def channel_id(self):
    return self._channel_id

  @property
  def logger(self):
    return self._connection.logger

  def dispatch(self, method_frame, *content_frames):
    '''
    Dispatch a method.
    '''
    klass = self._class_map.get( method_frame.class_id )
    if klass:
      klass.dispatch( method_frame, *content_frames)
    else:
      raise InvalidClass( "class %d is not support on channel %d", 
        method_frame.class_id, self.channel_id )
