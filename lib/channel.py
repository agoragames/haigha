from haigha.lib.classes import *
from haigha.lib.frames import Frame

class Channel(object):
  '''
  Define a channel
  '''

  class ChannelError(Exception): '''Base class for all channel errors'''
  class InvalidClass(ChannelError): '''The method frame referenced an invalid class.  Non-fatal.'''
  class InvalidMethod(ChannelError): '''The method frame referenced an invalid method.  Non-fatal.'''

  # TODO: If there is such a thing as extended classes for method frames, then
  # allow user to pass in a mapping.
  def __init__(self, connection, channel_id):
    '''Initialize with a handle to the connection and an id.'''
    self._connection = connection
    self._channel_id = channel_id
    
    self.channel = ChannelClass( self )
    self.exchange = ExchangeClass( self )
    self.queue = QueueClass( self )
    self.basic = BasicClass( self )
    self.tx = TransactionClass( self )

    self._class_map = {
      20 : self.channel,
      40 : self.exchange,
      50 : self.queue,
      60 : self.basic,
      90 : self.tx,
    }

    self._pending_events = []

  @property
  def connection(self):
    return self._connection

  @property
  def channel_id(self):
    return self._channel_id

  @property
  def logger(self):
    return self._connection.logger

  def open(self):
    '''
    Open this channel.  Routes to channel.open.
    '''
    self.channel.open()

  def close(self):
    '''
    Close this channel.  Routes to channel.close.
    '''
    self.channel.close()

  def dispatch(self, method_frame, *content_frames):
    '''
    Dispatch a method.
    '''
    klass = self._class_map.get( method_frame.class_id )
    if klass:
      self.logger.debug("Channel %d dispatching class_id : %s ", self.channel_id, method_frame.class_id)
      klass.dispatch( method_frame, *content_frames)
    else:
      raise Channel.InvalidClass( "class %d is not support on channel %d", 
        method_frame.class_id, self.channel_id )
        
  def send_frame(self, frame):
    '''
    Queue a frame for sending.  Will send immediately if there are no pending
    synchronous transactions on this connection.
    '''
    if not len(self._pending_events) or isinstance(self._pending_events[0],Frame):
      #self.logger.debug( 'no pending synch events, sending %s', frame )
      self._connection.send_frame(frame)
    else:
      #self.logger.debug( 'waiting for synch event %s', self._pending_events[0] )
      self._pending_events.append( frame )

  def add_synchronous_cb(self, cb):
    '''
    Add an expectation of a callback to release a synchronous transaction.
    '''
    self._pending_events.append( cb )

  def clear_synchronous_cb(self, cb):
    '''
    If the callback is the current expected callback, will clear it off the
    stack.  Else will raise in exception if there's an expectation but this
    doesn't satisfy it.
    '''
    if len(self._pending_events):
      ev = self._pending_events[0]
      if not isinstance(ev,Frame):
        if ev==cb:
          #self.logger.debug("Clearing synch cb %s", ev)
          self._pending_events.pop(0)
          self._flush_pending_events()
        else:
          raise ChannelError("Expected synchronous callback %s, called %s", ev, cb)

  def _flush_pending_events(self):
    '''
    Send pending frames that are in the event queue.
    '''
    while len(self._pending_events) and isinstance(self._pending_events[0],Frame):
      #self.logger.debug("Flusing pending frame %s", self._pending_events[0])
      self._connection.send_frame( self._pending_events.pop(0) )
