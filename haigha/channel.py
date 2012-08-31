'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from collections import deque

from haigha.classes import ProtocolClass
from haigha.frames import *
from haigha.exceptions import *

# Defined here so it's easier to test
class SyncWrapper(object):
  def __init__(self, cb):
    self._cb = cb
    self._read = True
    self._result = None
  def __eq__(self, other):
    return other==self._cb or \
      (isinstance(other,SyncWrapper) and other._cb==self._cb)
  def __call__(self, *args, **kwargs):
    self._read = False
    self._result = self._cb(*args, **kwargs)

class Channel(object):
  '''
  Define a channel
  '''

  class InvalidClass(ChannelError): '''The method frame referenced an invalid class.  Non-fatal.'''
  class InvalidMethod(ChannelError): '''The method frame referenced an invalid method.  Non-fatal.'''
  class Inactive(ChannelError): '''Tried to send a content frame while the channel was inactive. Non-fatal.'''

  def __init__(self, connection, channel_id, class_map):
    '''
    Initialize with a handle to the connection and an id. Caller must
    supply a mapping of {class_id:ProtocolClass} which defines what
    classes and methods this channel will support.
    '''
    self._connection = connection
    self._channel_id = channel_id

    self._class_map = {}
    for _id,_class in class_map.iteritems():
      impl = _class(self)
      setattr(self, impl.name, impl)
      self._class_map[ _id ] = impl
    
    # Out-bound mix of pending frames and synchronous callbacks
    self._pending_events = deque()

    # Incoming frame buffer
    self._frame_buffer = deque()

    # Listeners for when channel opens
    self._open_listeners = set()

    # Listeners for when channel closes
    self._close_listeners = set()

    # Moving state out of protocol class so that it's accessible even
    # after we've closed and deleted references to the protocol classes.
    # Note though that many of these fields are written to directly
    # from within ChannelClass.
    self._closed = False
    self._close_info = {
      'reply_code'    : 0,
      'reply_text'    : 'first connect',
      'class_id'      : 0,
      'method_id'     : 0
    }
    self._active = True

  @property
  def connection(self):
    return self._connection

  @property
  def channel_id(self):
    return self._channel_id

  @property
  def logger(self):
    '''Return a shared logger handle for the channel.'''
    return self._connection.logger

  @property
  def closed(self):
    '''Return whether this channel has been closed.'''
    return self._closed

  @property
  def close_info(self):
    '''Return dict with information on why this channel is closed.  Will
    return None if the channel is open.'''
    return self._close_info if self._closed else None

  @property
  def active(self):
    '''
    Return True if flow control turned off, False if flow control is on.
    '''
    return self._active

  def add_open_listener(self, listener):
    '''
    Add a listener for open events on this channel. The listener should be
    a callable that can take one argument, the channel that is opened. 
    Listeners will not be called in any particular order.
    '''
    self._open_listeners.add( listener )

  def remove_open_listener(self, listener):
    '''
    Remove an open event listener. Will do nothing if the listener is not
    registered.
    '''
    self._open_listeners.discard( listener )

  def _notify_open_listeners(self):
    '''Call all the open listeners.'''
    for listener in self._open_listeners:
      listener( self )

  def add_close_listener(self, listener):
    '''
    Add a listener for close events on this channel. The listener should be
    a callable that can take one argument, the channel that is closed. 
    Listeners will not be called in any particular order.
    '''
    self._close_listeners.add( listener )

  def remove_close_listener(self, listener):
    '''
    Remove a close event listener. Will do nothing if the listener is not
    registered.
    '''
    self._close_listeners.discard( listener )

  def _notify_close_listeners(self):
    '''Call all the close listeners.'''
    for listener in self._close_listeners:
      listener( self )

  def open(self):
    '''
    Open this channel.  Routes to channel.open.
    '''
    self.channel.open()

  def close(self, reply_code=0, reply_text='', class_id=0, method_id=0):
    '''
    Close this channel.  Routes to channel.close.
    '''
    # In the off chance that we call this twice. A good example is if there's
    # an error in close listeners and so we're still inside a single call to
    # process_frames, which will try to close this channel if there's an
    # exception.
    if hasattr(self, 'channel'):
      self.channel.close(reply_code, reply_text, class_id, method_id)

  def publish(self, *args, **kwargs):
    '''
    Standard publish.  See basic.publish.
    '''
    self.basic.publish( *args, **kwargs )

  def publish_synchronous(self, *args, **kwargs):
    '''
    Helper for publishing a message using transactions.  If 'cb' keyword arg
    is supplied, will be called when the transaction is committed.
    '''
    cb = kwargs.pop('cb', None)
    self.tx.select()
    self.basic.publish( *args, **kwargs )
    self.tx.commit( cb=cb )

  def dispatch(self, method_frame):
    '''
    Dispatch a method.
    '''
    klass = self._class_map.get( method_frame.class_id )
    if klass:
      klass.dispatch( method_frame )
    else:
      raise Channel.InvalidClass( "class %d is not supported on channel %d", 
        method_frame.class_id, self.channel_id )

  def buffer_frame(self, frame):
    '''
    Buffer an input frame.  Will append to current list of frames and ensure
    there's a pending event to process the queue.
    '''
    self._frame_buffer.append( frame )

  def process_frames(self):
    '''
    Process the input buffer.
    '''
    while len(self._frame_buffer):
      try:
        # It would make sense to call next_frame, but it's technically faster
        # to repeat the code here.
        frame = self._frame_buffer.popleft()
        self.dispatch( frame )
      except ProtocolClass.FrameUnderflow:
        return
      except Exception:
        # Spec says that channel should be closed if there's a framing error.
        # Unsure if we can send close if the current exception is transport
        # level (e.g. gevent.GreenletExit)
        self.close( 500, "Failed to dispatch %s"%(str(frame)) )
        raise

  def next_frame(self):
    '''
    Pop the next frame off the input queue. If the queue is empty, will return
    None.
    '''
    if len( self._frame_buffer ):
      return self._frame_buffer.popleft()
    return None

  def requeue_frames(self, frames):
    '''
    Requeue a list of frames. Will append to the head of the frame buffer.
    Frames should be in reverse order. Really only used to support BasicClass
    content consumers
    '''
    self._frame_buffer.extendleft( frames )

  def send_frame(self, frame):
    '''
    Queue a frame for sending.  Will send immediately if there are no pending
    synchronous transactions on this connection.
    '''
    if self.closed:
      if self.close_info and len(self.close_info['reply_text'])>0:
        raise ChannelClosed(
          "channel %d is closed: %s : %s",
          self.channel_id,
          self.close_info['reply_code'],
          self.close_info['reply_text'] )
      raise ChannelClosed()


    # If there's any pending event at all, then it means that when the current
    # dispatch loop started, all possible frames were flushed and the remaining
    # item(s) starts with a sync callback.  After careful consideration, it
    # seems that it's safe to assume the len>0 means to buffer the frame. The
    # other advantage here is 
    if not len(self._pending_events):
      if not self._active and isinstance( frame, (ContentFrame,HeaderFrame) ):
        raise Channel.Inactive( "Channel %d flow control activated", self.channel_id )
      self._connection.send_frame(frame)
    else:
      self._pending_events.append( frame )

  def add_synchronous_cb(self, cb):
    '''
    Add an expectation of a callback to release a synchronous transaction.
    '''
    if self.connection.synchronous:
      wrapper = SyncWrapper(cb)
      self._pending_events.append( wrapper )
      while wrapper._read:
        self.connection.read_frames()
      return wrapper._result
    else:
      self._pending_events.append( cb )

  def clear_synchronous_cb(self, cb):
    '''
    If the callback is the current expected callback, will clear it off the
    stack.  Else will raise in exception if there's an expectation but this
    doesn't satisfy it.
    '''
    if len(self._pending_events):
      ev = self._pending_events[0]

      # We can't have a strict check using this simple mechanism, because we
      # could be waiting for a synch response while messages are being published.
      # So for now, if it's not in the list, do a check to see if the callback
      # is in the pending list, and if so, then raise, because it means we
      # received stuff out of order.  Else just pass it through.
      # Note that this situation could happen on any broker-initiated message.
      if ev==cb:
        self._pending_events.popleft()
        self._flush_pending_events()
        return ev

      elif cb in self._pending_events:
        raise ChannelError("Expected synchronous callback %s, got %s", ev, cb)
    # Return the passed-in callback by default
    return cb

  def _flush_pending_events(self):
    '''
    Send pending frames that are in the event queue.
    '''
    while len(self._pending_events) and isinstance(self._pending_events[0],Frame):
      self._connection.send_frame( self._pending_events.popleft() )

  def _closed_cb(self, final_frame=None):
    '''
    "Private" callback from the ChannelClass when a channel is closed. Only
    called after broker initiated close, or we receive a close_ok. Caller has
    the option to send a final frame, to be used to bypass any synchronous or
    otherwise-pending frames so that the channel can be cleanly closed.
    '''
    # delete all pending data and send final frame if thre is one. note that
    # it bypasses send_frame so that even if the closed state is set, the frame
    # is published.
    if final_frame:
      self._connection.send_frame( final_frame )

    try:
      self._notify_close_listeners()
    finally:
      self._pending_events = deque()
      self._frame_buffer = deque()

      # clear out other references for faster cleanup
      for protocol_class in self._class_map.values():
        protocol_class._cleanup()
        delattr(self, protocol_class.name)
      self._connection = None
      self._class_map = None
      self._close_listeners = set()
