'''
Copyright (c) 2011-2013, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.classes import ProtocolClass
from haigha.frames import MethodFrame
from haigha.writer import Writer

class ChannelClass(ProtocolClass):
  '''
  Implements the AMQP Channel class
  '''
  
  def __init__(self, *args, **kwargs):
    super(ChannelClass, self).__init__(*args, **kwargs)
    self.dispatch_map = {
      11 : self._recv_open_ok,
      20 : self._recv_flow,
      21 : self._recv_flow_ok,
      40 : self._recv_close,
      41 : self._recv_close_ok,
    }
    self._flow_control_cb = None

  @property
  def name(self):
    return 'channel'

  def set_flow_cb(self, cb):
    '''
    Set a callback that will be called when the state of flow control has changed.
    The caller should use closures if they need to receive a handle to the channel
    on which flow control changes.
    '''
    self._flow_control_cb = cb
  
  def open(self):
    '''
    Open the channel for communication.
    '''
    args = Writer()
    args.write_shortstr('')
    self.send_frame( MethodFrame(self.channel_id, 20, 10, args) )
    self.channel.add_synchronous_cb( self._recv_open_ok )

  def _recv_open_ok(self, method_frame):
    '''
    Channel is opened.
    '''
    self.channel._notify_open_listeners()

  def activate(self):
    '''
    Activate this channel (disable flow control).
    '''
    if not self.channel.active:
      self._send_flow( True )

  def deactivate(self):
    '''
    Deactivate this channel (enable flow control).
    '''
    if self.channel.active:
      self._send_flow( False )

  def _send_flow(self, active):
    '''
    Send a flow control command.
    '''
    args = Writer()
    args.write_bit( active )
    self.send_frame( MethodFrame(self.channel_id, 20, 20, args) )
    self.channel.add_synchronous_cb( self._recv_flow_ok )

  def _recv_flow(self, method_frame):
    '''
    Receive a flow control command from the broker
    '''
    self.channel._active = method_frame.args.read_bit()
    
    args = Writer()
    args.write_bit( self.channel.active )
    self.send_frame( MethodFrame(self.channel_id, 20, 21, args) )

    if self._flow_control_cb is not None:
      self._flow_control_cb()

  def _recv_flow_ok(self, method_frame):
    '''
    Receive a flow control ack from the broker.
    '''
    self.channel._active = method_frame.args.read_bit()
    if self._flow_control_cb is not None:
      self._flow_control_cb()

  def close(self, reply_code=0, reply_text='', class_id=0, method_id=0):
    '''
    Close this channel.  Caller has the option of specifying the reason for
    closure and the class and method ids of the current frame in which an error
    occurred.  If in the event of an exception, the channel will be marked
    as immediately closed.  If channel is already closed, call is ignored.
    '''
    if not getattr(self, 'channel', None) or self.channel._closed: return

    self.channel._close_info = {
      'reply_code'    : reply_code,
      'reply_text'    : reply_text,
      'class_id'      : class_id,
      'method_id'     : method_id
    }

    # exceptions here likely due to race condition as connection is closing
    # cap the reply_text we send because it may be arbitrarily long
    try:
      args = Writer()
      args.write_short( reply_code )
      args.write_shortstr( reply_text[:255] )
      args.write_short( class_id )
      args.write_short( method_id )
      self.send_frame( MethodFrame(self.channel_id, 20, 40, args) )
      
      self.channel.add_synchronous_cb( self._recv_close_ok )
    finally:
      # Immediately set the closed flag so that no more frames can be sent
      # NOTE: in synchronous mode, by the time this is called we will have
      # already run self.channel._closed_cb and so the channel reference is
      # gone.
      if self.channel:
        self.channel._closed = True

  def _recv_close(self, method_frame):
    '''
    Receive a close command from the broker.
    '''
    self.channel._close_info = {
      'reply_code'    : method_frame.args.read_short(),
      'reply_text'    : method_frame.args.read_shortstr(),
      'class_id'      : method_frame.args.read_short(),
      'method_id'     : method_frame.args.read_short()
    }

    self.channel._closed = True
    self.channel._closed_cb( final_frame=MethodFrame(self.channel_id, 20, 41) )

  def _recv_close_ok(self, method_frame):
    '''
    Receive a close ack from the broker.
    '''
    self.channel._closed = True
    self.channel._closed_cb()
