"""
A socket wrapper that uses evented IO.
"""

import socket
import event
import time
import logging
import errno
import traceback

# TODO: Use new io objects from 2.6
# 26 July 10 - I looked into this and a potential problem with io.StringIO is
# that it assumes all text is unicode.  Without a full test and probably lots
# of code updated elsewhere, the older StringIO is probably the better choice
# to fix the bug @AW
#   https://agora.lighthouseapp.com/projects/47111/tickets/628-odd-amqp-error
from cStringIO import StringIO

class EventSocket(object):
  """
  A socket wrapper which uses libevent.
  """

  __MAX_READ_BUFFER = 10485760   # 10 MB

  def __init__( self, family=socket.AF_INET, type=socket.SOCK_STREAM, \
                protocol=socket.IPPROTO_IP, read_cb=None, accept_cb=None, \
                close_cb=None, error_cb=None, output_empty_cb=None, sock=None, \
                debug=False, logger=None, **kwargs):
    """
    Initialize the socket.  If no read_cb defined, socket will only be used
    for reading.  If this socket will be used for accepting new connections,
    set read_cb here and it will be passed to new sockets.  You can also set
    accept_cb and be notified with an EventSocket object on accept().  The
    error_cb will be called if there are any errors on the socket.  The args
    to it will be this socket, an error message, and an optional exception.
    The close_cb will be called when this socket closes, with this socket as
    its argument.  If needed, you can wrap an existing socket by setting the
    sock argument to a socket object.
    """
    self.__debug = debug
    self.__logger = logger
    if self.__debug and not self.__logger:
      print 'WARNING: to debug EventSocket, must provide a logger'
      self.__debug = False

    # There various events we may or may not schedule    
    self.__read_event = None
    self.__write_event = None
    self.__accept_event = None
    self.__pending_read_cb_event = None

    # Cache the peername so we can include it in logs even if the socket
    # is closed.  Note that connect() and bind() have to be the ones to do
    # that work.
    self.__peername = 'unknown'

    if sock:
      self.__sock = sock
  
      try:
        self.__peername = "%s:%d"%self.__sock.getpeername()
        # Like connect(), only initialize these if the socket is already connected.
        self.__read_event = event.read( self.__sock, self.__protected_cb, self.__read_cb )
        self.__write_event = event.write( self.__sock, self.__protected_cb, self.__write_cb )
      except socket.error, e:
        # unconnected
        pass
    else:
      self.__sock = socket.socket(family, type, protocol)

    # wholesale binding of stuff we don't need to alter or intercept
    self.listen = self.__sock.listen
    self.setsockopt = self.__sock.setsockopt
    self.fileno = self.__sock.fileno
    self.getpeername = self.__sock.getpeername
    self.getsockname = self.__sock.getsockname
    self.getsockopt = self.__sock.getsockopt
    self.setblocking = self.__sock.setblocking  # is this correct?
    self.settimeout = self.__sock.settimeout
    self.gettimeout = self.__sock.gettimeout
    self.setsockopt = self.__sock.setsockopt

    self.__write_buf = []
    self.__read_buf = StringIO()

    self.__parent_accept_cb = accept_cb
    self.__parent_read_cb = read_cb
    self.__parent_error_cb = error_cb
    self.__parent_close_cb = close_cb
    self.__parent_output_empty_cb = output_empty_cb

    # This is the pending global error message.  It's sort of a hack, but it's
    # used for __protected_cb in much the same way as errno.  This prevents
    # having to pass an error message around, when the best way to do that is
    # via kwargs that the event lib is itself trying to interpret and won't
    # allow to pass to __protected_cb.
    self.__error_msg = None
    self.__closed = False

    self.__inactive_event = None
    self.setinactivetimeout( 0 )

  def close(self):
    """
    Close the socket.
    """
    # if self.__debug:
    #   self.__logger.debug(\
    #     "closing connection %s to %s"%(self.__sock.getsockname(), self.__peername) )

    # Unload all our events
    if self.__read_event:
      self.__read_event.delete()
      self.__read_event = None
    if self.__accept_event:
      self.__accept_event.delete()
      self.__accept_event = None
    if self.__inactive_event:
      self.__inactive_event.delete()
      self.__inactive_event = None
    if self.__write_event:
      self.__write_event.delete()
      self.__write_event = None

    if self.__sock:
      self.__sock.close()
      self.__sock = None
    
    # Flush any pending data to the read callbacks as appropriate.  Do this
    # manually as there is a chance for the following race condition to occur:
    #   pending data read by cb
    #   callback reads 1.1 messages, re-buffers .1 msg back
    #   callback disconnects from socket based on message, calling close()
    #   we get back to this code and find there's still data in the input buffer
    #     and the read cb hasn't been cleared.  ruh roh.
    if self.__parent_read_cb and self.__read_buf.tell()>0:
      cb = self.__parent_read_cb
      self.__parent_read_cb = None
      self.__error_msg = "error processing remaining socket input buffer"
      self.__protected_cb( cb, self )

    # Only mark as closed after socket is really closed, we've flushed buffered
    # input, and we're calling back to close handlers.
    self.__closed = True
    if self.__parent_close_cb:
      self.__parent_close_cb( self )
    
    if self.__pending_read_cb_event: 
      self.__pending_read_cb_event.delete()
      self.__pending_read_cb_event = None
    
    if self.__inactive_event:
      self.__inactive_event.delete()
      self.__inactive_event = None
    
    # Delete references to callbacks to help garbage collection
    self.__parent_accept_cb = None
    self.__parent_read_cb = None
    self.__parent_error_cb = None
    self.__parent_close_cb = None
    self.__parent_output_empty_cb = None
    
    # Clear buffers
    self.__write_buf = None
    self.__read_buf = None

  def accept(self):
    """
    No-op as we no longer perform blocking accept calls.
    """
    return None

  def __set_read_cb(self, cb):
    """
    Set the read callback.  If there's data in the output buffer, immediately
    setup a call.
    """
    self.__parent_read_cb = cb
    if self.__read_buf.tell()>0 and self.__parent_read_cb!=None and self.__pending_read_cb_event==None:
      self.__pending_read_cb_event = \
        event.timeout( 0, self.__protected_cb, self.__parent_read_timer_cb )

  # Allow someone to change the various callbacks.
  read_cb =   property( fset=__set_read_cb )
  accept_cb = property( fset=lambda self,func: setattr(self, '_EventSocket__parent_accept_cb', func ) )
  close_cb =  property( fset=lambda self,func: setattr(self, '_EventSocket__parent_close_cb', func ) )
  error_cb =  property( fset=lambda self,func: setattr(self, '_EventSocket__parent_error_cb', func ) )
  output_empty_cb = property( fset=lambda self,func: setattr(self, '_EventSocket__parent_output_empty_cb',func) )

  def bind(self, *args):
    """
    Bind the socket.
    """
    if self.__debug:
      self.__logger.debug("binding to %s"%(args))

    self.__sock.bind( *args )
    self.__peername = "%s:%d"%self.getsockname()

    self.__accept_event = event.read( self, self.__protected_cb, self.__accept_cb )

  def connect(self, *args):
    """
    Connect the socket.
    """
    self.__sock.connect( *args )
    self.__peername = "%s:%d"%self.getpeername()
    
    # 7 Aug 09 aaron - Only set these up after a successful connection, else
    # an error could be raised by the read event that is passed back to the parent
    # error handler, which can then cause a loop if it assumes that the error is
    # normally generated by what was an otherwise-functional socket.  Of course
    # if no connect ever happened then it can't have been a functional socket.
    self.__read_event = event.read( self.__sock, self.__protected_cb, self.__read_cb )
    self.__write_event = event.write( self.__sock, self.__protected_cb, self.__write_cb )

  def setinactivetimeout(self, t):
    """
    Set the inactivity timeout.  If is None or 0, there is no activity timeout.
    If t>0 then socket will automatically close if there has been no activity
    after t seconds (float supported).  Will raise TypeError if <t> is invalid.
    """
    if t==None or t==0:
      if self.__inactive_event:
        self.__inactive_event.delete()
        self.__inactive_event = None
      self.__inactive_timeout = 0
    elif isinstance(t,(int,long,float)):
      if self.__inactive_event:
        self.__inactive_event.delete()
      self.__inactive_event = event.timeout( t, self.__inactive_cb )
      self.__inactive_timeout = t
    else:
      raise TypeError( "invalid timeout %s"%(str(t)) )
   
  ### Private support methods
  def __handle_error(self, exc):
    '''
    Gracefully handle errors.
    '''
    if self.__parent_error_cb:
      if self.__error_msg!=None:
        self.__parent_error_cb( self, self.__error_msg, exc )
      else:
        self.__parent_error_cb( self, "unknown error", exc )
    else:
      if self.__error_msg!=None:
        msg = "unhandled error %s"%(self.__error_msg)
      else:
        msg = "unhandled unknown error"
      if self.__logger:
        self.__logger.error( msg, exc_info=True )
      else:
        traceback.print_exc()
    
  def __protected_cb(self, cb, *args, **kwargs):
    """
    Wrap any callback from libevent so that we can be sure that exceptions are
    handled and errors forwarded to error_cb.
    """
    rval = None

    try:
      rval = cb(*args, **kwargs)
    except Exception, e:
      self.__handle_error( e )

    self.__error_msg = None
    return rval

  def __accept_cb(self):
    """
    Accept callback from libevent.
    """
    self.__error_msg = "error accepting new socket"
    (conn, addr) = self.__sock.accept()
    if self.__debug:
      self.__logger.debug("accepted connection from %s"%(str(addr)))


    evsock = EventSocket( read_cb=self.__parent_read_cb, \
                            error_cb=self.__parent_error_cb, \
                            close_cb=self.__parent_close_cb, sock=conn,
                            debug=self.__debug, logger=self.__logger )

    if self.__parent_accept_cb:
      # 31 march 09 aaron - We can't call accept callback asynchronously in the
      # event that the socket is quickly opened and closed.  What happens is
      # that a read event gets scheduled before __parent_accept_cb is run, and
      # since the socket is closed, it calls the __parent_close_cb.  If the
      # socket has not been correctly initialized though, we may encounter
      # errors if the close_cb is expected to be changed during the accept
      # callback.  This is arguably an application-level problem, but handling
      # that situation entirely asynchronously would be a giant PITA and prone
      # to bugs.  We'll avoid that.
      self.__protected_cb( self.__parent_accept_cb, evsock )

    # Still reschedule event even if there was an error.
    return True

  def __read_cb(self):
    """
    Read callback from libevent.
    """
    self.__error_msg = "error reading from socket"
    data = self.__sock.recv( self.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF) )
    if len(data)>0:
      if self.__debug:
        self.__logger.debug( "read %d bytes from %s"%(len(data), self.__peername) )
      # 23 Feb 09 aaron - There are cases where the client will have started
      # pushing data right away, and there's a chance that async handling of
      # accept will cause data to be read before the callback function has been
      # set.  I prefer to ignore data if no read callback defined, but it's
      # better to just limit the overall size of the input buffer then to use
      # a synchronous callback to __parent_accept_cb.
      # TODO: So what is the best way of handling this problem, and if sticking
      # with a max input buffer size, what's the correct algorithm?  Maybe better
      # approach is to raise a notice to a callback and let the callback decide
      # what to do.
      self.__flag_activity()
      self.__read_buf.write( data )

      if self.__read_buf.tell() > self.__MAX_READ_BUFFER:
        if self.__debug:
          self.__logger.debug( "buffer for %s overflowed!"%(self.__peername) )
        rebuf = self.__read_buf.getvalue()[ (self.__MAX_READ_BUFFER/2) : ]
        self.__read_buf = StringIO()
        self.__read_buf.write( rebuf )
  
      # Callback asynchronously so that priority is given to libevent to
      # allocate time slices.
      if self.__parent_read_cb!=None and self.__pending_read_cb_event==None:
        self.__pending_read_cb_event = \
          event.timeout( 0, self.__protected_cb, self.__parent_read_timer_cb )

    else:
      self.close()
      return None
    return True

  def __parent_read_timer_cb(self):
    """
    Callback when we want the parent to read buffered data.
    """
    # Shouldn't need to check closed state because all events should be
    # cancelled, but there seems to be a case where that can happen so deal
    # with it gracefully.  Possibly a bug in libevent when tons of events are
    # in play as this only happened during extreme testing.
    if not self.__closed:
      self.__error_msg = "error processing socket input buffer"
      self.__pending_read_cb_event = None   # allow for __close_cb and __read_cb to do their thing.
      self.__parent_read_cb( self )
    return None                           # never reschedule, let read do that.

  def __write_cb(self):
    """
    Write callback from libevent.
    """
    self.__error_msg = "error writing socket output buffer"

    # If no data, don't reschedule
    if len(self.__write_buf)==0:
      return None

    # 7 April 09 aaron - Changed this algorithm so that we continually send
    # data from the buffer until the socket didn't accept all of it, then
    # break.  This should be a bit faster.
    total_sent = 0
    total_len = sum( map(len,self.__write_buf) )
    while len(self.__write_buf)>0:
      cur = self.__write_buf[0]
      
      # Catch all env errors since that should catch OSError, IOError and
      # socket.error.
      try:
        bytes_sent = self.__sock.send( cur )
      except EnvironmentError, e:
        # For now this seems to be the only error that isn't fatal.  It seems
        # to be used only for nonblocking sockets and implies that it can't
        # buffer any more data right now.
        if e.errno==errno.EAGAIN:
          if self.__debug:
            self.__logger.debug( '"%s" raised, waiting to flush to %s'%( e, self.__peername ) )
          break
        else:
          raise e

      total_sent += bytes_sent

      if bytes_sent < len(cur):
        # keep the first entry and set to all remaining bytes.
        self.__write_buf[0] = cur[bytes_sent:]
        break
      else:
        # done with this piece of data.
        self.__write_buf.pop(0)
    
    if self.__debug:
      self.__logger.debug( "wrote %d/%d bytes to %s"%(total_sent,total_len,self.__peername) )
      
    # also flag activity here?  might not be necessary, but in some cases the
    # timeout could still be small enough to trigger between accesses to the
    # socket output.
    self.__flag_activity()    
    
    if len(self.__write_buf)>0:
      return True

    if self.__parent_output_empty_cb!=None:
      self.__parent_output_empty_cb( self )
    return None

  def __inactive_cb(self):
    """
    Timeout when a socket has been inactive for a long time.
    """
    self.__error_msg = "error closing inactive socket"
    self.close()

  def __flag_activity(self):
    """
    Flag that this socket is active.
    """
    # is there a better way of reseting a timer?
    if self.__inactive_event:
      self.__inactive_event.delete()
      self.__inactive_event = event.timeout( self.__inactive_timeout, self.__protected_cb, self.__inactive_cb )

  def write(self, data):
    """
    Write some data.  Will raise socket.error if connection is closed.
    """
    if self.__closed:
      raise socket.error('write error: socket is closed')

    if self.__write_event:
      self.__write_buf.append( data )

      # 21 July 09 aaron - I'm not sure if this has a significant benefit, but in
      # trying to improve throughput I confirmed that this doesn't break anything
      # and keeping the event queue cleaner is certainly good.
      if not self.__write_event.pending():
        self.__write_event.add()
    
      if self.__debug > 1:
        self.__logger.debug("buffered %d bytes (%d total) to %s"%\
          (len(data), sum(map(len,self.__write_buf)), self.__peername) )

    # Flag activity here so we don't timeout in case that event is ready to
    # fire and we're just now writing.
    self.__flag_activity()

  def read(self):
    """
    Return the current read buffer.  Will return an IO object (StringIO).
    """
    if self.__closed:
      raise socket.error('read error: socket is closed')

    rval = self.__read_buf
    rval.seek(0, 0)
    self.__read_buf = StringIO()
    return rval

  def buffer(self, s):
    '''
    Re-buffer a string to the input.  Will put it at the beginning of the buffer.
    '''
    self.__read_buf.write( s )
