'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from collections import deque

from haigha.writer import Writer
from haigha.classes import ProtocolClass
from haigha.frames import MethodFrame

class ExchangeClass(ProtocolClass):
  '''
  Implements the AMQP Exchange class
  '''

  def __init__(self, *args, **kwargs):
    super(ExchangeClass, self).__init__(*args, **kwargs)
    self.dispatch_map = {
      11 : self._recv_declare_ok,
      21 : self._recv_delete_ok,
    }

    self._declare_cb = deque()
    self._delete_cb = deque()

  def _cleanup(self):
    '''
    Cleanup local data.
    '''
    self._declare_cb = None
    self._delete_cb = None
    super(ExchangeClass,self)._cleanup()

  def declare(self, exchange, type, passive=False, durable=False,\
      auto_delete=True, internal=False, nowait=True, arguments=None, \
      ticket=None, cb=None):
    """
    Declare the exchange.

    exchange - The name of the exchange to declare
    type - One of 
    """
    # If a callback is defined, then we have to use synchronous transactions.
    if cb: nowait = False

    args = Writer()
    args.write_short(ticket or self.default_ticket).\
      write_shortstr(exchange).\
      write_shortstr(type).\
      write_bits(passive, durable, auto_delete, internal, nowait).\
      write_table(arguments or {})
    self.send_frame( MethodFrame(self.channel_id, 40, 10, args) )

    if not nowait:
      self.channel.add_synchronous_cb( self._recv_declare_ok )
      self._declare_cb.append( cb )

  def _recv_declare_ok(self, _method_frame):
    '''
    Confirmation that exchange was declared.
    '''
    cb = self._declare_cb.popleft()
    if cb: cb()
    
  def delete(self, exchange, if_unused=False, nowait=True, ticket=None, cb=None):
    '''
    Delete an exchange.
    '''
    # If a callback is defined, then we have to use synchronous transactions.
    if cb: nowait = False

    args = Writer()
    args.write_short(ticket or self.default_ticket).\
      write_shortstr(exchange).\
      write_bits(if_unused, nowait)
    self.send_frame( MethodFrame(self.channel_id, 40, 20, args) )
    
    if not nowait:
      self.channel.add_synchronous_cb( self._recv_delete_ok )
      self._delete_cb.append( cb )

  def _recv_delete_ok(self, _method_frame):
    '''
    Confirmation that exchange was deleted.
    '''
    cb = self._delete_cb.popleft()
    if cb: cb()
