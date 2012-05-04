'''
Copyright (c) 2012, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from collections import deque

from haigha.connection import Connection
from haigha.classes import *
from haigha.writer import Writer
from haigha.frames import MethodFrame

class RabbitConnection(Connection):
  '''
  A connection specific to RabbitMQ that supports its extensions.
  '''

  def __init__(self, **kwargs):
    '''
    Initialize the connection
    '''
    class_map = kwargs.get('class_map',{}).copy()
    class_map.setdefault(40, RabbitExchangeClass)
    class_map.setdefault(60, RabbitBasicClass)
    class_map.setdefault(85, RabbitConfirmClass)
    kwargs['class_map'] = class_map

    super(RabbitConnection,self).__init__(**kwargs)

class RabbitExchangeClass(ExchangeClass):
  '''
  Exchange class Rabbit extensions
  '''
  
  def __init__(self, *args, **kwargs):
    super(RabbitExchangeClass,self).__init__(*args, **kwargs)
    self.dispatch_map[31] = self._recv_bind_ok
    self.dispatch_map[51] = self._recv_unbind_ok

    self._bind_cb = deque()
    self._unbind_cb = deque()

  # I hate the code copying here
  def declare(self, exchange, type, passive=False, durable=False,
      auto_delete=True, internal=False, nowait=True, internal=False,
      arguments=None, ticket=None, cb=None):
    """
    Declare the exchange.

    exchange - The name of the exchange to declare
    type - One of 
    """
    nowait = nowait and self.allow_nowait() and not cb

    args = Writer()
    args.write_short(ticket or self.default_ticket).\
      write_shortstr(exchange).\
      write_shortstr(type).\
      write_bits(passive, durable, auto_delete, internal, nowait).\
      write_table(arguments or {})
    self.send_frame( MethodFrame(self.channel_id, 40, 10, args) )

    if not nowait:
      self._declare_cb.append( cb )
      self.channel.add_synchronous_cb( self._recv_declare_ok )

  def bind(self, exchange, source, routing_key='', nowait=True, 
      arguments={}, ticket=None, cb=None):
    '''
    Bind an exchange to another.
    '''
    nowait = nowait and self.allow_nowait() and not cb
    
    args = Writer()
    args.write_short(ticket or self.default_ticket).\
      write_shortstr(exchange).\
      write_shortstr(source).\
      write_shortstr(routing_key).\
      write_bit(nowait).\
      write_table(arguments or {})
    self.send_frame( MethodFrame(self.channel_id, 40, 30, args) )

    if not nowait:
      self._bind_cb.append( cb )
      self.channel.add_synchronous_cb( self._recv_bind_ok )

  def _recv_bind_ok(self, _method_frame):
    '''Confirm exchange bind.'''
    cb = self._bind_cb.popleft()
    if cb: cb()

  def unbind(self, exchange, source, routing_key='', nowait=True, 
      arguments={}, ticket=None, cb=None):
    '''
    Unbind an exchange from another.
    '''
    nowait = nowait and self.allow_nowait() and not cb
    
    args = Writer()
    args.write_short(ticket or self.default_ticket).\
      write_shortstr(exchange).\
      write_shortstr(source).\
      write_shortstr(routing_key).\
      write_bit(nowait).\
      write_table(arguments or {})
    self.send_frame( MethodFrame(self.channel_id, 40, 40, args) )

    if not nowait:
      self._unbind_cb.append( cb )
      self.channel.add_synchronous_cb( self._recv_unbind_ok )

  def _recv_unbind_ok(self, _method_frame):
    '''Confirm exchange unbind.'''
    cb = self._unbind_cb.popleft()
    if cb: cb()

class RabbitBasicClass(BasicClass):
  '''
  Support Rabbit extensions to Basic class.
  '''

  def __init__(self, *args, **kwargs):
    super(RabbitBasicClass,self).__init__(*args, **kwargs)
    self.dispatch_map[80] = self._recv_ack
    self.dispatch_map[120] = self._recv_nack

    self._bind_cb = deque()
    self._unbind_cb = deque()

  # TODO: deal with ack, nack and publisher confirms

  def _recv_ack(self, method_frame):
    '''Receive an ack from the broker.'''
    delivery_tag = method_frame.args.read_longlong    
    multiple = method_frame.args.read_bit()
    # TODO: clear some tracker of messages

  def nack(self, delivery_tag, multiple=False, requeue=False):
    '''Send a nack to the broker.'''
    args = Writer()
    args.write_longlong(delivery_tag).\
      write_bits(multiple, requeue)

    self.send_frame( MethodFrame(self.channel_id, 60, 120, args) )

  def _recv_nack(self, method_frame):
    '''Receive a nack from the broker.'''
    delivery_tag = method_frame.args.read_longlong    
    multiple, requeue = method_frame.args.read_bits(2)
    # TODO: call back to user code with messages that were nackered

class RabbitConfirmClass(ProtocolClass):
  '''
  Implementation of Rabbit's confirm class.
  '''
  
  def __init__(self, *args, **kwargs):
    super(RabbitConfirmClass, self).__init__(*args, **kwargs)
    self.dispatch_map = {
      11 : self._recv_select_ok,
    }

    self._enabled = False
    self._select_cb = deque()

  @property
  def name(self):
    return 'confirm'

  def select(self, nowait=True, cb=None):
    '''
    Set this channel to use publisher confirmations.
    '''
    nowait = nowait and self.allow_nowait() and not cb

    if not self._enabled:
      self._enabled = True
      args = Writer()
      args.write_bit(nowait)

      self.send_frame( MethodFrame(self.channel_id, 85, 10, args) )

      if not nowait:
        self._select_cb.append(cb)
        self.channel.add_synchronous_cb( self._recv_select_ok )

  def _recv_select_ok(self, _method_frame):
    cb = self._select_cb.popleft()
    if cb: cb()
