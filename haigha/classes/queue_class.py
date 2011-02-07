from haigha.writer import Writer
from haigha.frames import MethodFrame
from haigha.classes import ProtocolClass

class QueueClass(ProtocolClass):
  '''
  Implements the AMQP Queue class
  '''

  def __init__(self, *args, **kwargs):
    super(QueueClass, self).__init__(*args, **kwargs)
    self.dispatch_map = {
      11 : self._recv_declare_ok,
      21 : self._recv_bind_ok,
      31 : self._recv_purge_ok,
      41 : self._recv_delete_ok,
      51 : self._recv_unbind_ok,
    }

    self._declare_cb = []
    self._bind_cb = []
    self._unbind_cb = []
    self._delete_cb = []
    self._purge_cb = []

  def declare(self, queue='', passive=False, durable=False,
      exclusive=False, auto_delete=True, nowait=True,
      arguments={}, ticket=None, cb=None):
    '''
    Declare a queue.  By default is asynchronoous but will be synchronous if nowait=False
    or a callback is defined.

    queue - The name of the queue
    cb - An optional method which will be called with (queue_name, msg_count, consumer_count)
         if nowait=False
    '''
    # If a callback is defined, then we have to use synchronous transactions.
    if cb: nowait = False

    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(queue)
    #args.write_bit(passive)
    #args.write_bit(durable)
    #args.write_bit(exclusive)
    #args.write_bit(auto_delete)
    #args.write_bit(nowait)
    args.write_bits(passive, durable, exclusive, auto_delete, nowait)
    args.write_table(arguments)
    self.send_frame( MethodFrame(self.channel_id, 50, 10, args) )

    if not nowait:
      self.channel.add_synchronous_cb( self._recv_declare_ok )
      self._declare_cb.append( cb )

  def _recv_declare_ok(self, method_frame):
    queue = method_frame.args.read_shortstr()
    message_count = method_frame.args.read_long()
    consumer_count = method_frame.args.read_long()

    cb = self._declare_cb.pop(0)
    if cb: cb( queue, message_count, consumer_count )
    
  def bind(self, queue, exchange, routing_key='', nowait=True, arguments={}, ticket=None, cb=None):
    '''
    bind to a queue.
    '''
    # If a callback is defined, then we have to use synchronous transactions.
    if cb: nowait = False

    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(queue)
    args.write_shortstr(exchange)
    args.write_shortstr(routing_key)
    args.write_bit(nowait)
    args.write_table(arguments)
    self.send_frame( MethodFrame(self.channel_id, 50, 20, args) )
    
    if not nowait:
      self.channel.add_synchronous_cb( self._recv_bind_ok )
      self._bind_cb.append( cb )

  def _recv_bind_ok(self, method_frame):
    # No arguments defined.
    cb = self._bind_cb.pop(0)
    if cb: cb()

  def unbind(self, queue, exchange, routing_key, arguments=None, ticket=None, cb=None):
    '''
    Unbind a queue from an exchange.  This is always synchronous.
    '''
    if arguments is None: arguments = {}

    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(queue)
    args.write_shortstr(exchange)
    args.write_shortstr(routing_key)
    args.write_table(arguments)
    self.send_frame( MethodFrame(self.channel_id, 50, 50, args) )

    self.channel.add_synchronous_cb( self._recv_unbind_ok )
    self._unbind_cb.append( cb )

  def _recv_unbind_ok(self):
    # No arguments defined
    cb = self._unbind_cb.pop(0)
    if cb: cb()
    
  def purge(self, queue, nowait=True, ticket=None, cb=None):
    '''
    Purge all messages in a queue.
    '''
    # If a callback is defined, then we have to use synchronous transactions.
    if cb: nowait = False
    
    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(queue)
    args.write_bit(nowait)
    self.send_frame( MethodFrame(self.channel_id, 50, 30, args) )

    if not nowait:
      self.channel.add_synchronous_cb( self._recv_purge_ok )
      self._purge_cb.append( cb )

  def _recv_purge_ok(self, method_frame):
    message_count = method_frame.args.read_long()
    cb = self._purge_cb.pop(0)
    if cb: cb( message_count )

  def delete(self, queue, if_unused=False, if_empty=False, nowait=True, ticket=None, cb=None):
    '''
    queue delete.
    '''
    # If a callback is defined, then we have to use synchronous transactions.
    if cb: nowait = False

    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
       args.write_short(self.default_ticket)

    args.write_shortstr(queue)
    #args.write_bit(if_unused)
    #args.write_bit(if_empty)
    #args.write_bit(nowait)
    args.write_bits(if_unused, if_empty, nowait)
    self.send_frame( MethodFrame(self.channel_id, 50, 40, args) )

    if not nowait:
      self.channel.add_synchronous_cb( self._recv_delete_ok )
      self._delete_cb.append( cb )

  def _recv_delete_ok(self, method_frame):
    message_count = method_frame.args.read_long()
    cb = self._delete_cb.pop(0)
    if cb: cb( message_count )
