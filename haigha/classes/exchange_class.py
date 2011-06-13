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


  def declare(self, exchange, type, passive=False, durable=False,\
      auto_delete=True, internal=False, nowait=True, arguments=None, ticket=None):
    """
    Declare the exchange.

    exchange - The name of the exchange to declare
    type - One of 
    """
    args = Writer()
    args.write_short(ticket or self.default_ticket).\
      write_shortstr(exchange).\
      write_shortstr(type).\
      write_bits(passive, durable, auto_delete, internal, nowait).\
      write_table(arguments or {})
    self.send_frame( MethodFrame(self.channel_id, 40, 10, args) )

    if not nowait:
      self.channel.add_synchronous_cb( self._recv_declare_ok )
    
  def delete(self, exchange, if_unused=False, nowait=True, ticket=None):
    '''
    Delete an exchange.
    '''
    args = Writer()
    args.write_short(ticket or self.default_ticket).\
      write_shortstr(exchange).\
      write_bits(if_unused, nowait)
    self.send_frame( MethodFrame(self.channel_id, 40, 20, args) )
    
    if not nowait:
      self.channel.add_synchronous_cb( self._recv_delete_ok )

  def _recv_declare_ok(self, _method_frame):
    '''
    Confirmation that exchange was declared.
    '''
    # No arguments in method frame, nothing to do

  def _recv_delete_ok(self, _method_frame):
    '''
    Confirmation that exchange was deleted.
    '''
    # No arguments in method frame, nothing to do
