from collections import deque
from io import BytesIO

from haigha.message import Message
from haigha.writer import Writer
from haigha.frames import MethodFrame, HeaderFrame, ContentFrame
from haigha.classes import ProtocolClass

class BasicClass(ProtocolClass):
  '''
  Implements the AMQP Basic class
  '''

  def __init__(self, *args, **kwargs):
    super(BasicClass, self).__init__(*args, **kwargs)
    self.dispatch_map = {
      11 : self._recv_qos_ok,
      21 : self._recv_consume_ok,
      31 : self._recv_cancel_ok,
      50 : self._recv_return,
      60 : self._recv_deliver,
      71 : self._recv_get_response,   # see impl
      72 : self._recv_get_response,   # see impl
      111 : self._recv_recover_ok,
    }

    self._consumer_tag_id = 0
    self._pending_consumers = []
    self._consumer_cb = {}
    self._get_cb = []
    self._recover_cb = []
    self._cancel_cb = []

  def _generate_consumer_tag(self):
    '''
    Generate the next consumer tag.

    The consumer tag is local to a channel, so two clients can use the
    same consumer tags.
    '''
    self._consumer_tag_id += 1
    return "channel-%d-%d"%( self.channel_id, self._consumer_tag_id )
  
  def qos(self, prefetch_size=0, prefetch_count=0, is_global=False):
    '''
    Set QoS on this channel.
    '''
    args = Writer()
    args.write_long(prefetch_size)
    args.write_short(prefetch_count)
    args.write_bit(is_global)
    self.send_frame( MethodFrame(self.channel_id, 60, 10, args) )

    self.channel.add_synchronous_cb( self._recv_qos_ok )

  def _recv_qos_ok(self, _method_frame):
    # No arguments, nothing to do
    pass
    
  def consume(self, queue, consumer, consumer_tag='', no_local=False,
        no_ack=True, exclusive=False, nowait=True, ticket=None):
    '''
    start a queue consumer.
    '''
    if nowait and consumer_tag=='':
      consumer_tag = self._generate_consumer_tag()

    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(queue)
    args.write_shortstr(consumer_tag)
    args.write_bits( no_local, no_ack, exclusive, nowait )
    args.write_table({})
    self.send_frame( MethodFrame(self.channel_id, 60, 20, args) )

    if not nowait:
      self.channel.add_synchronous_cb( self._recv_consume_ok )
      self._pending_consumers.append( consumer )
    else:
      self._consumer_cb[ consumer_tag ] = consumer

  def _recv_consume_ok(self, method_frame):
    consumer_tag = method_frame.args.read_shortstr()
    self._consumer_cb[ consumer_tag ] = self._pending_consumers.pop(0)

  def cancel(self, consumer_tag='', nowait=True, consumer=None, cb=None):
    '''
    Cancel a consumer. Can choose to delete based on a consumer tag or the
    function which is consuming.  If deleting by function, take care to only
    use a consumer once per channel.

    Callbacks only apply if nowait=False
    '''
    if consumer:
      for (tag,func) in self._consumer_cb.iteritems():
        if func==consumer:
          consumer_tag = tag
          break

    args = Writer()
    args.write_shortstr(consumer_tag)
    args.write_bit(nowait)
    self.send_frame( MethodFrame(self.channel_id, 60, 30, args) )

    if not nowait:
      self.channel.add_synchronous_cb( self._recv_cancel_ok )
      self._cancel_cb.append( cb )
    else:
      try:
        del self._consumer_cb[consumer_tag]
      except KeyError:
        self.logger.warning( 'no callback registered for consumer tag " %s "', consumer_tag )

  def _recv_cancel_ok(self, method_frame):
    consumer_tag = method_frame.args.read_shortstr()
    try:
      del self._consumer_cb[consumer_tag]
    except KeyError:
      self.logger.warning( 'no callback registered for consumer tag " %s "', consumer_tag )

    cb = self._cancel_cb.pop(0)
    if cb is not None: cb()
    
  def publish(self, msg, exchange, routing_key, mandatory=False, immediate=False, ticket=None):
    '''
    publish a message.
    '''
    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(exchange)
    args.write_shortstr(routing_key)
    #args.write_bit(mandatory)
    #args.write_bit(immediate)
    args.write_bits(mandatory, immediate)

    self.send_frame( MethodFrame(self.channel_id, 60, 40, args) )
    self.send_frame( HeaderFrame(self.channel_id, 60, 0, len(msg), msg.properties) )

    frame_max = self.channel.connection.frame_max
    for frame in ContentFrame.create_frames(self.channel_id, msg.body, frame_max):
      self.send_frame( frame )

  def return_msg(self, reply_code, reply_text, exchange, routing_key):
    '''
    Return a failed message.  Not named "return" because python interpreter
    can't deal with that.
    '''
    args = Writer()
    args.write_short( reply_code )
    args.write_shortstr( reply_text )
    args.write_shortstr( exchange )
    args.write_shortstr( routing_key )

    self.send_frame( MethodFrame(self.channel_id, 60, 50, args) )

  def _recv_return(self, _method_frame):
    # This seems like the right place to callback that the operation has
    # completed.
    pass

  def _recv_deliver(self, method_frame):
    # No need to assert that is instance of Header or Content frames because
    # failure to access as such will result in exception that channel will
    # pick up and handle accordingly.
    header_frame = self.channel.next_frame()
    if header_frame:
      size = header_frame.size
      body = bytearray()
      rbuf_frames = deque([header_frame, method_frame])
      
      while len(body) < size:
        content_frame = self.channel.next_frame()
        if content_frame:
          rbuf_frames.appendleft( content_frame )
          body.extend( content_frame.payload.buffer() )
        else:
          self.channel.requeue_frames( rbuf_frames )
          raise self.FrameUnderflow()
    else:
      self.channel.requeue_frames( [method_frame] )
      raise self.FrameUnderflow()

    consumer_tag = method_frame.args.read_shortstr()
    delivery_tag = method_frame.args.read_longlong()
    redelivered = method_frame.args.read_bit()
    exchange = method_frame.args.read_shortstr()
    routing_key = method_frame.args.read_shortstr()

    delivery_info = {
      'channel': self.channel,
      'consumer_tag': consumer_tag,
      'delivery_tag': delivery_tag,
      'redelivered': redelivered,
      'exchange': exchange,
      'routing_key': routing_key,
    }
    msg = Message( body=body, delivery_info=delivery_info, **header_frame.properties )
    
    func = self._consumer_cb.get(consumer_tag, None)
    if func: func(msg)

  def get(self, queue, consumer, no_ack=True, ticket=None):
    '''
    Ask to fetch a single message from a queue.  The consumer will be called
    if an actual message exists, but if not, the consumer will not be called.
    '''
    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(queue)
    args.write_bit(no_ack)

    self._get_cb.append( consumer )
    self.send_frame( MethodFrame(self.channel_id, 60, 70, args) )
    self.channel.add_synchronous_cb( self._recv_get_response )

  def _recv_get_response(self, method_frame, *content_frames):
    '''
    Handle either get_ok or get_empty.  This is a hack because the synchronous
    callback stack is expecting one method to satisfy the expectation.  To
    keep that loop as tight as possible, work within those constraints. Use
    of get is not recommended anyway.
    '''
    if method_frame.method_id==71:
      self._recv_get_ok( method_frame, *content_frames )
    elif method_frame.method_id==72:
      self._recv_get_empty( method_frame )

  def _recv_get_ok(self, method_frame, header, *content_frames):
    # No need to assert that is instance of Header or Content frames because
    # failure to access as such will result in exception that channel will
    # pick up and handle accordingly.
    header_frame = self.channel.next_frame()
    if header_frame:
      size = header_frame.size
      body = bytearray()
      rbuf_frames = deque([header_frame, method_frame])
      
      while len(body) < size:
        content_frame = self.channel.next_frame()
        if content_frame:
          rbuf_frames.appendleft( content_frame )
          body.extend( content_frame.payload.buffer() )
        else:
          self.channel.requeue_frames( rbuf_frames )
          raise self.FrameUnderflow()
    else:
      self.channel.requeue_frames( [method_frame] )
      raise self.FrameUnderflow()

    delivery_tag = method_frame.args.read_longlong()
    redelivered = method_frame.args.read_bit()
    exchange = method_frame.args.read_shortstr()
    routing_key = method_frame.args.read_shortstr()
    message_count = method_frame.args.read_long()
    
    delivery_info = {
      'channel': self.channel,
      'delivery_tag': delivery_tag,
      'redelivered': redelivered,
      'exchange': exchange,
      'routing_key': routing_key,
      'message_count' : message_count,
    }
    msg = Message( body=body, delivery_info=delivery_info, **header_frame.properties )

    cb = self._get_cb.pop(0)
    if cb: cb( msg )

  def _recv_get_empty(self):
    self._get_cb.pop(0)

  def ack(self, delivery_tag, multiple=False):
    '''
    Acknowledge delivery of a message.  If multiple=True, acknowledge up-to
    and including delivery_tag.
    '''
    args = Writer()
    args.write_longlong(delivery_tag)
    args.write_bit(multiple)

    self.send_frame( MethodFrame(self.channel_id, 60, 80, args) )
    
  def reject(self, delivery_tag, requeue=False):
    '''
    Reject a message.
    '''
    args = Writer()
    args.write_longlong( delivery_tag )
    args.write_bit( requeue )

    self.send_frame( MethodFrame(self.channel_id, 60, 90, args) )

  def recover_async(self, requeue=False):
    '''
    Redeliver all unacknowledged messaages on this channel.
    
    This method is deprecated in favour of the synchronous recover/recover-ok
    '''
    args = Writer()
    args.write_bit( requeue )

    self.send_frame( MethodFrame(self.channel_id, 60, 100, args) )

  def recover(self, requeue=False, cb=None):
    '''
    Ask server to redeliver all unacknowledged messages.
    '''
    args = Writer()
    args.write_bit( requeue )

    # The XML spec is incorrect; this method is synchronous
    #  http://lists.rabbitmq.com/pipermail/rabbitmq-discuss/2011-January/010738.html
    self._recover_cb.append( cb )
    self.send_frame( MethodFrame(self.channel_id, 60, 110, args) )
    self.channel.add_synchronous_cb( self._recv_recover_ok )

  def _recv_recover_ok(self):
    cb = self._recover_cb.pop(0)
    if cb is not None: cb()
