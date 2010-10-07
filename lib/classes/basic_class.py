from haigha.lib.message import Message
from haigha.lib.writer import Writer
from haigha.lib.frames import MethodFrame, HeaderFrame, ContentFrame
from haigha.lib.classes import ProtocolClass

from cStringIO import StringIO

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
      71 : self._recv_get_ok,
      72 : self._recv_get_empty,
      111 : self._recv_recover_ok,
    }

    self._consumer_tag_id = 0
    self._pending_consumers = []
    self._consumer_cb = {}

  def _generate_consumer_tag(self):
    '''
    Generate the next consumer tag.

    The consumer tag is local to a channel, so two clients can use the
    same consumer tags.
    '''
    self._consumer_tag_id += 1
    return "channel-%d-%d"%( self.channel_id, self._consumer_tag_id )

  # TODO: Add a concept of number of pending transactions when we re-implement
  # public_synchronous.  May be something that goes into the Channel object,
  # or that it will walk all the pending frames in the Channel sync buffer and
  # determine how many transaction commits there are

  # TODO: Also include an optional callback method when a transaction is committed.
  
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

  def _recv_qos_ok(self):
    # No arguments, nothing to do
    pass
    
  def consume(self, queue, consumer, consumer_tag='', no_local=False,
        no_ack=False, exclusive=False, nowait=True, ticket=None):
    '''
    start a queue consumer.
    '''
    # 22 Apr 09 aaron - I discovered a bug/feature/behavior of amqp/rabbit
    # where we can't have two pending consume calls at a time.  The response
    # message will only be generated once, but then the delivery method will
    # be called with two different consumer tags for the same callback.  So
    # if this is being called for msgs A and B, we'll only ever hear back
    # regarding A, but we'll recieve those msgs with two consumer tags, the
    # one for A, and one which was never bound to anything but is probably the
    # one which should correspond to B.
    # TODO: document this to Rabbit mailing list.
    if nowait and consumer_tag=='':
      consumer_tag = self._generate_consumer_tag()

    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(queue)
    args.write_shortstr(consumer_tag)
    args.write_bit(no_local)
    args.write_bit(no_ack)
    args.write_bit(exclusive)
    args.write_bit(nowait)
    self.send_frame( MethodFrame(self.channel_id, 60, 20, args) )

    if not nowait:
      self._pending_consumers.append( consumer )
      self.channel.add_synchronous_cb( self._recv_consume_ok )
    else:
      self._consumer_cb[ consumer_tag ] = consumer

  def _recv_consume_ok(self, method_frame):
    consumer_tag = method_frame.args.read_shortstr()
    self._consumer_cb[ consumer_tag ] = self._pending_consumers.pop(0)

  def cancel(self, consumer_tag='', nowait=False, consumer=None):
    '''
    Cancel a consumer. Can choose to delete based on a consumer tag or the
    function which is consuming.  If deleting by function, take care to only
    use a consumer once per channel.
    '''
    if consumer:
      for (tag,cb) in self._consumer_cb.iteritems():
        if cb==consumer:
          consumer_tag = tag
          break

    args = Writer()
    args.write_shortstr(consumer_tag)
    args.write_bit(nowait)
    self.send_frame( MethodFrame(self.channel_id, 60, 30, args) )

    if not nowait:
      self.channel.add_synchronous_cb( self._recv_cancel_ok )
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
    
  def publish(self, msg, exchange='', routing_key='', mandatory=False, immediate=False, ticket=None):
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
    args.write_bit(mandatory)
    args.write_bit(immediate)

    self.send_frame( MethodFrame(self.channel_id, 60, 40, args) )
    self.send_frame( HeaderFrame(self.channel_id, 60, 0, len(msg.body), msg.properties) )

    # TODO: Make this more performant by not creating and deleting objects.
    # TODO: Access frame size in Connection
    # TODO: Think of how to incorporate this into ContentFrame, since it's
    # the one that knows that 8 is the size of its header bytes
    idx = 0
    frame_max = 1024
    while idx < len(msg.body):
      #payload, body = body[:self.frame_max - 8], body[self.frame_max -8:]

      start = idx
      end = start + frame_max - 8
      self.send_frame( ContentFrame(self.channel_id, msg.body[start:end]) )
      idx = end

  def _recv_return(self):
    pass

  def _recv_deliver(self, method_frame, *content_frames):
    consumer_tag = method_frame.args.read_shortstr()
    delivery_tag = method_frame.args.read_longlong()
    redelivered = method_frame.args.read_bit()
    exchange = method_frame.args.read_shortstr()
    routing_key = method_frame.args.read_shortstr()

    buffer = StringIO()
    for frame in content_frames:
      buffer.write( frame.payload )

    delivery_info = {
      'channel': self,
      'consumer_tag': consumer_tag,
      'delivery_tag': delivery_tag,
      'redelivered': redelivered,
      'exchange': exchange,
      'routing_key': routing_key,
    }
    msg = Message( body=buffer, delivery_info=delivery_info )

    func = self._consumer_cb.get(consumer_tag, None)
    if func is not None:
      func(msg)

  def get(self):
    pass

  def _recv_get_ok(self):
    pass

  def _recv_get_empty(self):
    pass

  def ack(self):
    pass
    
  def reject(self):
    pass

  def recover_async(self):
    pass

  def recover(self):
    pass

  def _recv_recover_ok(self):
    pass
