from haigha.lib.frames import MethodFrame
from haigha.lib.classes import ProtocolClass

class TransactionClass(ProtocolClass):
  '''
  Implements the AMQP Transaction class
  '''

  def __init__(self, *args, **kwargs):
    super(TransactionClass, self).__init__(*args, **kwargs)
    self.dispatch_map = {
      11 : self._recv_select_ok,
      21 : self._recv_commit_ok,
      31 : self._recv_rollback_ok,
    }

    self._enabled = False
    self._commit_cb = []

  @property
  def enabled(self):
    '''Get whether transactions have been enabled.'''
    return self._enabled
  
  def select(self):
    '''
    Set this channel to use transactions.
    '''
    self._enabled = True
    self.send_frame( MethodFrame(self.channel_id, 90, 10) )
    self.channel.add_synchronous_cb( self._recv_select_ok )

  def _recv_select_ok(self, method_frame):
    # nothing to do
    pass
    
  def commit(self, cb=None):
    '''
    Commit the current transaction.  Caller can specify a callback to use
    when the transaction is committed.
    '''
    self._commit_cb.append( cb )
    self.send_frame( MethodFrame(self.channel_id, 90, 20) )
    self.channel.add_synchronous_cb( self._recv_commit_ok )

  def _recv_commit_ok(self, method_frame):
    cb = self._commit_cb.pop(0)
    if cb is not None: cb()

  def rollback(self):
    '''
    Abandon all message publications and acks in the current transaction.
    '''
    self.send_frame( MethodFrame(self.channel_id, 90, 30) )
    self.channel.add_synchronous_cb( self._recv_rollback_ok )

  def _recv_rollback_ok(self, method_frame):
    # nothing to do
    pass
