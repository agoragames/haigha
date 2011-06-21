'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

import event
import socket

AMQP_PORT = 5672

# enumeration of stats
UNCONNECTED, CONNECTED, FAILED = 0,1,2

class ConnectionStrategy(object):
  """
  Implements the algorithms for connecting to an AMQP server.  The strategy is
  designed to handle failover in a clustered environment.
  """

  def __init__(self, connection, addr, reconnect_cb = None):
    '''Initialize the strategy to a host.  No matter what additional
    hosts are learned about, the original will be maintained as the fallback.
    This is most useful when all hosts in a cluster are DNS aliased.'''
    self._connection = connection
    self._orig_host = Host(addr)
    self._known_hosts = [ self._orig_host ]
    self._cur_host = self._known_hosts[0]
    self._pending_connect = None
    
    self._reconnecting = False
    self.reconnect_callbacks = []
    if reconnect_cb:
      self.reconnect_callbacks.append(reconnect_cb)

  def set_known_hosts(self, hosts):
    '''Set the list of known hosts.  Can be an empty string or a list of comma
    separated values.'''
    self._known_hosts = [ self._orig_host ]
    hosts = hosts.split(',')
    for host in hosts:
      h = Host(host)

      if h not in self._known_hosts:
        self._known_hosts.append( h )
   
    # This is a protection measure to ensure the integrity of the configuration
    # and the next_host logic.  On reconnect, wait a sec to prevent a really odd
    # config from breaking it.
    if not self._cur_host in self._known_hosts:
      delay = 5
      self._connection.logger.warning( 
        "current host %s not in known hosts %s, reconnecting to %s in %ds!", 
        self._cur_host, self._known_hosts, self._orig_host, delay )
      self._known_hosts = [ self._orig_host ]
      self._cur_host = self._orig_host
      self.connect( delay )

  def next_host(self):
    '''Iterate to the next host.'''
    try:
      old_idx = self._known_hosts.index( self._cur_host )
      host_idx = old_idx + 1
    except ValueError:
      old_idx = 0
      host_idx = 0

    # Give preference to hosts we haven't connected to yet.
    next = None
    while host_idx < len(self._known_hosts):
      host = self._known_hosts[ host_idx ]
      if host.state == UNCONNECTED:
        next = host
        break
      host_idx += 1

    # If there's no host at the end of our list then walk back through and
    # see if there's one which has not failed that we can pick from.  Don't
    # try to filter out if next==_cur_host because it might still be the case
    # that we have to reestablish the socket.
    if not next:
      host_idx = 0
      while host_idx <= old_idx:
        host = self._known_hosts[ host_idx ]
        if host.state != FAILED:
          next = host
          break
        host_idx += 1

    if next:
      self._cur_host = next
      self.connect()
    else:
      delay = 5
      self._cur_host = self._orig_host
      self._connection.logger.warning(
        "Failed to connect to any of %s, will retry %s in %d seconds",
        self._known_hosts, self._cur_host, delay )
      self._reconnecting = True
      self.connect( delay )

  def fail(self):
    '''
    Mark the current strategy as a failure.
    '''
    self._cur_host.state = FAILED

  def connect(self, delay=0):
    '''Connect.'''
    # Ensure that the connection has cleaned up old resources.  Do it immediately
    # to be sure that output is buffered and no errors are raised.
    try:
      self._connection.logger.debug("disconnecting connection")
      self._connection.disconnect()
    except:
      self._connection.logger.exception( "error while disconnecting" )

    self._connection.logger.debug("Pending connect: %s", self._pending_connect)
    if not self._pending_connect:
      self._connection.logger.debug("Scheduling a connection in %s", delay)
      self._pending_connect = event.timeout(delay, self._connect_cb)

  def _connect_cb(self):
    '''Async connect.'''
    self._pending_connect = None
    try:
      self._connection.logger.debug("Connecting to %s on %s", self._cur_host.host, self._cur_host.port)
      self._connection.connect( self._cur_host.host, self._cur_host.port )
      self._cur_host.state = CONNECTED
      if self._reconnecting:
        self._connection.logger.info( "Connected to %s", self._cur_host )
      else:
        self._connection.logger.debug( "Connected to %s", self._cur_host )
      
      if self._reconnecting:
        for callback in self.reconnect_callbacks:
          callback()
        self._reconnecting = False
    except:
      if self._cur_host.state == FAILED:
        self._connection.logger.critical(
          "Failed to connect to %s", self._cur_host )
        self.next_host()
      else:
        delay = 2
        self._connection.logger.exception(
          "Failed to connect to %s, will try again in %d seconds", self._cur_host, delay )
        self._cur_host.state = FAILED
        self.connect( delay )

class Host(object):
  def __init__(self, addr):
    self.state = UNCONNECTED

    addr = str(addr)  # strip unicode
    if ':' in addr:
      self.host,self.port = addr.split(':')
      self.port = int(self.port)
    else:
      self.host = addr
      self.port = AMQP_PORT

    # translate localhost because when amqp replies it will use fully qualified
    # hostnames, and we need to know when we have a duplicate.
    if self.host=='localhost' or self.host=='127.0.0.1':
      self.host = socket.gethostname()

  # Not strictly a __repr__ but we want this to look good when printing lists
  # of hosts.
  def __str__(self):
    return "%s:%s"%( self.host, self.port )
  __repr__ = __str__
  
  def __eq__(self, other):
    if isinstance(other,Host):
      return (self.host,self.port)==(other.host,other.port)
    return False

  def __hash__(self):
    return hash( (self.host,self.port) )
