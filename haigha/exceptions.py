'''
Public exceptions that can be used by clients
'''

class ConnectionError(Exception): '''Base class for all connection errors.'''
class ConnectionClosed(ConnectionError): '''The connection is closed.  Fatal.'''

class ChannelError(Exception): '''Base class for all channel errors.'''
class ChannelClosed(ChannelError): '''The channel is closed. Fatal.'''
