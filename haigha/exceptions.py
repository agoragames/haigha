'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

class ConnectionError(Exception): '''Base class for all connection errors.'''
class ConnectionClosed(ConnectionError): '''The connection is closed.  Fatal.'''

class ChannelError(Exception): '''Base class for all channel errors.'''
class ChannelClosed(ChannelError): '''The channel is closed. Fatal.'''
