'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
import gevent
from gevent.coros import Semaphore
from gevent import socket
from gevent.pool import Pool

from haigha.transports import Transport

class TransportTest(Chai):

  def test_init_and_connection_property(self):
    t = Transport( 'conn' )
    assert_equals( 'conn', t._connection )
    assert_equals( 'conn', t.connection )

  def test_process_channels(self):
    t = Transport('conn')
    ch1 = mock()
    ch2 = mock()
    chs = set([ch1,ch2])
    expect( ch1.process_frames )
    expect( ch2.process_frames )

    t.process_channels( chs )
