import socket

from test_case import TestCase
from lib.connection_strategy import ConnectionStrategy, Host

class ConnectionStrategyTestCase(TestCase):

  def test_connection_strategy_init(self):
    c = ConnectionStrategy(None, "localhost:5672")
  
  def test_host(self):  
    h = Host("localhost:5672")
    self.assertEqual(h.host, socket.gethostname())
    self.assertEqual(h.port, 5672)
    
    
