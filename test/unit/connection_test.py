from test_case import TestCase, unittest

class ConnectionTestCase(TestCase):
  def test_pass(self):
    self.assertTrue(True)
  
  def test_fail(self):
    self.assertTrue(False)
  
  @unittest.skip("This is broken")
  def test_skip(self):
    self.assertTrue(False)

