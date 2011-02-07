
#from cStringIO import StringIO

class Message(object):
  '''
  Represents an AMQP message.
  '''

  def __init__(self, body=None, delivery_info=None, **properties):
    if isinstance(body, unicode):
      if properties.get('content_encoding', None) is None:
        properties['content_encoding'] = 'utf-8'
      body = body.encode(properties['content_encoding'])
    
    self._body = body
    self._delivery_info = delivery_info
    self._properties = properties
 
  @property
  def body(self):
    return self._body

  def __len__(self):
    return len( self._body )

  def __nonzero__(self):
    '''Have to define this because length is defined.'''
    return True

  def __eq__(self, rhs):
    if isinstance(rhs,Message):
      return self.properties == rhs.properties and \
             self.body_text == rhs.body_text
    return False

  @property
  def delivery_info(self):
    return self._delivery_info

  @property
  def properties(self):
    return self._properties

  def __str__(self):
    return "Message[body: %s, delivery_info: %s, properties: %s]"%( self._body.getvalue(), self._delivery_info, self._properties )
