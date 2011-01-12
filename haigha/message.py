
class Message(object):
  '''
  Represents an AMQP message.
  '''

  def __init__(self, body=None, delivery_info=None, **properties):
    if isinstance(body, unicode):
      if properties.get('content_encoding', None) is None:
        properties['content_encoding'] = 'UTF-8'
      self._body = body.encode(properties['content_encoding'])
    else:
      self._body = body
    self._delivery_info = delivery_info
    self._properties = properties
 
  @property
  def body(self):
    return self._body

  @property
  def delivery_info(self):
    return self._delivery_info

  @property
  def properties(self):
    return self._properties

  def __str__(self):
    if isinstance( self._body, (str,unicode) ):
      return "Message[body: %s, delivery_info: %s, properties: %s]"%( self._body, self._delivery_info, self._properties )
    return "Message[body: %s, delivery_info: %s, properties: %s]"%( self._body.getvalue(), self._delivery_info, self._properties )
