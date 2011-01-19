
from cStringIO import StringIO

class Message(object):
  '''
  Represents an AMQP message.
  '''

  def __init__(self, body=StringIO(''), delivery_info=None, **properties):
    if isinstance(body, unicode):
      if properties.get('content_encoding', None) is None:
        properties['content_encoding'] = 'utf-8'
      body = body.encode(properties['content_encoding'])
    if not hasattr(body,'read'):
      if isinstance(body, str):
        self._body = StringIO( body )
      else:
        raise TypeError("Invalid message body type %s", type(body))
    else:
      self._body = body
    self._delivery_info = delivery_info
    self._properties = properties
 
  @property
  def body(self):
    return self._body

  @property
  def body_text(self):
    return self._body.getvalue()

  @property
  def delivery_info(self):
    return self._delivery_info

  @property
  def properties(self):
    return self._properties

  def __str__(self):
    return "Message[body: %s, delivery_info: %s, properties: %s]"%( self._body.getvalue(), self._delivery_info, self._properties )
