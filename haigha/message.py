'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

class Message(object):
  '''
  Represents an AMQP message.
  '''

  def __init__(self, body=None, delivery_info=None, **properties):
    if isinstance(body, unicode):
      if 'content_encoding' not in properties:
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

  def __eq__(self, other):
    if isinstance(other,Message):
      return self._properties == other._properties and \
             self._body == other._body
    return False

  @property
  def delivery_info(self):
    return self._delivery_info

  @property
  def properties(self):
    return self._properties

  def __str__(self):
    return "Message[body: %s, delivery_info: %s, properties: %s]"%\
      ( str(self._body).encode('string_escape'), self._delivery_info, self._properties )
