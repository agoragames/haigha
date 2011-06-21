'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from struct import unpack, unpack_from, Struct
from datetime import datetime
from decimal import Decimal

class Reader(object):
  """
  A stream-like reader object that supports all the basic data types of AMQP.
  """

  class ReaderError(Exception): '''Base class for all reader errors.'''
  class BufferUnderflow(ReaderError): '''Not enough bytes to satisfy the request.'''
  class FieldError(ReaderError): '''Unsupported field type was read.'''

  def __init__(self, source, start_pos=0, size=None):
    """
    source should be a bytearray, io object with a read() method, another
    Reader, a plain or unicode string. Can be allocated over a slice of source.
    """
    # Note: buffer used here because unpack_from can't accept an array,
    # which I think is related to http://bugs.python.org/issue7827
    if isinstance(source, bytearray):
      self._input = buffer(source)
    elif isinstance(source, Reader):
      self._input = source._input
    elif hasattr(source, 'read'):
      self._input = buffer( source.read() )
    elif isinstance(source, str):
      self._input = buffer(source)
    elif isinstance(source, unicode):
      self._input = buffer(source.encode('utf8'))
    else:
      raise ValueError('Reader needs a bytearray, io object or plain string')

    self._start_pos = self._pos = start_pos
    self._end_pos = len(self._input)
    if size:
      self._end_pos = self._start_pos + size

  def __str__(self):
    return ''.join( ['\\x%s'%(c.encode('hex')) for c in self._input[self._start_pos:self._end_pos]] )

  def tell(self):
    '''
    Current position
    '''
    return self._pos

  def seek(self, offset, whence=0):
    '''
    Simple seek. Follows standard interface.
    '''
    if whence==0:
      self._pos = self._start_pos + offset
    elif whence==1:
      self._pos += offset
    else:
      self._pos = ( self._end_pos-1 ) + offset

  def _check_underflow(self, n):
    '''
    Raise BufferUnderflow if there's not enough bytes to satisfy the request.
    '''
    if self._pos+n > self._end_pos:
      raise self.BufferUnderflow()

  def __len__(self):
    '''
    Supports content framing in Channel
    '''
    return self._end_pos - self._start_pos

  def buffer(self):
    '''
    Get a copy of the buffer that this is reading from. Returns a buffer object
    '''
    return buffer( self._input, self._start_pos, (self._end_pos-self._start_pos) )

  def read(self, n):
    """
    Read n bytes.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    """
    self._check_underflow( n )
    rval = self._input[ self._pos:self._pos+n ]
    self._pos += n
    return rval

  def read_bit(self):
    """
    Read a single boolean value, returns 0 or 1. Convience for single bit fields

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    """
    # Perform a faster check on underflow
    if self._pos >= self._end_pos: raise self.BufferUnderflow()
    result = ord(self._input[ self._pos ]) & 1
    self._pos += 1
    return result

  def read_bits(self, num):
    '''
    Read several bits packed into the same field. Will return as a list. The
    bit field itself is little-endian, though the order of the returned array
    looks big-endian for ease of decomposition.

    Reader('\x02').read_bits(2) -> [False,True]
    Reader('\x08').read_bits(2) -> [False,True,False,False,False,False,False,False]
    first_field, second_field = Reader('\x02').read_bits(2)

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise ValueError if num < 0 or num > 9
    '''
    # Perform a faster check on underflow
    if self._pos >= self._end_pos: raise self.BufferUnderflow()
    if num < 0 or num >= 9: raise ValueError("8 bits per field")
    field = ord(self._input[self._pos])
    result = map(lambda x: field>>x & 1, xrange(num) )
    self._pos += 1
    return result

  def read_octet(self, unpacker=Struct('B').unpack_from, size=Struct('B').size):
    """
    Read one byte, return as an integer

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    # Technically should look at unpacker.size, but skipping that is way
    # faster and this method is the most-called of the readers
    if self._pos >= self._end_pos: raise self.BufferUnderflow()
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval

  def read_short(self, unpacker=Struct('>H').unpack_from, size=Struct('>H').size):
    """
    Read an unsigned 16-bit integer

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    self._check_underflow( size )
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval

  def read_long(self, unpacker=Struct('>I').unpack_from, size=Struct('>I').size):
    """
    Read an unsigned 32-bit integer

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    self._check_underflow( size )
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval

  def read_longlong(self, unpacker=Struct('>Q').unpack_from, size=Struct('>Q').size):
    """
    Read an unsigned 64-bit integer

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    self._check_underflow( size )
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval

  def read_shortstr(self):
    """
    Read a utf-8 encoded string that's stored in up to
    255 bytes.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise UnicodeDecodeError if the text is mal-formed.
    Will raise struct.error if the data is malformed
    """
    slen = self.read_octet()
    return self.read(slen)

  def read_longstr(self):
    """
    Read a string that's up to 2**32 bytes, the encoding
    isn't specified in the AMQP spec, so just return it as
    a plain Python string.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    slen = self.read_long()
    return self.read(slen)
  
  def read_timestamp(self):
    """
    Read and AMQP timestamp, which is a 64-bit integer representing
    seconds since the Unix epoch in 1-second resolution.  Return as
    a Python datetime.datetime object, expressed as localtime.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    return datetime.fromtimestamp( self.read_longlong() )

  def read_table(self):
    """
    Read an AMQP table, and return as a Python dictionary.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise UnicodeDecodeError if the text is mal-formed.
    Will raise struct.error if the data is malformed
    """
    # Only need to check underflow on the table once
    tlen = self.read_long()
    self._check_underflow(tlen)
    end_pos = self._pos + tlen
    result = {}
    while self._pos < end_pos:
      name = self._field_shortstr()
      result[name] = self._read_field()
    return result

  def _read_field(self):
    '''
    Read a single byte for field type, then read the value.
    '''
    ftype = self._input[ self._pos ]
    self._pos += 1
    
    reader = self.field_type_map.get( ftype )
    if reader:
      return reader(self)

    raise Reader.FieldError('Unknown field type %s', ftype)
    
  def _field_bool(self):
    result = ord(self._input[ self._pos ]) & 1
    self._pos += 1
    return result

  def _field_short_short_int(self, unpacker=Struct('b').unpack_from, size=Struct('b').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval
    
  def _field_short_short_uint(self, unpacker=Struct('B').unpack_from, size=Struct('B').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval

  def _field_short_int(self, unpacker=Struct('>h').unpack_from, size=Struct('>h').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval
    
  def _field_short_uint(self, unpacker=Struct('>H').unpack_from, size=Struct('>H').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval

  def _field_long_int(self, unpacker=Struct('>i').unpack_from, size=Struct('>i').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval
    
  def _field_long_uint(self, unpacker=Struct('>I').unpack_from, size=Struct('>I').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval

  def _field_long_long_int(self, unpacker=Struct('>q').unpack_from, size=Struct('>q').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval
    
  def _field_long_long_uint(self, unpacker=Struct('>Q').unpack_from, size=Struct('>Q').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval

  def _field_float(self, unpacker=Struct('>f').unpack_from, size=Struct('>f').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval
    
  def _field_double(self, unpacker=Struct('>d').unpack_from, size=Struct('>d').size):
    rval = unpacker( self._input, self._pos )[0]
    self._pos += size
    return rval

  # Coding to http://dev.rabbitmq.com/wiki/Amqp091Errata#section_3 which
  # differs from spec in that the value is signed.
  def _field_decimal(self):
    d = self._field_short_short_uint()
    n = self._field_long_int()
    return Decimal(n) / Decimal(10 ** d)

  def _field_shortstr(self):
    slen = self._field_short_short_uint()
    rval = self._input[ self._pos:self._pos+slen ]
    self._pos += slen
    return rval
  
  def _field_longstr(self):
    slen = self._field_long_uint()
    rval = self._input[ self._pos:self._pos+slen ]
    self._pos += slen
    return rval

  def _field_array(self):
    alen = self.read_long()
    end_pos = self._pos + alen
    rval = []
    while self._pos < end_pos:
      rval.append( self._read_field() )
    return rval
     
  def _field_timestamp(self):
    """
    Read and AMQP timestamp, which is a 64-bit integer representing
    seconds since the Unix epoch in 1-second resolution.  Return as
    a Python datetime.datetime object, expressed as localtime.

    Will raise BufferUnderflow if there's not enough bytes in the buffer.
    Will raise struct.error if the data is malformed
    """
    return datetime.fromtimestamp( self._field_long_long_uint() )

  def _field_bytearray(self):
    slen = self._field_long_uint()
    rval = bytearray( self._input[ self._pos:self._pos+slen ] )
    self._pos += slen
    return rval

  def _field_none(self):
    return None

  # A mapping for quick lookups
  # Rabbit and Qpid 0.9.1 mapping
  field_type_map = {
    't' : _field_bool,
    'b' : _field_short_short_int,
    's' : _field_short_int,
    'I' : _field_long_int,
    'l' : _field_long_long_int,
    'f' : _field_float,
    'd' : _field_double,
    'D' : _field_decimal,
    'S' : _field_longstr,
    'T' : _field_timestamp,
    'F' : read_table,
    'V' : _field_none,
    'x' : _field_bytearray,
  }

  # 0.9.1 spec mapping
  #field_type_map = {
  #  't' : _field_bool,
  #  'b' : _field_short_short_int,
  #  'B' : _field_short_short_uint,
  #  'U' : _field_short_int,
  #  'u' : _field_short_uint,
  #  'I' : _field_long_int,
  #  'i' : _field_long_uint,
  #  'L' : _field_long_long_int,
  #  'l' : _field_long_long_uint,
  #  'f' : _field_float,
  #  'd' : _field_double,
  #  'D' : _field_decimal,
  #  's' : _field_shortstr,
  #  'S' : _field_longstr,
  #  'A' : _field_array,
  #  'T' : _field_timestamp,
  #  'F' : read_table,
  #  'V' : _field_none,
  #}
