'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai

from haigha.frames import content_frame, ContentFrame, Frame

class ContentFrameTest(Chai):

  def test_type(self):
    assert_equals(3, ContentFrame.type() )

  def test_payload(self):
    klass = ContentFrame(42, 'payload')
    assert_equals( 'payload', klass.payload )

  def test_parse(self):
    frame = ContentFrame.parse(42, 'payload')
    assert_true( isinstance(frame, ContentFrame) )
    assert_equals( 42, frame.channel_id )
    assert_equals( 'payload', frame.payload )

  def test_create_frames(self):
    itr = ContentFrame.create_frames(42, 'helloworld', 13)
    
    frame = itr.next()
    assert_true( isinstance(frame, ContentFrame) )
    assert_equals( 42, frame.channel_id )
    assert_equals( 'hello', frame.payload )
    
    frame = itr.next()
    assert_true( isinstance(frame, ContentFrame) )
    assert_equals( 42, frame.channel_id )
    assert_equals( 'world', frame.payload )

    assert_raises(StopIteration, itr.next)

  def test_init(self):
    expect(Frame.__init__).args( is_a(ContentFrame), 42 )
    frame = ContentFrame(42, 'payload')
    assert_equals( 'payload', frame._payload )

  def test_str(self):
    # Test both branches but don't assert the actual content because its
    # not worth it
    frame = ContentFrame(42, 'payload')
    str(frame)

    frame = ContentFrame(42, 8675309)
    str(frame)

  def test_write_frame(self):
    w = mock()
    expect( mock(content_frame, 'Writer') ).args('buffer').returns( w )
    expect( w.write_octet ).args( 3 ).returns( w )
    expect( w.write_short ).args( 42 ).returns( w )
    expect( w.write_long ).args( 5 ).returns( w )
    expect( w.write ).args( 'hello' ).returns( w )
    expect( w.write_octet ).args( 0xce )

    frame = ContentFrame(42, 'hello')
    frame.write_frame( 'buffer' )
