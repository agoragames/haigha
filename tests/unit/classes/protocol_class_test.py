'''
Copyright (c) 2011-2015, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai

from haigha.classes import protocol_class
from haigha.classes.protocol_class import ProtocolClass


class ProtocolClassTest(Chai):

    def test_dispatch_when_in_dispatch_map(self):
        ch = mock()
        frame = mock()
        frame.method_id = 42

        klass = ProtocolClass(ch)
        klass.dispatch_map = {42: 'method'}

        with expect(ch.clear_synchronous_cb).args('method').returns(mock()) as cb:
            expect(cb).args(frame)

        klass.dispatch(frame)
