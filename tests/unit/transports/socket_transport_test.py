'''
Copyright (c) 2011-2017, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
import errno
import socket

from haigha.transports import socket_transport
from haigha.transports.socket_transport import *


class SocketTransportTest(Chai):

    def setUp(self):
        super(SocketTransportTest, self).setUp()

        self.connection = mock()
        self.transport = SocketTransport(self.connection)
        self.transport._host = 'server:1234'

    def test_init(self):
        assert_equals(bytearray(), self.transport._buffer)
        assert_true(self.transport._synchronous)

    def _set_up_connect_test(self, sock):
        """Set up common options and expects for connect() related tests."""
        self.connection._connect_timeout = 4.12
        self.connection._sock_opts = {
            ('family', 'tcp'): 34,
            ('range', 'ipv6'): 'hex'
        }
        expect(sock.settimeout).args(4.12).at_least_once()
        expect(sock.setsockopt).any_order().args(
            'family', 'tcp', 34).any_order().at_least_once()
        expect(sock.setsockopt).any_order().args(
            'range', 'ipv6', 'hex').any_order().at_least_once()
        expect(sock.settimeout).args(None).at_least_once()

    def _set_up_connect_test_fail(self, sock):
        """Set up common options and expects for connect() tests that fail."""
        self.connection._connect_timeout = 4.12
        self.connection._sock_opts = {
            ('family', 'tcp'): 34,
            ('range', 'ipv6'): 'hex'
        }
        expect(sock.settimeout).args(4.12).at_least_once()
        expect(sock.setsockopt).any_order().args(
            'family', 'tcp', 34).any_order().at_least_once()
        expect(sock.setsockopt).any_order().args(
            'range', 'ipv6', 'hex').any_order().at_least_once()

    def test_connect_with_no_klass_arg_ipv4(self):
        sock = mock()
        expect(socket.socket).returns(sock)
        self._set_up_connect_test(sock)
        expect(socket, 'getaddrinfo').args(
            'host', 5309, 0, 0, socket.IPPROTO_TCP,
        ).returns(
            [(socket.AF_INET, socket.SOCK_STREAM, 0, 'canon', ('host.net', 5309))]
        )
        expect(sock.connect).args(('host.net', 5309))
        self.transport.connect(('host', 5309))

    def test_connect_with_no_klass_arg_ipv6(self):
        sock = mock()
        expect(socket.socket).returns(sock)
        self._set_up_connect_test(sock)
        expect(socket, 'getaddrinfo').args(
            'host', 5309, 0, 0, socket.IPPROTO_TCP,
        ).returns(
            [
                (
                    socket.AF_INET6, socket.SOCK_STREAM, 0, 'canon',
                    ('::1', 5309, 0, 0),
                ),
            ]
        )
        expect(sock.connect).args(('::1', 5309, 0, 0))
        self.transport.connect(('host', 5309))

    def test_connect_with_no_klass_arg_first_fail(self):
        """Test that failed connections are bypassed until a success."""
        sock = mock()
        expect(socket.socket).returns(sock)
        self._set_up_connect_test(sock)
        expect(socket, 'getaddrinfo').args(
            'host', 5309, 0, 0, socket.IPPROTO_TCP,
        ).returns(
            [
                (
                    socket.AF_INET, socket.SOCK_STREAM, 0, 'canon',
                    ('host.net', 5309),
                ),
                (
                    socket.AF_INET6, socket.SOCK_STREAM, 0, 'canon',
                    ('::1', 5309, 0, 0),
                ),
            ]
        )
        expect(sock.connect).args(('host.net', 5309)).raises(socket.error)
        expect(self.transport.connection.logger.exception).any_args()
        expect(sock.connect).args(('::1', 5309, 0, 0))
        self.transport.connect(('host', 5309))

    def test_connect_with_no_klass_arg_all_fail(self):
        """Test that exception is raised when all connections fail."""
        sock = mock()
        expect(socket.socket).returns(sock)
        self._set_up_connect_test_fail(sock)
        expect(socket, 'getaddrinfo').args(
            'host', 5309, 0, 0, socket.IPPROTO_TCP,
        ).returns(
            [
                (
                    socket.AF_INET, socket.SOCK_STREAM, 0, 'canon',
                    ('host.net', 5309),
                ),
                (
                    socket.AF_INET6, socket.SOCK_STREAM, 0, 'canon',
                    ('::1', 5309, 0, 0),
                ),
            ]
        )
        expect(sock.connect).args(('host.net', 5309)).raises(socket.error)
        expect(sock.connect).args(('::1', 5309, 0, 0)).raises(socket.error)
        expect(
            self.transport.connection.logger.exception,
        ).any_args().at_least_once()
        self.assert_raises(
            socket.error, self.transport.connect, ('host', 5309),
        )

    def test_connect_with_klass_arg_ipv4(self):
        klass = mock()
        sock = mock()
        expect(klass).returns(sock)
        self._set_up_connect_test(sock)
        expect(socket, 'getaddrinfo').args(
            'host', 5309, 0, 0, socket.IPPROTO_TCP,
        ).returns(
            [(socket.AF_INET, socket.SOCK_STREAM, 0, 'canon', ('host.net', 5309))]
        )
        expect(sock.connect).args(('host.net', 5309))
        self.transport.connect(('host', 5309), klass=klass)

    def test_connect_with_klass_arg_ipv6(self):
        klass = mock()
        sock = mock()
        expect(klass).returns(sock)
        self._set_up_connect_test(sock)
        expect(socket, 'getaddrinfo').args(
            'host', 5309, 0, 0, socket.IPPROTO_TCP,
        ).returns(
            [
                (
                    socket.AF_INET6, socket.SOCK_STREAM, 0, 'canon',
                    ('::1', 5309, 0, 0),
                ),
            ]
        )
        expect(sock.connect).args(('::1', 5309, 0, 0))
        self.transport.connect(('host', 5309), klass=klass)

    def test_connect_with_klass_arg_first_fail(self):
        klass = mock()
        sock = mock()
        expect(klass).returns(sock)
        self._set_up_connect_test(sock)
        expect(socket, 'getaddrinfo').args(
            'host', 5309, 0, 0, socket.IPPROTO_TCP,
        ).returns(
            [
                (
                    socket.AF_INET, socket.SOCK_STREAM, 0, 'canon',
                    ('host.net', 5309),
                ),
                (
                    socket.AF_INET6, socket.SOCK_STREAM, 0, 'canon',
                    ('::1', 5309, 0, 0),
                ),
            ]
        )
        expect(sock.connect).args(('host.net', 5309)).raises(socket.error)
        expect(self.transport.connection.logger.exception).any_args()
        expect(sock.connect).args(('::1', 5309, 0, 0))
        self.transport.connect(('host', 5309), klass=klass)

    def test_connect_with_klass_arg_all_fail(self):
        klass = mock()
        sock = mock()
        expect(klass).returns(sock)
        self._set_up_connect_test_fail(sock)
        expect(socket, 'getaddrinfo').args(
            'host', 5309, 0, 0, socket.IPPROTO_TCP,
        ).returns(
            [
                (
                    socket.AF_INET, socket.SOCK_STREAM, 0, 'canon',
                    ('host.net', 5309),
                ),
                (
                    socket.AF_INET6, socket.SOCK_STREAM, 0, 'canon',
                    ('::1', 5309, 0, 0),
                ),
            ]
        )
        expect(sock.connect).args(('host.net', 5309)).raises(socket.error)
        expect(sock.connect).args(('::1', 5309, 0, 0)).raises(socket.error)
        expect(
            self.transport.connection.logger.exception,
        ).any_args().at_least_once()
        self.assert_raises(
            socket.error, self.transport.connect, ('host', 5309), klass=klass,
        )

    def test_read(self):
        self.transport._sock = mock()
        self.transport.connection.debug = False

        expect(self.transport._sock.settimeout).args(None)
        expect(self.transport._sock.getsockopt).args(
            socket.SOL_SOCKET, socket.SO_RCVBUF).returns(4095)
        expect(self.transport._sock.recv).args(4095).returns('buffereddata')

        assert_equals('buffereddata', self.transport.read())

    def test_read_when_data_buffered(self):
        self.transport._sock = mock()
        self.transport.connection.debug = False
        self.transport._buffer = bytearray('buffered')

        expect(self.transport._sock.settimeout).args(3)
        expect(self.transport._sock.getsockopt).any_args().returns(4095)
        expect(self.transport._sock.recv).args(4095).returns('data')

        assert_equals('buffereddata', self.transport.read(3))
        assert_equals(bytearray(), self.transport._buffer)

    def test_read_when_debugging(self):
        self.transport._sock = mock()
        self.transport.connection.debug = 2

        expect(self.transport._sock.settimeout).args(None)
        expect(self.transport._sock.getsockopt).any_args().returns(4095)
        expect(self.transport._sock.recv).args(4095).returns('buffereddata')
        expect(self.transport.connection.logger.debug).args(
            'read 12 bytes from server:1234')

        assert_equals('buffereddata', self.transport.read(0))

    def test_read_when_socket_closes(self):
        self.transport._sock = mock()
        self.transport.connection.debug = 2

        expect(self.transport._sock.settimeout).args(None)
        expect(self.transport._sock.getsockopt).any_args().returns(4095)
        expect(self.transport._sock.recv).args(4095).returns('')
        expect(self.transport.connection.transport_closed).args(
            msg='error reading from server:1234')

        self.transport.read()

    def test_read_when_socket_timeout(self):
        self.transport._sock = mock()
        self.transport.connection.debug = 2

        expect(self.transport._sock.settimeout).args(42)
        expect(self.transport._sock.getsockopt).any_args().returns(4095)
        expect(self.transport._sock.recv).args(4095).raises(
            socket.timeout('not now'))

        assert_equals(None, self.transport.read(42))

    def test_read_when_raises_eagain(self):
        self.transport._sock = mock()
        self.transport.connection.debug = 2

        expect(self.transport._sock.settimeout).args(42)
        expect(self.transport._sock.getsockopt).any_args().returns(4095)
        expect(self.transport._sock.recv).args(4095).raises(
            EnvironmentError(errno.EAGAIN, 'tryagainlater'))

        assert_equals(None, self.transport.read(42))

    def test_read_when_raises_socket_timeout(self):
        self.transport._sock = mock()
        self.transport.connection.debug = 2

        expect(self.transport._sock.settimeout).args(42)
        expect(self.transport._sock.getsockopt).any_args().returns(4095)
        expect(self.transport._sock.recv).args(4095).raises(
            socket.timeout())

        assert_equals(None, self.transport.read(42))

    def test_read_when_raises_other_errno(self):
        self.transport._sock = mock()
        self.transport.connection.debug = 2

        expect(self.transport._sock.settimeout).args(42)
        expect(self.transport._sock.getsockopt).any_args().returns(4095)
        expect(self.transport._sock.recv).args(4095).raises(
            EnvironmentError(errno.EBADF, 'baddog'))
        expect(self.transport.connection.logger.exception).args(
            'error reading from server:1234')
        expect(self.transport.connection.transport_closed).args(
            msg='error reading from server:1234')

        with assert_raises(EnvironmentError):
            self.transport.read(42)

    def test_read_when_no_sock(self):
        self.transport.read()

    def test_buffer(self):
        self.transport._sock = mock()

        self.transport.buffer(bytearray('somedata'))
        assert_equals(bytearray('somedata'), self.transport._buffer)

    def test_buffer_when_already_buffered(self):
        self.transport._sock = mock()
        self.transport._buffer = bytearray('some')

        self.transport.buffer(bytearray('data'))
        assert_equals(bytearray('somedata'), self.transport._buffer)

    def test_buffer_when_no_sock(self):
        self.transport.buffer('somedata')

    def test_write(self):
        self.transport._sock = mock()
        self.transport.connection.debug = False

        expect(self.transport._sock.sendall).args('somedata')
        self.transport.write('somedata')

    def test_write_when_sendall_fails(self):
        self.transport._sock = mock()
        self.transport.connection.debug = False

        expect(self.transport._sock.sendall).args(
            'somedata').raises(Exception('fail'))
        assert_raises(Exception, self.transport.write, 'somedata')

    def test_write_when_sendall_raises_environmenterror(self):
        self.transport._sock = mock()
        self.transport.connection.debug = False

        expect(self.transport._sock.sendall).args('somedata').raises(
            EnvironmentError(errno.EAGAIN, 'tryagainlater'))
        expect(self.transport.connection.logger.exception).args(
            'error writing to server:1234')
        expect(self.transport.connection.transport_closed).args(
            msg='error writing to server:1234')
        self.transport.write('somedata')

    def test_write_when_debugging(self):
        self.transport._sock = mock()
        self.transport.connection.debug = 2

        expect(self.transport._sock.sendall).args('somedata')
        expect(self.transport.connection.logger.debug).args(
            'sent 8 bytes to server:1234')

        self.transport.write('somedata')

    def test_write_when_no_sock(self):
        self.transport.write('somedata')

    def test_disconnect(self):
        self.transport._sock = mock()
        expect(self.transport._sock.close)
        self.transport.disconnect()
        assert_equals(None, self.transport._sock)

    def test_disconnect_when_no_sock(self):
        self.transport.disconnect()
