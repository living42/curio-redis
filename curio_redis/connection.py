from curio import socket
import hiredis

from . import exceptions


class Connection:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self._sock = None
        self._reader = hiredis.Reader(protocolError=exceptions.ProtocolError,
                                      replyError=exceptions.ReplyError)

    async def connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        await self._sock.connect((self.host, self.port))
        self._sock.setsockopt(socket.SOL_SOCKET, socket.TCP_NODELAY, 1)

    async def close(self):
        if self._sock is None:
            return

        await self._sock.close()
        self._sock = None

    async def send_command(self, *args):
        if self._sock is None:
            await self.connect()

        return await self._sock.sendall(self.pack_command(*args))

    async def read_response(self):
        if self._sock is None:
            await self.connect()

        while True:
            data = await self._sock.recv(65536)
            self._reader.feed(data)
            response = self._reader.gets()
            if response is not False:
                if isinstance(response, Exception):
                    raise response

                return response

    def pack_command(self, *args):
        if b' ' in args[0]:
            args = (*args[0].split(), *args[1:])

        buf = bytearray(b'*%d\r\n' % len(args))

        for arg in args:
            buf += b'$%d\r\n' % len(arg)
            buf += arg + b'\r\n'

        return buf


class ConnectionPool:
    def __init__(self, host='localhost', port=6379, max_connections=None):
        self.host = host
        self.port = port
        self._active_connections = set()
        self._idle_connections = []
        self._created_connections = 0
        self.max_connections = max_connections or 2 ** 32

    async def make_connection(self):
        if self._created_connections >= self.max_connections:
            raise exceptions.ConnectionError('Too many connections')

        connection = Connection(self.host, self.port)
        self._created_connections += 1
        return connection

    async def get_connection(self):
        try:
            connection = self._idle_connections.pop()
        except IndexError:
            connection = await self.make_connection()

        self._active_connections.add(connection)

        return connection

    async def release(self, connection):
        self._active_connections.remove(connection)
        self._idle_connections.append(connection)

    async def close(self):
        for connection in self._idle_connections:
            await connection.close()
