from curio import socket
from curio.meta import awaitable
import hiredis

from . import errors


class Connection:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self._sock = None
        self._reader = hiredis.Reader(protocolError=errors.ProtocolError,
                                      replyError=errors.ReplyError)

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
                return response

    def pack_command(self, *args):
        buf = bytearray()

        def append(data):
            buf.extend(data + b'\r\n')

        def bytelen(data):
            return str(len(data)).encode()

        append(b'*' + bytelen(args))
        for arg in args:
            assert isinstance(arg, bytes)
            append(b'$' + bytelen(arg))
            append(arg)

        return buf

