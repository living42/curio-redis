from .connection import ConnectionPool


class Client:
    def __init__(self, host='localhost', port=6379, max_connections=None):
        self.pool = ConnectionPool(host, port, max_connections)

    async def execute_command(self, *args):
        connection = await self.pool.get_connection()
        try:
            await connection.send_command(*args)
            return await connection.read_response()
        finally:
            await connection.close()

    async def get(self, name):
        return await self.execute_command(b'GET', name)

    async def set(self, name, value):
        return await self.execute_command(b'SET', name, value)

    async def time(self):
        return await self.execute_command(b'TIME')
