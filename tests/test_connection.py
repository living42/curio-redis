import pytest

from curio_redis.connection import Connection, ConnectionPool
from curio_redis.exceptions import ConnectionError


def test_connection(kernel):
    results = []

    async def main():
        conn = Connection()
        await conn.send_command(b'SETEX', b'a', b'5', b'balabala')
        response = await conn.read_response()
        results.append(response)

        await conn.send_command(b'GET', b'a')
        response = await conn.read_response()
        results.append(response)

        await conn.send_command(b'DEL', b'a')
        response = await conn.read_response()
        results.append(response)

        await conn.send_command(b'GET', b'a')
        response = await conn.read_response()
        results.append(response)

        await conn.send_command(b'CONFIG GET', b'__foobar__')
        response = await conn.read_response()
        results.append(response)

    kernel.run(main())

    assert results == [
        b'OK',
        b'balabala',
        1,
        None,
        [],
    ]


def test_open_close(kernel):
    conn = Connection()
    results = []

    async def main():
        await conn.connect()
        results.append(conn._sock is not None)
        await conn.close()
        results.append(conn._sock is None)

    kernel.run(main())

    assert all(results)


def test_connection_pool(kernel):

    async def main():
        pool = ConnectionPool()

        connection = await pool.get_connection()
        await pool.release(connection)

        connection2 = await pool.get_connection()

        assert connection is connection2

        await pool.release(connection2)

        await pool.close()

        pool = ConnectionPool(max_connections=1)

        await pool.get_connection()

        with pytest.raises(ConnectionError):
            await pool.get_connection()

        await pool.close()

        pool = ConnectionPool()

        for i in range(5):
            connection = await pool.get_connection()
            await pool.release(connection)

        assert pool._created_connections == 1

    kernel.run(main())
