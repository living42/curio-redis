from curio_redis import connection


def test_connection(kernel):
    results = []

    async def main():
        conn = connection.Connection()
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

    kernel.run(main())

    assert results == [
        b'OK',
        b'balabala',
        1,
        None
    ]


def test_open_close(kernel):
    conn = connection.Connection()
    results = []

    async def main():
        await conn.connect()
        results.append(conn._sock is not None)
        await conn.close()
        results.append(conn._sock is None)

    kernel.run(main())

    assert all(results)
