from curio_redis.client import Client


def test_client(kernel):
    async def main():
        client = Client()

        value = b'foo'
        await client.set(b'balabala', value)

        value2 = await client.get(b'balabala')

        assert value == value2

        t = await client.time()

        assert isinstance(t, list)

    kernel.run(main())
