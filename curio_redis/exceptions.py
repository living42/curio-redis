class RedisError(Exception):
    pass


class ProtocolError(RedisError):
    pass


class ReplyError(ProtocolError):
    pass


class ConnectionError(RedisError):
    pass
