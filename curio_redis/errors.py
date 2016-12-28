class CurioRedisError(Exception):
    pass


class ProtocolError(CurioRedisError):
    pass


class ReplyError(ProtocolError):
    pass
