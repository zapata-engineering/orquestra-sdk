################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import select
import socket
import time
import typing as t

TRIGGER_MSG = "t".encode()


class TriggerServer:
    def __init__(self):
        self._trigger_msg: bytes = TRIGGER_MSG
        self._socket: t.Optional[socket.socket] = None
        self._port: t.Optional[int] = None

        self.start()

    @property
    def port(self) -> int:
        assert self._port is not None
        return self._port

    def start(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(("127.0.0.1", 0))
        self._socket.listen()
        self._port = self._socket.getsockname()[1]

    def trigger(self):
        assert self._socket is not None

        client_socket, _ = self._socket.accept()
        client_socket.send(self._trigger_msg)

    def close(self):
        assert self._socket is not None

        self._socket.close()


class TriggerClient:  # pragma: no cover
    def __init__(self, port: int):
        self._port = port
        self._trigger_msg = TRIGGER_MSG

    def wait_on_trigger(self, timeout):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", self._port))
        start_time = time.time()

        while True:
            input, _, _ = select.select([sock], [], [], 0.1)
            if input:
                incoming_data = sock.recv(1024)
                assert incoming_data == self._trigger_msg
                break
            ts = time.time()
            if ts - start_time > timeout:
                raise TimeoutError()
