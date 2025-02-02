"""Test wait_for_uri() module method"""
from contextlib import contextmanager
import socket

import pytest

import pbench.common
from pbench.common.exceptions import BadConfig


def test_wait_for_uri_succ(monkeypatch):
    called = [None]

    @contextmanager
    def success(*args, **kwargs):
        called[0] = args
        yield None

    monkeypatch.setattr(socket, "create_connection", success)
    pbench.common.wait_for_uri("http://localhost:42", 142)
    first_arg = called[0][0]
    assert first_arg[0] == "localhost" and first_arg[1] == 42, f"{called[0]!r}"


def test_wait_for_uri_bad():
    with pytest.raises(BadConfig) as exc:
        pbench.common.wait_for_uri("http://:42", 142)
    assert str(exc.value).endswith("host name")

    with pytest.raises(BadConfig) as exc:
        pbench.common.wait_for_uri("http://example.com", 142)
    assert str(exc.value).endswith("port number")


def setup_conn_ref(monkeypatch):
    """Setup a mock'd up environment for socket.create_connection to drive
    ConnectionRefusedError behaviors.

    The `wait_for_uri()` method invokes `socket.create_connection()`,
    `time.time()` to get the current timestamp, and `time.sleep()` to wait one
    second before re-trying to create a connection.

    An example sequence of calls made by `wait_for_uri()`:

        1. time()
        2. create_connection() - raises ConnectionRefusedError()
        3. time()
        4. sleep()
        5. create_connection() - raises ConnectionRefusedError()
        6. time()
        7. sleep()
        8. create_connection() - succeeds

    Each mock'd call to `time()` returns the current time, starting at 0, and
    increments the "clock" by one.

    Each mock'd call to `sleep()` simply records that it was called, and returns
    immediately.

    Each mock'd call to `create_connection()` records it was called, along with
    the current clock value, raises ConnectionRefusedError() while the clock is
    strictly less than 3, and returns successfully (really yields to make the
    contextmanager behavior work) when the clock is 3 or greater.
    """
    clock = [0]
    called = []

    @contextmanager
    def conn_ref(*args, **kwargs):
        called.append(f"conn_ref [{clock[0]}]")
        if clock[0] < 3:
            raise ConnectionRefusedError()
        yield None

    def sleep(*args, **kwargs):
        called.append("sleep")

    def time() -> int:
        curr_time = clock[0]
        called.append(f"time [{curr_time}]")
        clock[0] += 1
        return curr_time

    monkeypatch.setattr(socket, "create_connection", conn_ref)
    monkeypatch.setattr(pbench.common, "sleep", sleep)
    monkeypatch.setattr(pbench.common, "time", time)
    return called


def test_wait_for_uri_conn_ref_succ(monkeypatch):
    """Verify connection attempts initially fail, but then ultimately succeed
    before the timeout period.
    """
    called = setup_conn_ref(monkeypatch)
    # The mock will return successfully after 3 ticks of the mock'd clock, so 42
    # is sufficiently long enough to wait given it is the answer to the ultimate
    # question of life, the universe, and everything.
    pbench.common.wait_for_uri("http://localhost:42", 42)
    assert called == [
        "time [0]",  # Clock moves from 0 to 1
        "conn_ref [1]",  # Raises
        "time [1]",  # Clock moves from 1 to 2
        "sleep",
        "conn_ref [2]",  # Raises
        "time [2]",  # Clock moves from 2 to 3
        "sleep",
        "conn_ref [3]",  # Succeeds
    ], f"{called!r}"


def test_wait_for_uri_conn_ref_fail(monkeypatch):
    """Verify connection attempts fail until the timeout period has expired."""
    called = setup_conn_ref(monkeypatch)
    with pytest.raises(ConnectionRefusedError):
        pbench.common.wait_for_uri("http://localhost:42", 1)
    assert called == [
        "time [0]",  # Clock moves from 0 to 1
        "conn_ref [1]",  # Raises
        "time [1]",  # Clock moves from 1 to 2
        "sleep",
        "conn_ref [2]",  # Raises
        "time [2]",  # Clock moves from 2 to 3
        # wait_for_uri() re-raises because the clock is now
        # 2 and we told it to stop once we have moved beyond
        # the time.
    ], f"{called!r}"
