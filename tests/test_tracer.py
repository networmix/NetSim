import builtins
from unittest import mock

import pytest

from netsim.tracer import Tracer
from netsim.stat import StatFrame


def test_tracer_create_1(mocker):
    mocker.patch("builtins.open", mock.mock_open())
    tracer = Tracer()
    assert builtins.open.mock_calls == [mock.call("trace.jsonl", "w", encoding="utf8")]


def test_tracer_dump_data_1():
    tracer = Tracer()

    tracer.fd = mock.MagicMock()
    tracer.dump_data(StatFrame())
    assert tracer.fd.mock_calls == [
        mock.call.write(
            '{"timestamp": 0, "duration": 0, "last_state_change_timestamp": 0}\n'
        ),
        mock.call.flush(),
    ]

    tracer.fd = mock.MagicMock()
    tracer.dump_data({"timestamp": 0, "duration": 0, "last_state_change_timestamp": 0})
    assert tracer.fd.mock_calls == [
        mock.call.write(
            '{"timestamp": 0, "duration": 0, "last_state_change_timestamp": 0}\n'
        ),
        mock.call.flush(),
    ]

    tracer.fd = mock.MagicMock()
    tracer.dump_data(["TEST", "DATA"])
    assert tracer.fd.mock_calls == [
        mock.call.write("['TEST', 'DATA']\n"),
        mock.call.flush(),
    ]


def test_tracer_get_trace_dumper_1():
    tracer = Tracer()
    tracer.fd = mock.MagicMock()

    data = StatFrame()
    td = tracer.get_trace_dumper(data)
    td()

    data.set_time(5, 5)
    td()

    assert tracer.fd.mock_calls == [
        mock.call.write(
            '{"timestamp": 0, "duration": 0, "last_state_change_timestamp": 0}\n'
        ),
        mock.call.flush(),
        mock.call.write(
            '{"timestamp": 5, "duration": 5, "last_state_change_timestamp": 0}\n'
        ),
        mock.call.flush(),
    ]
