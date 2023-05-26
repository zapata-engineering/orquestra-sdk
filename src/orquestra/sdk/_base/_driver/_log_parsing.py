################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Driver/CE API endpoints for logs return a single compressed archive. This module
contains code for turning the logs archive into a list of log line strings we can
present to the user.
"""
import io
import json
import typing as t
import zlib
from tarfile import TarFile

import pydantic

from orquestra.sdk._ray._ray_logs import WFLog


def _archived_file_contents(response_content: bytes, file_name: str) -> str:
    """
    Raises:
        zlib.error: when the archive can't be decompressed
    """
    try:
        unzipped: bytes = zlib.decompress(response_content, 16 + zlib.MAX_WBITS)
    except zlib.error:
        raise

    untarred = TarFile(fileobj=io.BytesIO(unzipped)).extractfile(file_name)
    assert untarred is not None

    return untarred.read().decode()


# Each event is a pair of [timestamp, message]. There's no good way of specyfing
# fixed-sized lists in Python, so we're using the "Annotated" type hint.
#
# The timestamp is a float unix epoch timestamp of the time when the log line was
# *indexed* by the Platform-side log service. This is different from the time when the
# log line was emitted.
#
# The message contains a single line from a file produced by Ray + some metadata about
# the log source.
#
# The CE API returns a single archived file called "step-logs". After unarchiving, this
# file contains newline-separated chunks. Each chunk is a JSON-encoded list of events.


class Message(t.TypedDict):
    """
    Represents a single line indexed by the server side log service.
    """

    log: str
    """
    Single line content.
    """

    ray_filename: str
    """
    Server-side file path of the indexed file.
    """

    tag: str
    """
    An identifier in the form of "workflow.logs.ray.<workflow run ID>".
    """


Event = t.Annotated[t.List, float, Message]
Section = t.List[Event]


def _iter_chunks(response_content: bytes):
    decoded = _archived_file_contents(response_content, file_name="step-logs")
    for chunk in decoded.splitlines():
        yield chunk


def _iter_events(chunks: t.Iterable[str]):
    for chunk in chunks:
        section: Section = json.loads(chunk)
        for event in section:
            yield event


def _iter_messages(events: t.Iterable[Event]):
    for event in events:
        _, message = event
        message: Message
        yield message


T = t.TypeVar("T")


def _group_by_key(
    it: t.Iterable[T], key: t.Callable[[T], t.Any]
) -> t.Mapping[t.Any, t.Sequence[T]]:
    grouped = {}

    for element in it:
        key_value = key(element)
        grouped.setdefault(key_value, []).append(element)

    return grouped


def parse_logs_archive(response_content: bytes) -> t.List[str]:
    chunks = _iter_chunks(response_content)
    events = _iter_events(chunks)
    messages = _iter_messages(events)
    grouped = _group_by_key(messages, key=lambda msg: msg["ray_filename"])

    # Parse the decoded data as logs
    # TODO: index by taskinvocationID rather than workflowrunID [ORQSDK-777]
    logs = []
    for message in messages:
        message: Message
        try:
            # Orquestra logs are jsonable - where we can we parse these and
            # extract the useful information
            interpreted_log = WFLog.parse_raw(message["log"])
            logs.append(interpreted_log.message)
        except pydantic.ValidationError:
            # If the log isn't jsonable (i.e. it comes from Ray) we just return
            # plain log content.
            logs.append(message["log"])

    return logs
