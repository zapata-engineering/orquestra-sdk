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
import re
import typing as t
import zlib
from dataclasses import dataclass
from enum import Enum
from tarfile import TarFile

import pydantic

from orquestra.sdk._ray._ray_logs import WFLog

from orquestra.sdk.schema.ir import TaskInvocationId


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


RayFilename = t.NewType("RayFilename", str)


class Message(t.TypedDict):
    """
    Represents a single line indexed by the server side log service.
    """

    log: str
    """
    Single line content.
    """

    ray_filename: RayFilename
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


WorkerID = t.NewType("WorkerID", str)
JobID = t.NewType("JobID", str)


class OutputKind(Enum):
    out = "out"
    err = "err"


@dataclass(frozen=True)
class WorkerLogs:
    ray_filename: RayFilename
    worker_id: WorkerID
    job_id: JobID
    output_kind: OutputKind
    lines: t.Sequence[str]


@dataclass(frozen=True)
class EnvSetupLogs:
    ray_filename: RayFilename
    job_id: str
    lines: t.Sequence[str]


# Example:
# "/tmp/ray/session_latest/logs/worker-903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-01000000-249.err"
WORKER_FILE_PATTERN = re.compile(r".+/worker-(\w+)-(\d+)-\d+.(\w{3})")

# Example: "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
ENV_SETUP_FILE_PATTERN = re.compile(r".+/runtime_env_setup-(\d+).log")


def _parse_ray_filename(filename: RayFilename) -> t.Tuple[WorkerID, JobID, OutputKind]:
    match = WORKER_FILE_PATTERN.match(filename)
    assert match is not None

    worker_id, job_id, ext = match.groups()
    assert worker_id is not None
    assert job_id is not None

    return WorkerID(worker_id), JobID(job_id), OutputKind(ext)


def _parse_env_setup_filename(filename: RayFilename) -> JobID:
    match = ENV_SETUP_FILE_PATTERN.match(filename)
    assert match is not None

    return JobID(match.groups()[0])


def _parse_worker_logs(
    messages: t.Sequence[Message], filename: RayFilename
) -> WorkerLogs:
    worker_id, job_id, output_kind = _parse_ray_filename(filename)

    return WorkerLogs(
        ray_filename=filename,
        worker_id=worker_id,
        job_id=job_id,
        output_kind=output_kind,
        lines=[msg["log"] for msg in messages],
    )


def _parse_env_logs(
    messages: t.Sequence[Message], filename: RayFilename
) -> EnvSetupLogs:
    job_id = _parse_env_setup_filename(filename)

    return EnvSetupLogs(
        ray_filename=filename,
        job_id=job_id,
        lines=[msg["log"] for msg in messages],
    )


@dataclass(frozen=True)
class ParsedLogs:
    workers: t.Sequence[WorkerLogs]
    envs: t.Sequence[EnvSetupLogs]


def parse_logs_archive(response_content: bytes) -> ParsedLogs:
    chunks = _iter_chunks(response_content)
    events = _iter_events(chunks)
    messages = _iter_messages(events)
    messages = list(messages)

    grouped = _group_by_key(messages, key=lambda msg: msg["ray_filename"])

    workers: t.List[WorkerLogs] = []
    envs: t.List[EnvSetupLogs] = []

    for filename, group in grouped.items():
        if WORKER_FILE_PATTERN.match(filename) is not None:
            worker_logs: WorkerLogs = _parse_worker_logs(group, filename=filename)
            workers.append(worker_logs)
        elif ENV_SETUP_FILE_PATTERN.match(filename) is not None:
            env_logs: EnvSetupLogs = _parse_env_logs(group, filename=filename)
            envs.append(env_logs)
        else:
            # We received an unsupported file in the archive. We silently ignore this
            # for future-proofness. We might ask the Platform to index more files in the
            # future.
            pass

    return ParsedLogs(
        workers=workers,
        envs=envs,
    )


@dataclass(frozen=True)
class InterpretedLogs:
    by_task: t.Mapping[TaskInvocationId, t.Sequence[str]]
    env: t.Sequence[str]


def interpret_logs(parsed_logs: ParsedLogs) -> InterpretedLogs:
    """
    Regroups the logs from Ray-based groupind into a form that's suitable for Orquestra.
    """
    # TODO: should this be moved to a runtime? We might wanna reuse it local runtime.
    return InterpretedLogs(
        by_task={
            # TODO: split logs by task invocation IDs
            # https://zapatacomputing.atlassian.net/browse/ORQSDK-840
            "UNKNOWN TASK INVOCATION": [
                line
                for worker_logs in parsed_logs.workers
                for line in worker_logs.lines
            ]
        },
        env=[line for env in parsed_logs.envs for line in env.lines],
    )


def process_logs_response(response_content: bytes) -> InterpretedLogs:
    # Processing logs happens in two steps:
    # 1. We unarchive, de-chunkify the log events we get from CE and group it by
    #    original Ray file like workers and envs.
    # 2. We interpret contents some of the files to group it by the Orquestra concepts
    #    like tasks.
    parsed_logs = parse_logs_archive(response_content)
    return interpret_logs(parsed_logs)
