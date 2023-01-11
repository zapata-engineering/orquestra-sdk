################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import inspect


def _get_fn_body_as_str(fn, start_marker: str, end_marker: str) -> str:
    src_lines = inspect.getsourcelines(fn)[0]
    start_line_i = None
    for line_i, line in enumerate(src_lines):
        if start_marker in line:
            start_line_i = line_i + 1
            break

    end_line_i = None
    for line_i, line in enumerate(src_lines):
        if end_marker in line:
            end_line_i = line_i
            break

    body_lines = src_lines[start_line_i:end_line_i]
    body_lines = [line.strip("\n") for line in body_lines]

    dedent = 8
    body_lines = [line[dedent:] for line in body_lines]

    return "\n".join(body_lines)


def get_snippet_as_str(fn) -> str:
    return _get_fn_body_as_str(fn, start_marker=fn.__name__, end_marker="</snippet>")
