################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################
def except_from():
    try:
        try:
            raise KeyError("key")
        except KeyError as e:
            raise ValueError("Invalid file") from e
    except ValueError as e:
        raise RuntimeError("Unable to do thing") from e


def except_from_within_except():
    try:
        try:
            raise KeyError("key")
        except KeyError:
            raise ValueError("Invalid file")
    except ValueError as e:
        raise RuntimeError("Unable to do thing") from e


def except_within_except():
    try:
        try:
            raise KeyError("key")
        except KeyError:
            raise ValueError("Invalid file")
    except ValueError:
        raise RuntimeError("Unable to do thing")


def except_no_message():
    raise RuntimeError()


def except_plain():
    raise RuntimeError("Unable to do thing")


def _a():
    except_plain()


def _b():
    _a()


def except_stack():
    _b()
