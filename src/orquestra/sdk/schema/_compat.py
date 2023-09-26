################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Backwards-compatibility layer for accessing models generated with previous SDK versions.
"""  # noqa: D200, D212
from . import ir


def result_is_packed(task_def: ir.TaskDef) -> bool:
    """Tasks can return one or multiple values.

    We call multi-valued result "packed" because it can be unpacked in the workflow
    function.

    Nowadays, we base this information on the task definition's metadata.
    With orquestra-sdk<=0.45.1 the behavior was unreliable.
    """
    if (meta := task_def.output_metadata) is not None:
        return meta.is_subscriptable
    else:
        # We can't be sure. It's safer to assume the output is a single value so we
        # don't attempt to subscript it.
        return False


def versions_are_compatible(generated_at: ir.Version, current: ir.Version) -> bool:
    """Checks if we can assume that we're okay with consuming the given IR.

    Args:
        generated_at: version of the Workflow SDK used to generate the IR.
        current: currently installed version of the Workflow SDK
    """
    if current.major == 0:
        # For v0 we require exact match.
        return generated_at == current

    if generated_at.major != current.major:
        return False

    return generated_at.minor <= current.minor
