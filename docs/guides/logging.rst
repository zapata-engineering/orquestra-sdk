Logging
=================

.. note: find a good place for this

What are logs useful for?
-------------------------

Logs could help keep track of workflow execution. It could print intermediate
values, warnings, or errors.


Logging in Workflows
--------------------

Orquestra Workflow SDK provides tools to organize effective logging in workflows.
SDK logger not only prints messages, but also adds extra tags with the workflow
and task run IDs.
The helper function ``wfprint()`` function acts similarly to built-in ``print()`` and
provides tags with workflow execution context like SDK's logger.

.. literalinclude:: ../examples/tests/test_logging.py
    :start-after: def test_task_logging():
    :end-before: </snippet>
    :language: python
    :dedent: 8
