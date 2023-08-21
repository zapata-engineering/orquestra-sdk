Logging
=======

What are logs useful for?
-------------------------

Logs could help keep track of workflow execution.
It could print intermediate values, warnings, or errors.


Logging in Workflows
--------------------

Orquestra automatically keeps track of any prints made inside a task.

.. literalinclude:: ../examples/tests/test_logging.py
    :start-after: def test_task_logging():
    :end-before: </snippet>
    :language: python
    :dedent: 8
