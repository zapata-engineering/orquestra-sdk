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
    :start-after: def execute_workflow():
    :end-before: </snippet>
    :language: python
    :dedent: 8

.. code-block:: python

    from orquestra.sdk import workflow_logger
    from orquestra.sdk import wfprint

    @sdk.task
    def say_hello():
        logger = workflow_logger()
        logger.info("We're doing some quantum work here!")
        wfprint("Another good way to use raw prints from a workflow!")

.. note::

    It is not recommended (but not prohibited) to use Python built-in logger
    in workflows. The built-in logger will not each log message with the workflow
    and task run IDs. This may make debugging more difficult.
