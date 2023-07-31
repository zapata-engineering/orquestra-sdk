==========
Quickstart
==========

This example presents the shortest possible path to running a workflow with Orquestra from a Python script.
It assumes you've :doc:`installed Orquestra Workflow SDK <installing-macos-linux>`.

Define a task and a workflow
============================

Create a file called ``workflow_defs.py`` with the following content [1]_:

.. literalinclude:: ../examples/workflow_defs.py
    :start-after: >> Start
    :end-before: >> End
    :language: python


``@sdk.task`` exposes a Python function so that Orquestra can execute it.

``@sdk.workflow`` transforms a Python function into an Orquestra workflow.
It tells Orquestra what tasks to execute, in which order, and how to pass the data between tasks.


Run!
====

Now paste the following snippet into your ``python`` REPL:

.. literalinclude:: ../examples/quickstart.py
    :start-after: >> Start
    :end-before: >> End
    :language: python


You're supposed to see ``Hello Orquestra!`` text in your terminal output.

Running with ``in_process`` executes the workflow in a single thread.
This means that the ``run()`` command blocks execution until the workflow run completes, and that results from the run will not persist if the process is terminated.

The ``in_process`` execution is similar to directly calling your function in Python, and it's suitable for quick prototypes or debugging, but it doesn't show the full potential of Orquestra.
For more advanced features, see the next tutorials in this series.

.. [1] The use of the name ``workflow_defs.py`` is not a requirement. Rather, this is a convenient convention that we adopt throughout this documentation.
