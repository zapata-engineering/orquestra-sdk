========================
Running Locally with Ray
========================

This tutorial walks you through running a workflow on your computer using Ray.
Ray executes your tasks in parallel!

Prerequisites
=============

#. You've :doc:`installed Orquestra Workflow SDK<installing-macos-linux>`.
#. You've :doc:`defined a task and a workflow<quickstart>` in a ``workflow_defs.py`` file.


Start Ray
=========

Run the following command in your terminal to start the background services on your machine:

.. code:: bash

    orq up


This ``orq`` commands ensures that the Ray cluster was set up correctly.


.. note::

    There are other ways to start Ray, e.g. via ``ray start``, but some features of Orquestra Workflow SDK might not work properly.
    In particular, you won't be able to retrieve workflow logs.


After you're done, the Ray cluster can be shut down with:

.. code:: bash

    orq down


Execute Workflow
================

Then, run the following snippet in your ``python`` REPL or a Python script.
It loads your workflow definition, and sends it to Ray.


.. literalinclude:: ../examples/tests/test_local_ray.py
    :start-after: def execute_workflow():
    :end-before: </snippet>
    :language: python
    :dedent: 8


Get Results
===========

The workflow is executed in the background.
To get the calculated values, run the following.

.. literalinclude:: ../examples/tests/test_local_ray.py
    :start-after: def get_results():
    :end-before: </snippet>
    :language: python
    :dedent: 8


The above snippet will print whatever was returned from the ``@sdk.workflow`` function.
For more details on retrieving more information like logs or intermediate task results visit the :doc:`Workflow Runs guide <../guides/workflow-runs>`.
