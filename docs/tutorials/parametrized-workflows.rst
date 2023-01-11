========================
Parametrized Workflows
========================

This tutorial walks you through running a workflow that is parametrized.
Examples given in this tutorial will run those workflows in a local Ray instance.

Prerequisites
=============

#. You've :doc:`installed Orquestra Workflow SDK<installing-macos-linux>`.
#. You've :doc:`started Ray<ray>`.


Definition of Parametrized Workflow
===================================

Parametrized workflow is nothing more than workflow function that accepts parameters.

As an example of parametrized workflow with 1 task, you can define one as follows:

.. literalinclude:: ../examples/tests/test_parametrized_workflows.py
    :start-after: def parametrized_workflow():
    :end-before: </snippet>
    :language: python
    :dedent: 8

For further snippets to work - save it in file called ``parametrized_workflow.py``


Execute Workflow
================

Then, run the following snippet in your ``python`` REPL or a Python script.
It loads your workflow definition, and executes it on local Ray cluster, waits for it to finish
and prints out results


.. literalinclude:: ../examples/tests/test_parametrized_workflows.py
    :start-after: def execute_single_workflow():
    :end-before: </snippet>
    :language: python
    :dedent: 8

.. warning::

    Parametrized workflows can be submitted only via Python API. Currently, it is not
    possible to submit parametrized workflows via the CLI.


Get Results
===========
The workflow is executed in the background.
To get the calculated values, either follow the instruction in :doc:`started Ray<ray>`.
or run the following

.. literalinclude:: ../examples/tests/test_parametrized_workflows.py
    :start-after: def get_results():
    :end-before: </snippet>
    :language: python
    :dedent: 8


Tuning Workflows Using Parameters
=================================

The biggest advantage of parametrized workflow is to run multiple versions of the same workflow
to compare results for different inputs. This can be obtained by executing workflow with
different input parameters in a loop as in this example:

.. literalinclude:: ../examples/tests/test_parametrized_workflows.py
    :start-after: def execute_multiple_workflows():
    :end-before: </snippet>
    :language: python
    :dedent: 8


The above snippet will print results from all 5 of the workflows executed.
