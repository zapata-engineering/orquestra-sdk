===================================
Using the Workflow SDK with Jupyter
===================================

The Orquestra Workflow SDK can be used in Jupyter Notebooks!

Prerequisites
=============

#. You've installed Orquestra Workflow SDK (:doc:`macOS, Linux<installing-macos-linux>`).

Setup
=====

Ensure that Jupyter in installed in your environment.
With the environment active, run

.. code:: bash

    pip install jupyter

Jupyter can then be run using simply

.. code:: bash

    jupyter notebook

Jupyter Notebooks and the Orquestra Workflow SDK must be installed in the same environment.

Defining tasks and workflows inside a notebook
==============================================

Workflows and tasks can be defined within Jupyter notebooks, in the same way as in standard Python files:


.. literalinclude:: ../examples/tests/test_jupyter_sdk.py
    :start-after: <snippet cell_task_wf>
    :end-before: </snippet>
    :language: python
    :dedent: 8


Once defined, you can run the workflow:

.. literalinclude:: ../examples/tests/test_jupyter_sdk.py
    :start-after: <snippet cell_wf_run>
    :end-before: </snippet>
    :language: python
    :dedent: 8


and get the results:

.. literalinclude:: ../examples/tests/test_jupyter_sdk.py
    :start-after: <snippet cell_results>
    :end-before: </snippet>
    :language: python
    :dedent: 8


.. note::

   If you define your task in a notebook, you can only use it for building workflows in the same notebook.
   It's a standard code reuse limitation imposed by Jupyter, unrelated to Orquestra.
   If you'd like to reuse a task between notebooks, move it to a ``.py`` file.


Using tasks defined in a different file
=======================================

Tasks defined in other modules can be imported and used to build and run workflows:


.. literalinclude:: ../examples/tests/test_jupyter_sdk.py
    :start-after: def another_task_file_tasks_py():
    :end-before: </snippet>
    :language: python
    :dedent: 12

.. literalinclude:: ../examples/tests/test_jupyter_sdk.py
    :start-after: def another_task_file_notebook():
    :end-before: </snippet>
    :language: python
    :dedent: 12


Using workflows defined in a different file
===========================================

As with tasks, workflows can be defined in a ``.py`` file and used inside a notebook:


.. literalinclude:: ../examples/tests/test_jupyter_sdk.py
    :start-after: def another_wf_file_defs_py():
    :end-before: </snippet>
    :language: python
    :dedent: 12

.. literalinclude:: ../examples/tests/test_jupyter_sdk.py
    :start-after: def another_wf_file_notebook():
    :end-before: </snippet>
    :language: python
    :dedent: 12


.. hint::

   Workflows defined in a notebook can use tasks from the same notebook or a ``.py`` file.

   Workflows defined in a ``.py`` file can use tasks also defined in a ``.py`` file.


.. error::
   Workflows defined in a ``.py`` file can't use tasks defined a notebook, because Jupyter doesn't allow importing symbols from notebooks.


Running Workflows with Ray
==========================

The examples above run the workflows "in-process".
To run workflows from Jupyter using Ray, simply use the ``ray`` config:

.. code-block:: python

    # in: a notebook cell
    ray_wf_run = wf_hello().run("ray")


See :doc:`Running Locally with Ray <ray>` for more information.


Running Workflows with CE
=========================

Running workflows with CE requires transmitting the task code to the CE runtime.

If your task is defined in a notebook cell, the ``@sdk.task(source_import=...)`` needs to be set to an ``InlineImport`` (the default behavior).
Otherwise, CE won't be able to access the task code.

If your task is defined in a ``.py`` file, you can use any of the standard imports.
Please refer to the :doc:`Dependency Installation guide<../guides/dependency-installation>` for more details about imports and CE.
