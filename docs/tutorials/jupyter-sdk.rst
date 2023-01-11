===================================
Using the Workflow SDK with Jupyter
===================================

The Orquestra Workflow SDK can be used in Jupyter Notebooks!

Setup
=====

Ensure that Jupyter in installed in your environment. With the environment active, run

.. code:: bash

    pip install jupyter

Jupyter can then be run using simply

.. code:: bash

    jupyter notebook

Jupyter Notebooks and the Orquestra Workflow SDK must be installed in the same environment.

Defining tasks and workflows inside a notebook
==============================================

Workflows and tasks can be defined within Jupyter notebooks, in the same way as in vanilla Python:

.. code-block:: python

    import orquestra.sdk as sdk

    @sdk.task()
    def task_sum_numbers(numbers: tuple):
        return sum(numbers)

    @sdk.workflow
    def wf_sum_numbers():
        numbers = (1, 2)
        return task_sum_numbers(numbers)

Once defined, you can run the workflow:

.. code-block:: python

    workflow_run = wf_sum_numbers().run("in_process")

and get the results:

.. code-block:: python

    assert workflow_run.get_results() == 3

or, using the more verbose forms:

.. code-block:: python

    workflow_definition = wf_sum_numbers()
    workflow_run = workflow_definition.prepare("in_process")
    workflow_run.start()

.. code-block:: python

    workflow_run_results = workflow_run.get_results()
    assert workflow_run_results == 3


Using tasks defined in a different module
=========================================

Tasks that are defined in other modules can be imported and used to build workflows:

.. code-block:: python

    from orquestra.sdk.examples.exportable_wf import make_greeting

.. code-block:: python

    @sdk.workflow
    def wf_hello():
        wf_abc = "ABC"
        return make_greeting("Emiliano", "Zapata", additional_message=wf_abc)

Workflows that use external tasks operate identically to those defined solely in the notebook:

.. code-block:: python

    workflow_run = wf_hello().run("in_process")

.. code-block:: python

    workflow_run_results = workflow_run.get_results()
    assert workflow_run_results == "hello, Emiliano Zapata!ABC"

Using workflows defined in a different module
=============================================

As with tasks, workflows can be imported and used inside a notebook:

.. code-block:: python

    from orquestra.sdk.examples.exportable_wf import my_workflow

.. code-block:: python

    workflow_run = my_workflow().run("in_process")

.. code-block:: python

    workflow_run_results = workflow_run.get_results()
    assert workflow_run_results == ["hello, alex zapata!there"]

Running Workflows with Ray
==========================

The examples above run the workflows "in-process" (the default behaviour for ``start`` / ``run``). To run workflows using Ray, you'll need to define a configuration:

.. code-block:: python

    ray_config = sdk.RuntimeConfig.ray(
        project_dir="path/to/dir", # Optional. Path of the project directory. Defaults to the current dir.
    )

and pass it as an argument when creating the workflow run:

.. code-block:: python

    ray_workflow_run = wf_hello().run(ray_config)

Running Workflows with QE
=========================

Running workflows with QE requires transmitting the code to the QE runtime. This can be done in one of two ways: via a Git or Github import, or an inline import. The method used is controlled by setting the ``source_import`` parameter of the task definition. Tasks defined in a Jupyter notebook will default to an inline import.

As with Ray, a configuration must be defined telling Orquestra what runtime should be used, and this will then be passed to as an argument to ``prepare`` or ``run`` (whichever you are using).

.. code-block:: python

    qe_config = sdk.RuntimeConfig.qe(
        uri = "https://uri/of/cluster",
        token = "authorization token providing access to the cluster",
        project_dir = "path/to/dir", # Optional. Path of the project directory. Defaults to the current dir.
    )


Inline Import
-------------

The inline import serializes the code and sends it to QE. While this is the default for Jupyter notebooks, the code snippet below shows how you can manually express this:

.. code-block:: python

    @sdk.task(source_import=sdk.InlineImport())
    def task_sum_numbers(numbers: tuple):
        return sum(numbers)

    @sdk.workflow
    def wf_sum_numbers():
        numbers = (1, 2)
        return task_sum_numbers(numbers)

.. code-block:: python
    qe_workflow_run = wf_hello().run(qe_config)

Github Import
-------------

A Github import uses a Github repository to store the task source code. You will have to make sure the code is pushed before running on QE.

.. code-block:: python

    import orquestra.sdk as sdk

    @sdk.task(
        source_import=sdk.GithubImport(
            repo="zapatacomputing/braket-workflow-test",
            git_ref="main",
        )
    )
    def task_sum_numbers(numbers: tuple):
        return sum(numbers)

    @sdk.workflow
    def wf_sum_numbers():
        numbers = (1, 2)
        return task_sum_numbers(numbers)

.. code-block:: python

    qe_workflow_run = wf_hello().run(qe_config)
