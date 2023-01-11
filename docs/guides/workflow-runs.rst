=============
Workflow Runs
=============

This guide covers interacting with Orquestra Workflows using the ``sdk.WorkflowRun`` API.

Some useful terminology:

Task definition
    A function exposed to Orquestra.
    A single workflow can include multiple invocations of the same task definition with different arguments.

Task run
    A record of a single execution of a task definition.
    Can be used to retrieve logs, and values of arguments and computed output artifacts.
    Task runs can be executed in parallel.

Workflow definition
    A combination of invocations of task definitions.

Workflow run
    A collection of task runs related to executing a single workflow definition.


Importing Workflow Definitions
------------------------------

Workflow and task definitions can be imported into your Python script from other Python files, including your workflow-defs file, in exactly the same way as a regular Python import.
The following examples use this method.
Note that the steps following the import of the workflow are agnostic to whether the workflow is imported or defined in the same file.


Results, Logs, and Artifacts
----------------------------

Interacting with workflow runs is made possible by the WorkflowRun object and its methods. WorkflowRun objects are not intended to be instantiated directly, but are returned when a workflow run is created (i.e. when the ``prepare()`` or ``run()`` method of a workflow definition is called), or from the ``WorkflowRun.by_id()`` class method. The former case provides WorkflowRun objects for runs that were created previously in the same process, the creates WorkflowRun objects for runs created in a separate process. Runs can be identified solely by their IDs, allowing you to reconstruct the WorkflowRun of a previously started workflow as long as you know the run ID. The following example demonstrates how a separate process can get the results of a workflow run.

.. literalinclude:: ../examples/tests/test_local_ray.py
    :start-after: def execute_workflow():
    :end-before: </snippet>
    :language: python
    :dedent: 8

.. note::
    As the workflows are being executed in a separate process, it is possible to call ``get_results()`` before the workflow has completed, which will raise an error. This can be avoided either by checking whether the workflow run has completed using ``get_status()``, or by calling ``get_results(wait=True)`` which will block until the workflow run ends.


Storing and Retrieving Configurations
-------------------------------------

Configurations control interaction with runtime backend. The choice of a configuration
determines what runtime to use (in-process, local Ray, remote).
In addition, a configuration contains details required for connection with the runtime, like cluster URL and auth token.

The built-in configurations include:
* ``in_process``. You can use it via ``sdk.RuntimeConfiguration.in_process()` or by passing `"in_process"` whenever config is required.
* ``ray``. You can use it via ``sdk.RuntimeConfiguration.ray()` or by passing `"ray"` whenever config is required.
Configurations for interaction with remote runtime are created by using the CLI for auth flow (<paste tutorial link here>).

.. literalinclude:: ../examples/config_management.py
    :start-after: >> Tutorial code snippet: save config
    :end-before: >> End save config
    :language: python

Configs for remote clusters will get auto-named based on URI.
Local ray runtime has hardcoded config name, either "local" or "ray".
In-process runtime has also hardcoded config name, "in-process"

Saved configs can be listed with ``list_configs()`` and retrieved with ``load()``:

.. literalinclude:: ../examples/config_management.py
    :start-after: >> Tutorial code snippet: list configs
    :end-before: >> End list configs
    :language: python

This will display a list of the saved configs. Once the desired config is identified, it can be loaded as follows:

.. literalinclude:: ../examples/config_management.py
    :start-after: >> Tutorial code snippet: load config
    :end-before: >> End load config
    :language: python


Running Workflows with Configurations
-------------------------------------

Before running a workflow with a custom configuration, the confiuration must first be saved. This configuration can then be passed to the ``prepare()`` or ``run()`` methods of the workflow definition to run the workflow:

.. literalinclude:: ../examples/quickstart.py
    :start-after: >> Tutorial code snippet: run workflow with stored config - long version
    :end-before: >> end run workflow with stored config - long version
    :language: python

or, if you don't need the RuntimeConfig object accessible in your script, loading the configuration can be handled by the ``run()`` or ``prepare()`` method:

.. literalinclude:: ../examples/quickstart.py
    :start-after: >> Tutorial code snippet: run workflow with stored config - short version
    :end-before: >> end run workflow with stored config - short version
    :language: python
