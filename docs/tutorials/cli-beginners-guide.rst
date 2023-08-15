=========================================
The Beginner's Guide to the Orquestra CLI
=========================================

.. note::

    This guide assumes that you have already installed the ``orquestra-sdk``. This can be confirmed by running

    .. code-block::

        orq -V

    For installation instructions, see :doc:`Installing<installing-macos-linux>` or :doc:`Installing on Windows <installing-windows>`.

Creating a Workflow Definition
==============================

In order to run a workflow, it must be defined in a python file.
For this guide, we'll assume that we have created a file ``workflow_defs.py`` with the following contents:

.. literalinclude:: ../examples/workflow_defs.py
    :start-after: >> Start
    :end-before: >> End
    :language: python

For an explanation of this workflow, see the :doc:`API Quickstart guide<quickstart>`.

Running the Workflow locally
============================

Starting the Ray Service
------------------------

Orquestra provides utilities to run workflows in a local Ray session.
To start the Ray service, use:

.. code-block:: bash

    orq up

You should see a progress bar, followed by confirmation that the service is running:

.. code-block::

    [####################################]  100%
    Ray  Running  Started!

You can check on the status of the service at any time by running

.. code-block:: bash

    orq status

Submitting the Workflow to be Run
---------------------------------

Once the Ray service has started, the workflow can be submitted with the ``orq wf submit`` command:

.. code-block:: bash

    orq wf submit -c local workflow_defs.py

Take not of the run ID that is displayed - we will need this to interact with the workflow going forward.


.. note::

    ``-c local`` tells orquestra that we want to submit the workflow to run on the local Ray runtime.
    If this argument is omitted, the CLI will ask you to choose from a list of available configurations.

Recovering Workflow Results
---------------------------

Workflow results are accessible via the ``orq wf results`` command.
This takes the workflow run ID as its argument, and can either display a preview of the workflow run outputs in the terminal, or write them to files.

.. code-block:: bash

    orq wf results <WORKFLOW RUN ID>

    orq wf results <WORKFLOW RUN ID> --download-dir <path>

The first command prints the summary:

.. code-block::

    Workflow run [WF RUN ID] has 1 outputs.

    Output 0. Object type: <class 'str'>
    Pretty printed value:
    'Hello Orquestra!'

The second writes each output to a file:

.. code-block::

    Artifact saved at [WF RUN ID]/wf_results/0.json as a text json file.

Running the Workflow Remotely
=============================

By design, running a workflow remotely requires very little to change in comparison to running locally.
The primary difference is that rather than starting a local service, you must be logged in to a cluster.

Access to clusters is managed via the ``orq login`` command:

.. code-block:: bash

    orq login -s <cluster URL> [<runtime>]

Where ``SERVER URI`` is the URI of the server to log into.
This will open a page in your browser where you can log in with your Orquestra credentials.
For more information on handling remote authorisations, see :doc:`Remote Workflows<remote>`.

Managing Workflow Runs
======================

You can access a list of previously submitted workflow runs using:

.. code-block:: bash

    orq wf list

This will allow you to specify one or more configurations and display the IDs, statuses, and start times of workflow runs submitted using that configuration.
This can be very helpful when trying to remember the Run ID of a workflow!

Flags and Prompters
===================

Most ``orq`` commands use a prompter system to guide you through specifying the required details.
For example, the ``orq wf results`` command, if called without any arguments, will prompt you for:

#. The runtime configuration with which the workflow was run.
#. The Workspace in which the workflow was run (if supported by the runtime).
#. The Project in which the workflow was run (if supported by the runtime).
#. The ID of the workflow the results of which are to be displayed or downloaded.

Each of these stages will present a choice of the valid options.
For example:

.. code-block:: bash

    $ orq wf results
    [?] Runtime config: prod-d
    > prod-d
      moreau
      ray

    [?] Workspace: Demo  demo-7c49f2
    > Demo                                  demo-7c49f2
      Scratch                               scratch-6b38e1

    [?] Projects - only one option is available. Proceed with Migration  migration? (Y/n): Y
    [?] Workflow run ID: hello_orquestra_wf-7MJLb-r000
    > hello_orquestra_wf-7MJLb-r000
      test_demo_wf-1Ybre-r000
      hello_orquestra_wf-uu8OM-r000
      hello_orquestra_wf-76gJ8-r000
      hello_orquestra_wf-BEERq-r000

If you already have the information required at your disposal, you can bypass the promper system by specifying these details as flags and arguments to the initial command.
For example, if we already knew the workflow run ID, we can specify this directly and skip the process of identifying it:

.. code-block:: bash

    $ orq wf results hello_orquestra_wf-7MJLb-r000
