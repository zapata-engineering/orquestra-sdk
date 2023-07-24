================================
Workspace and Project Management
================================

Compute Engine (CE) organises workflow runs into ``Workspaces`` and ``projects``.
As such, in order to submit or interact with a workflows run from the CLI, it is necessary to specify the ``Workspace`` and ``Project`` that contains it [1]_.

There are three mechanisms by which this can be managed. In descernding order of priority these are:

Explicit CLI Arguments
----------------------

When interacting with CE from the CLI, managing workspaces and projects is typically achieved by setting the ``-w`` and ``-p`` arguments respectively when using the relevant commands.

Environment Variables
---------------------

Where explicit arguments are not supplied, Orquestra looks for two environment variables, ``ORQ_CURRENT_WORKSPACE`` and ``ORQ_CURRENT_PROJECT``. These are unset by default, but can be controlled using:

.. code-block::

    export ORQ_CURRENT_WORKSPACE=my_workspace
    export ORQ_CURRENT_PROJECT=my_project

These variables define a default value for the workspace and/or project that Orquestra will use unless overruled by an explicit CLI argument.

The "vanilla" Orquestra behavior can be restored by simply unsetting these variables:

.. code-block::

    unset ORQ_CURRENT_WORKSPACE
    unset ORQ_CURRENT_PROJECT

Interactive Prompters
---------------------

Where the workspace and project arguments are not provided and one or both of the environment variables are not set, the CLI has a system of interactive prompters to allow the user to select the appropriate unspecified values.

.. [1] When working in the ``Studio``, the workspace and project are set when starting the session.
