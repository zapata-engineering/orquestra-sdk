=====================
Version Compatibility
=====================

This guide covers the details behind the warning::

    VersionMismatch: Attempting to read a workflow definition generated with a different version of Orquestra Workflow SDK. Please consider re-running your workflow or installing 'orquestra-sdk==...'.


Client-Server Coupling
======================

When you're running workflows remotely, you have a local version of Orquestra Workflow SDK on your local machine or the Jupyter environment in Orquestra Portal.
An Intermediate Representation (IR) is generated from your ``@workflow``-decorated function and sent over the wire to the remote runtime.

The remote runtime also comes with its own installation of the Workflow SDK to interpret the workflow IR, execute tasks, and store results.
The Workflow SDK version installed on the server side is selected to match the client's one.

Finally, you're likely to interact with the workflow run to query its status, peek into the graph of task runs, or access the data they produced.
This interaction can happen a long time after the workflow was originally executed; the workflow data lives longer than the Workflow SDK code that produced it.

Recommended Usage
=================

We do our best to maintain backwards compatibility.
However, we constantly develop new features and implement bug fixes which change how the Workflow SDK works internally.
Sometimes it's difficult to predict all consequences, especially across multiple library versions.
When consuming results of a workflow run it's best to use a version of the Workflow SDK that matches the one used to submit and run the workflow.

Why It's Only a Warning
=======================

Whenever we detect a SemVer mismatch between the resources we read and the currently installed library version we emit a warning about version mismatch.
Orquestra Workflow SDK is still in the ``v0.x.x`` stage, so according to the `SemVer rules <https://semver.org/#spec-item-4>`_ every release is breaking.
As a result, anytime you upgrade your local ``orquestra-sdk`` you're likely to see the ``VersionMismatch`` warning when interacting with previously submitted workflow runs.

Example Failure Scenarios
=========================

#. To fix a bug in IR generation we've had to change the representation of the task outputs in the workflow graph.
   After upgrading ``orquestra-sdk``, ``orq task results``/``task_run.get_outputs()`` or ``orq wf results``/``wf_run.get_results()`` show invalid values of the computed artifacts.

#. We've changed the place of artifact serialization to better handle returning values coming from 3rd-party libraries.
   After upgrading ``orquestra-sdk``, ``orq task results``/``task_run.get_outputs()`` or ``orq wf results``/``wf_run.get_results()`` show serialized byte strings instead of plain artifact values.
