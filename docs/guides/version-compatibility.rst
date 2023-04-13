=====================
Version Compatibility
=====================

This guide covers the details behind the warning::

    VersionMismatch: Attempting to read a workflow definition generated with a different version of Orquestra Workflow SDK. Please consider re-running your workflow or installing 'orquestra-sdk==...'.


Workflow Version Coupling
=========================

When you write a workflow, the Orquestra Workflow SDK generates an Intermediate Representation (IR) from your ``@workflow``-decorated function.
When you "run" the workflow, this IR is sent to a runtime for execution.
Inside the IR, we store the version of the Workflow SDK that generated it.

When executing the workflow remotely, the remote runtime reads the Workflow SDK version from the IR and uses the same version to interpret the IR, execute tasks, and store results.
Once a workflow run has been started, you will query its status, peek into its graph of task runs, or access the data it produced.

Sometimes these interactions can happen a long time after the workflow was originally executed; the workflow data lives longer than the Workflow SDK version that produced it.
In the meantime, the internals of the Workflow SDK may have changed resulting in unexpected behavior.

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
