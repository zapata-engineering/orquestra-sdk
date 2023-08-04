=======
Secrets
=======

Your code often interacts with external services.
For example, you may want to run a quantum circuit on real quantum hardware::

    from orquestra.integrations.qiskit.runner import create_ibmq_runner

    circuit = ...
    backend = create_ibmq_runner("???", "ibmq_lima")
    measurements = backend.run_and_measure(circuit, 1000)


Orquestra lets you store and retrieve secret values without embedding them directly in your source code.
This is especially important when sending workflows for remote runs.

Prerequisites
=============

#. You've :doc:`installed Orquestra Workflow SDK<installing-macos-linux>`.
#. You're familiar with :doc:`running workflows remotely<remote>`.

Accessing Secrets From Local Machine
====================================

Orquestra retains secrets on an Orquestra Platform server.
Accessing it requires authorization.
Before continuing, please complete the authorization flow by following step 1 from :doc:`Remote workflows <remote>`.

Then, you can read and set secrets within a script or a Python REPL:

.. literalinclude:: ../examples/tests/test_secrets.py
    :start-after: <snippet set-secret-from-repl>
    :end-before: </snippet>
    :language: python
    :dedent: 8


``name`` is the secret identifier.
Secret names may include letters, numbers, and dashes, and must not start or end with a dash.
This identifier is subsequently used to access the secret.
Secrets are scoped to a workspace, so care must be taken when managing secrets in shared workspaces.

``value`` is any string you like (up a reasonable length limit).

``workspace_id`` specifies the workspace id where the secret will be saved.
If omitted, your personal workspace will be used.
You can get workspace_id from the workspace view in Orquestra Portal or by using ``sdk.list_workspaces()``."

``config_name`` specifies the name of a stored configuration that specifies the cluster URI and auth token that are used to access the secrets vault.


You can also read the secret value from a local script or the REPL:

.. literalinclude:: ../examples/tests/test_secrets.py
    :start-after: <snippet get-secret-from-repl>
    :end-before: </snippet>
    :language: python
    :dedent: 8


Accessing Secrets Within Workflows
==================================

Secret values can be read from within tasks:


.. literalinclude:: ../examples/tests/test_secrets.py
    :start-after: <snippet get-secret-from-task>
    :end-before: </snippet>
    :language: python
    :dedent: 8


``config_name`` is required when running the task on the Local Runtime.
The workflow runs on your computer, but the secrets storage is on Orquestra Platform.

``config_name`` is ignored when running the task remotely.
The workflow runs on Orquestra Platform, and the secrets vault is already on the same cluster.
In this case, the SDK handles authorization for accessing secrets vault automatically.

``workspace_id`` specifies the workspace id which contains the secret. If omitted, personal workspace will be used. You can get workspace_id from the workspace view in the web Orquestra Portal or by using list_workspace API call.
