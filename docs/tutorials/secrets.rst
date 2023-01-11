=======
Secrets
=======

Your code often interacts with external services.
For example, you may want to run a quantum circuit::

    from orquestra.integrations.qiskit.runner import create_ibmq_runner

    circuit = ...
    backend = create_ibmq_runner("???", "ibmq_lima")
    measurements = backend.run_and_measure(circuit, 1000)


Orquestra lets you store and retrieve secret values without embedding them directly in your source code.
This is especially important when sending workflows for remote runs.


Accessing Secrets From Local Machine
====================================

Orquestra retains secrets on an Orquestra Platform server. Accessing it requires authorization. Before continuing, please complete the authorization flow by following the step 1 from :doc:`Remote workflows <remote>`.

Then, you can read and set secrets within a script or a Python REPL:

.. literalinclude:: ../examples/tests/test_secrets.py
    :start-after: <snippet set-secret-from-repl>
    :end-before: </snippet>
    :language: python
    :dedent: 8


``name`` is the secret identifier. It can be any string you like, as long as it fits in the length limit. This identifier is subsequently used to access the secret. Secrets are scoped to a single user account, so there's no risk you're overriding somebody else's entry.

``value`` is any string you like (up a reasonable length limit).

``config_name`` specifies the configuration that stores the cluster URI and auth token that are used to access the secrets vault.


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
