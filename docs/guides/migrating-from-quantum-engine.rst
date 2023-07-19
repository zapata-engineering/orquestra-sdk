=============================
Migrating From Quantum Engine
=============================

By August 2023, Quantum Engine (QE) is being sun set.
Running workflows remotely will only work with Compute Engine (CE).
This document covers the differences between QE and CE, and how to move to CE.


Am I Affected?
==============

If you're using ``orq login --qe``, you're on QE.
Read this guide.

If you're using ``orq login`` or ``orq login --ce``, you're already on CE.
You can probably skip this doc.

.. note:: ``orq login`` uses CE by default since the version 0.51.0.

Another way to check it is to run ``orq login --list``.
If the "Runtime" column says "CE_REMOTE", you're all set.


Checklist
=========

Short list:

#. Use Python 3.9. You can check it with ``python -V``.
#. Run ``pip install -U orquestra-sdk`` frequently.
#. Update your task and workflow "imports". More info below.
#. Update your task and workflow "resources". More info below.
#. When logging in, use ``orq login``.


What's The Difference?
======================

.. list-table:: Feature Comparison
   :header-rows: 1

   * - Feature
     - QE
     - CE
   * - Underlying workflow engine
     - `Argo <https://github.com/argoproj/argo-workflows>`_
     - `Ray <https://www.ray.io>`_
   * - Tasks are executed in...
     - Containers. Each task run results in spawning a separate container. This wastes your time and Platform's CPU cycles.
     - Ray workers. If possible, Ray automatically reuses matching Python environments across task runs.
   * - Maximum workflow size
     - Typically, hundreds of task invocations. Hard limit on length of the internal workflow representation. It gets worse if the workflow contains embedded input data.
     - Probably tens of thousands of task invocations in a single workflow. The limit haven't been hit yet.
   * - Authorization with private git repos
     - A special Orquestra service account has to be added to your repo with a read access. Requires coordination between you and an Orquestra administrator.
     - Relies on Personal Access Tokens. Self-service.
   * - Loading tasks from a git repo
     - Works with bare scripts and setuptools-like packages.
     - Requires a setuptools-like package manifest (``pyproject.toml``/``setup.cfg``/``setup.py``, etc).
   * - Compute resources
     - Each task has dedicated compute resources.
       Resource request for one task doesn't affect the execution of other tasks (up to the cluster's limit).
     - Compute resources are first assigned to the workflow, and then distributed across tasks.

Python Version
==============

With QE, it was common to use Python 3.8.
It was also possible to override the Python version by providing a custom image.

CE requires using Python 3.9.
Custom task images need to come with a Python version that matches the Ray head node, so changing the Python version isn't supported yet.


Imports That Still Work
=======================

Defaults
--------

If you didn't specify any imports in your task's decorator, no action is required.

.. literalinclude:: ../examples/tests/test_migrating_from_quantum_engine.py
   :start-after: def defaults():
   :end-before: </snippet>
   :language: python
   :dedent: 8


``InlineImport``
----------------

Albeit verbose, this still works as before:

.. literalinclude:: ../examples/tests/test_migrating_from_quantum_engine.py
   :start-after: def inline_import():
   :end-before: </snippet>
   :language: python
   :dedent: 8


``PythonImports``
------------------

Still works.

.. literalinclude:: ../examples/tests/test_migrating_from_quantum_engine.py
   :start-after: def python_imports():
   :end-before: </snippet>
   :language: python
   :dedent: 8


Migrating Git Imports
=====================

Git-related imports need special care when migrating to CE.
The underlying mechanism differs a lot.
In QE, git imports were powered by running scripts to clone repos and install repos from source.
In CE, we rely on `Ray's Runtime Environments <https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#runtime-environments>`_ to do the heavy lifting for us.


.. Note:: The ``@sdk.task()`` examples in this section show migrating ``source_import=``, but the same rules apply to ``dependency_imports=``.


Project Structure
-----------------

QE used to support loading code from a git repo with loosely related Python files.
CE requires setting up a package manifest.

.. note::
   This advice applies to both ``GithubImport`` and ``GitImportWithAuth``.


Before
~~~~~~

.. code-block::

   $ tree .
   .
   ├── metrics.py
   ├── ml.py
   └── preproc.py


After
~~~~~

.. code-block::

   $ tree .
   .
   ├── pyproject.toml
   ├── setup.cfg
   └── src
       └── my_proj
           ├── metrics.py
           ├── ml.py
           └── preproc.py


.. _github-import-private:

``GithubImport`` With A Private Repo
------------------------------------

If the referenced repo is private, you need to set up authentication.
Otherwise, CE won't be able to clone your repo.

Before
~~~~~~

.. literalinclude:: ../examples/tests/test_migrating_from_quantum_engine.py
   :start-after: def github_import_private_before():
   :end-before: </snippet>
   :language: python
   :dedent: 8


After
~~~~~

.. warning:: This use case requires setting up an access token.
   Please refer to the :doc:`Dependency Installation guide <./dependency-installation>` for more information.


.. literalinclude:: ../examples/tests/test_migrating_from_quantum_engine.py
   :start-after: def github_import_private_after():
   :end-before: </snippet>
   :language: python
   :dedent: 8


.. _github-import-public:

``GithubImport`` With A Public Repo
-----------------------------------

If the repo is public, you don't have to specify the username and access token secret.
In this situation, the following example will continue to work on CE.

.. literalinclude:: ../examples/tests/test_migrating_from_quantum_engine.py
   :start-after: def github_import_public():
   :end-before: </snippet>
   :language: python
   :dedent: 8


``GitImport``
-------------

Before
~~~~~~

A ``GitImport`` with URI and ref name.

.. code-block:: python

    @sdk.task(
        source_import=sdk.GitImport(
            repo_url="https://example.com/some_person/some_repo.git",
            git_ref="v1.0.1",
        ),
    )
    def demo_task():
        pass


After
~~~~~

#. If possible, please use a :ref:`GithubImport<github-import-private>`.
#. Otherwise, see the following example.
   Please refer to the :doc:`Dependency Installation guide<./dependency-installation>` for instructions on setting up the auth secret.


.. literalinclude:: ../examples/tests/test_migrating_from_quantum_engine.py
   :start-after: def git_import_with_auth():
   :end-before: </snippet>
   :language: python
   :dedent: 8


``GitImport.infer()``
---------------------

The usage of ``GitImport.infer()`` isn't recommended.
Orquestra users often use multiple levels of nested git repos, and the result of ``.infer()`` is difficult to predict.

When referring to a public repo, ``GitImport.infer()`` will continue to work with CE.
However, please consider switching to an explicit :ref:`GithubImport<github-import-public>`.

``GitImport.infer()`` won't be extended to support referring to private repos. Please use a :ref:`GithubImport<github-import-private>` instead.

.. note::
   Orquestra Workflow SDK is just a Python library.
   Most of the machinery can be easily extended to better fit your specific use case.
   For example, you can write your own helpers that resolve repo name or the latest commit hash, and passes them as arguments to the ``GithubImport``.
   This approach has a potential to be more reliable than a generic helper provided by the Orquestra Workflow SDK.

   .. code-block:: python

        REPO_NAME = "zapatacomputing/my_source_repo"

        @sdk.task(
            source_import=sdk.GithubImport(
                REPO_NAME,
                git_ref=latest_commit_sha(),
            ),
        )
        def my_task():
            ...


GPU And Custom Images
=====================

Please reach out to the SDK team if you need to use custom images on CE.
There's a :doc:`detailed guide on this topic<./custom-images>`, but the images need to be rebuilt with every Orquestra release.


Resource Requests
=================

In CE there's a difference between *task resources* and *workflow resources*.
If you don't change the resource requests, your workflow resources will be inferred to fit the largest task resource request.
The largest task (by resources request) will take all the available resources, preventing other tasks from running in parallel.
**The consequence is**: the output values will be correct, but the overall workflow execution might take longer than on QE.

To better allocate compute resources to run your tasks, please refer to :doc:`Resource Management guide<resource-management>`.


.. note::
   It's a complex topic, and it's tricky to provide a generic migration guide ahead of time.
   Please reach out to the SDK or Platform teams if you need help.
