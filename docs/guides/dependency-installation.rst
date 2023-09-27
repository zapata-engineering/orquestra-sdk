=======================
Dependency Installation
=======================

If you send your workflow for execution on a remote runtime (Orquestra Platform), you have to somehow move your task code to the server.
It's not always a straightforward goal and there are a couple of techniques to achieve it, each one comes with trade-offs.

In Orquestra, you declare how to supply the task's code and the libraries it depends on using *Imports*.
These are specified on a per-task basis by setting ``source_import=`` and ``dependency_imports=`` parameters in the ``@sdk.task()`` decorator.


Quick Reference
===============

For convenience, the table below summarizes which *Import* should be used in various contexts.
The following sections give a more complete explanation of these importers and their usage.


.. list-table:: Quick Reference For Orquestra Imports
   :header-rows: 1

   * - Import
     - For Use With
     - Be Aware

   * - Default (internally called ``InlineImport``).
       An analogue of copying and pasting the task source code to move it to the server.
     - * Works out-of-the-box for simple cases.
       * Doesn't require running ``git commit && git push`` every time you edit your code.
       * Works with tasks defined in a Jupyter notebook.
     - * Has to be accompanied by ``dependency_imports`` if the task code depends on a third-party library.
       * Uses pickle-like serialization under the hood.
         This might cause edge cases like invalid symbol resolution errors when the scenario is complicated.
       * Task source code is part of the internal workflow definition representation.
         It might result in hitting workflow size limits.

   * - ``PythonImports``.
       An analogue of running ``pip install ...`` on the server.
     - * Works well as a ``dependency_imports=[...]`` addition to ``InlineImport`` to allow using 3rd-party libraries.
       * Best suited for referencing libraries available on `PyPI <https://pypi.org/>`_ like ``torch``.
     - * Can't be reliably used to refer to an unpublished, WIP projects.
       * Be cautious when including ``orquestra-sdk`` as a dependency - the version installed inside the task *must* match the version used to submit the workflow.

   * - ``GithubImport``
     - * Well-suited for unpublished, WIP projects.
       * More robust than ``InlineImport``---it's not prone to pickling errors.
       * Less likely to hit the workflow size limits than ``InlineImport``.
     - * Requires a setuptools-like package manifest (``pyproject.toml``/``setup.cfg``/``setup.py``, etc.)
       * Requires setting up an access token if the repo isn't public.
       * Requires committing and pushing code edits before they can be used in a remote workflow.
       * Doesn't work with other git hostings than GitHub.
       * Doesn't work with tasks defined in a Jupyter notebook.

   * - ``GitImportWithAuth``
     - * All pros of ``GithubImport``.
       * Is likely to work with other git hostings than GitHub.
       * Might support more complicated use cases than ``GithubImport``.
     - * All cons of ``GithubImport``, apart from being GitHub-exclusive.
       * Verbose.
         It's easy to make a spelling mistake when specifying its parameters.


Good Practice
=============

The following advices are sorted by an increasing level of complexity, from "hello world" to "complicated project within a niche use case".
See also `Imports In Detail`_ for more info about each one.

#. The defaults: ``InlineImport`` as the ``source_import`` and empty ``dependency_imports``.

   .. literalinclude:: ../examples/tests/test_dependency_installation.py
       :start-after: def good_practice_defaults():
       :end-before: </snippet>
       :language: python
       :dedent: 8


#. If your task depends on third-party libraries: ``PythonImports`` set as ``dependency_imports=[...]``.

   .. literalinclude:: ../examples/tests/test_dependency_installation.py
       :start-after: def good_practice_python_imports():
       :end-before: </snippet>
       :language: python
       :dedent: 8


#. ``GithubImport`` as ``source_import=`` with no ``dependency_imports=`` and specify the project deps in a ``setup.cfg`` file.
   Better than (2) if you hit an edge case related to source code serialization, or when you prefer to have a clear place to specify your project dependencies, e.g. for better collaboration with other developers.

   .. code-block::

      # file: setup.cfg
      [options]
      install_requires =
          orquestra-sdk>=0.51.0
          torch~=2.0


   .. literalinclude:: ../examples/tests/test_dependency_installation.py
       :start-after: def good_practice_github_import():
       :end-before: </snippet>
       :language: python
       :dedent: 8


#. ``GitImportWithAuth`` as ``source_import=`` with no ``dependency_imports=`` and specify the project deps in a ``setup.cfg`` file.
   Safety hatch if (3) is not enough, e.g. if you need a different authorization mechanism than HTTPS.


   .. code-block::

      # file: setup.cfg
      [options]
      install_requires =
          orquestra-sdk>=0.51.0
          torch~=2.0


   .. literalinclude:: ../examples/tests/test_dependency_installation.py
       :start-after: def good_practice_git_import_with_auth():
       :end-before: </snippet>
       :language: python
       :dedent: 8


Source vs Dependency
====================

The ``@sdk.task`` decorator accepts two types of import arguments---source and dependency.
Both accept any of the import types specified here.
The importer passed to ``source_import=`` should provide the source code for locating the task definition, while external dependencies should be specified in ``dependency_imports=``.

.. note::
   The ``source`` vs ``dependency`` import distinction used to be more relevant in the past.
   It's possible they're going to be unified in the future.


Imports In Detail
=================


``InlineImport``
----------------

``InlineImport`` instructs the Workflow SDK to take the decorated function, serialize it, and embed in the workflow definition itself.

At workflow execution time, the function is deserialized and executed.

.. literalinclude:: ../examples/tests/test_dependency_installation.py
    :start-after: def good_practice_defaults():
    :end-before: </snippet>
    :language: python
    :dedent: 8


.. literalinclude:: ../examples/tests/test_dependency_installation.py
    :start-after: def simple_task_explicit():
    :end-before: </snippet>
    :language: python
    :dedent: 8


.. warning::
   Do not hardcode any confidential information in the task code.
   ``InlineImport`` can leak it.
   Please use :doc:`Orquestra Secrets <../tutorials/secrets>` instead.


``PythonImports``
-----------------

``PythonImports`` handles the importing of Python packages, specifically from PyPI.

The required packages can be specified as arguments, or listed in a ``requirements.txt`` file specified by the ``file`` argument.


.. literalinclude:: ../examples/tests/test_dependency_installation.py
    :start-after: def python_imports():
    :end-before: </snippet>
    :language: python
    :dedent: 8

.. warning::
    Take care when declaring ``orquestra-sdk`` as a dependency!
    ``PythonImports`` allows you to install any available python package in the environment that executes the task.
    Consequently, it is possible to create a situation where the environment that executes the task and the environment that submits a task have different versions of ``orquestra-sdk`` installed.
    This will cause workflow runs to fail!



``GithubImport`` With A Private Repo
------------------------------------

``GithubImport`` instructs the remote runtime to load code from a git repo on GitHub.

Access Token
~~~~~~~~~~~~

Accessing private git repos requires setting up an access token.
To use this functionality, the following steps must be carried out:

#. Create a personal access token (PAT) in GitHub with permission to access the private repo.
   See `GitHub docs <https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-fine-grained-personal-access-token>`_ for more details.
#. Create a new secret in Orquestra Portal containing the PAT (open ``<cluster>.orquestra.io`` in your web browser).
   For this example we have named our secret "my_pat".
   Alternatively, you can also create the secret :doc:`using Python <../tutorials/secrets>`.
#. Use the name of the Orquestra Portal secret to specify the ``personal_access_token`` argument for your import and use the ``username`` argument to supply your GitHub account username.


Packaging
~~~~~~~~~

The following example shows a source import from a (fictional) repo located at ``https://github.com/zapatacomputing/my_source_repo``.
The remote runtime clones the repo and pip-installs the project when the workflow is being executed.

.. warning::
   Every Python file you want to use with ``GithubImport`` needs to be part of a package.
   Just committing the file to a git repo is not enough!
   Visit `Setuptools user guide <https://setuptools.pypa.io/en/latest/userguide/quickstart.html>`_ for more information about setting up the package manifest for your repo.

.. code-block::

   # file: setup.cfg
   [options]
   install_requires =
       orquestra-sdk>=0.51.0
       torch~=2.0


.. literalinclude:: ../examples/tests/test_dependency_installation.py
    :start-after: def github_import_private_repo():
    :end-before: </snippet>
    :language: python
    :dedent: 8

The PAT is retrieved from Orquestra Portal at run time, and installation of the import proceeds as usual.



``GithubImport`` With A Public Repo
-----------------------------------

If the repo is publicly available, the ``username`` and ``personal_access_token`` can be omitted.
You also don't have to set up the Personal Access Token in the GitHub UI.

The following snippet shows the minimal usage.
The ``main`` branch will be used if you don't specify ``git_ref``.

.. literalinclude:: ../examples/tests/test_dependency_installation.py
    :start-after: def github_import_public_repo():
    :end-before: </snippet>
    :language: python
    :dedent: 8


``GitImportWithAuth``
---------------------

For sources or dependencies stored in a git repo that is hosted somewhere other than GitHub, the ``GitImportWithAuth`` can be used.
The ``sdk.Secret`` should be configured similarly as for ``GithubImport``.


.. literalinclude:: ../examples/tests/test_dependency_installation.py
   :start-after: def good_practice_git_import_with_auth():
   :end-before: </snippet>
   :language: python
   :dedent: 8


As in the case of ``GithubImport``, the code imported from the repo is pip installed at execution time.


Deprecated Imports
==================

These imports have been used in the past, but are not relevant anymore:

#. ``GitImport(...)``
#. ``GitImport.infer(...)``
#. ``LocalImport()``
