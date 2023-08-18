=====================
Installing on Windows
=====================

Installing developer packages on Windows can be a challenge.
This tutorial is to assist anyone who wants to use the Workflow SDK natively on Windows.

Prerequisites
=============


1. Install Python 3.9.
   https://www.python.org/downloads/

.. note::
    Remember to add Python to your path (check the box Add Python <version> to PATH)

2. Install the Microsoft Visual C++ Redistributable package.
This is likely already installed, unless you're working on clean machine.

You can verify if the Visual C++ Redistributable package is installed either by:

* Powershell command:

.. code-block::

   Get-WmiObject -Class Win32_Product -Filter "Name LIKE '%Visual C++%'"


*  Or checking add/remove programs list:

   .. image:: images/win-redist-picture.png

-  If the Visual C++ Redistributable package is not installed, it can be downloaded from `the official Microsoft site, <https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170#visual-studio-2015-2017-2019-and-2022>`__\

Installing Orquestra Workflow SDK
=================================

You can install the Workflow SDK with:

.. code-block::

    pip install "orquestra-sdk[all]"

We highly recommended installing Orquestra Workflow SDK inside a virtual environment, which can be easily created with ``python -m venv <venv name>``.

.. warning::

    Without a virtual environment, the location of the ``orq`` command is stored is NOT in ``%PATH%``.
    If this happens, you may see this error when installing Orquestra Workflow SDK:

    .. code-block::

        The script orq.exe is installed in ‘C:\\<somepath>\\Python38\\Scripts’ which is not on PATH.

    This will prevent ``orq`` CLI from working.
    To fix this, either add above path to ``%PATH%``, or use a virtual environment which automatically add its own ``scripts/`` directory to PATH.
