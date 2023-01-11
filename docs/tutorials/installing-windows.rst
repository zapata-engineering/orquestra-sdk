=====================
Installing on Windows
=====================

Installing developer packages on Windows can be a challenge.
This tutorial is to assist anyone who wants to use the Workflow SDK
natively on Windows.

Prerequisites
=============


1. Install Python (supported versions are Python 3.8.* and 3.9.*) -
   https://www.python.org/downloads/

.. note::
    Remember to add Python to your path (check the box Add Python <version> to PATH)

2. Install the Microsoft Visual C++ Redistributable package. This is likely already installed, unless you're working on clean machine.

You can verify if the Visual C++ Redistributable package is installed either by:

* Powershell command:

.. code-block::

   Get-WmiObject -Class Win32_Product -Filter "Name LIKE '%Visual C++%'"


*  Or checking add/remove programs list:

   .. image:: images/win-redist-picture.png

-  If the Visual C++ Redistributable package is not installed, it can be downloaded from `the official
   Microsoft
   site, <https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170#visual-studio-2015-2017-2019-and-2022>`__\

.. only:: internal

    Setting up ``pip``
    ==================

    To download Orquestra Workflow SDK from Nexus, you need to setup ``pip``:

    Create pip.ini file in ~\\pip\\ and add the lines provided by Zapata.


    For Zapata internal, use these lines

    .. code-block::

        [global]
        index = https://nexus.orquestra.wtf/repository/pypi/pypi
        index-url = https://nexus.orquestra.wtf/repository/pypi/simple

    ..

    Verify that it works with the command:

    .. code-block::

        pip config debug

        > user:
        C:\Users\<user_name>\pip\pip.ini, exists: True
        global.index: <YOUR_URL>
        global.index-url: <YOUR_URL>

    ..

    This command shows if the proper file is created (exists: True/False).
    In case the path is different (not ``~/pip/pip.ini``), move the ``pip.ini`` file
    to the path shown, you may have to create the ``pip`` directory
    manually - the Python installer doesn't create it for you).

    .. warning::
        Careful with extensions! Windows by default hides extension of the file.
        If you create new text file by right-clicking in the Windows file browser →
        name it ``pip.ini`` → your file might have hidden name ``pip.ini.txt`` which
        won’t be detected by ``pip``. Running ``pip config debug``
        will show ``exists:false``. Create file using cmd, powershell or
        enable extensions in file browser.

    Log in to Nexus
    ===============

    Currently there are 2 ways to log in to Nexus:

    Either create a ~/.netrc file with the content as shown below

    .. code-block::

        machine <link to nexus>
        login <YOUR E-MAIL>
        password <YOUR PASSWORD>

    Or enter the password manually each time you use Nexus for packages.

    .. note::
        Using Git Bash on Windows is known to hang the console during log-in
        operation. There is no workaround currently. Please use either cmd
        or Powershell

Installing Orquestra Workflow SDK
=================================

You can install the Workflow SDK with:

.. code-block::

    pip install orquestra-sdk[all]

We highly recommended installing Orquestra Workflow SDK inside a virtual environment, which can be
easily created with ``python -m venv <venv name>``.

.. warning::

    Without a virtual environment, the location of the ``orq`` command is stored is NOT in ``%PATH%``.
    If this happens, you may see this error when installing Orquestra Workflow SDK:

    .. code-block::

        The script orq.exe is installed in ‘C:\\<somepath>\\Python38\\Scripts’ which is not on PATH.

    This will prevent ``orq`` CLI from working. To fix this, either add above path
    to ``%PATH%``, or use a virtual environment which automatically add its own ``scripts/`` directory to
    PATH.
