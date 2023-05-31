# flake8: noqa

"""
Extracted from ``resp_mocks`` because we don't want to bother running flake8 on this.
"""
from orquestra.sdk._base._driver._models import Message, RayFilename


def make_expected_messages():
    """
    Deserialized messages that match the content of ``logs.tar.gz``.
    """
    return [
        Message(
            log=":job_id:01000000",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-01000000-249.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":actor_name:Manager",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-01000000-249.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":job_id:01000000",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-01000000-249.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":actor_name:Manager",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-01000000-249.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":job_id:01000000",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:_workflow_task_executor_remote",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,009\tINFO task_executor.py:78 -- Task status [RUNNING]\t[add_some_ints-be1OQ-r000@invocation-2-task-sum-with-oom]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:_workflow_task_executor_remote",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,028\tINFO task_executor.py:78 -- Task status [RUNNING]\t[add_some_ints-be1OQ-r000@invocation-1-task-sum-with-oom]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:_workflow_task_executor_remote",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,037\tINFO task_executor.py:78 -- Task status [RUNNING]\t[add_some_ints-be1OQ-r000@invocation-0-task-sum-with-logs]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='{"timestamp": "2023-05-24T12:02:55.043421+00:00", "level": "INFO", "filename": "_log_adapter.py:186", "message": "Adding two numbers: 30, 60", "wf_run_id": "add_some_ints-be1OQ-r000", "task_inv_id": "invocation-0-task-sum-with-logs", "task_run_id": "add_some_ints-be1OQ-r000@invocation-0-task-sum-with-logs.d0648"}',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":job_id:01000000",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:_workflow_task_executor_remote",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:_workflow_task_executor_remote",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:_workflow_task_executor_remote",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":job_id:01000000",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":actor_name:WorkflowManagementActor",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:54,120\tINFO workflow_executor.py:86 -- Workflow job [id=add_some_ints-be1OQ-r000] started.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,024\tINFO workflow_executor.py:284 -- Task status [SUCCESSFUL]\t[add_some_ints-be1OQ-r000@invocation-2-task-sum-with-oom]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,032\tINFO workflow_executor.py:284 -- Task status [SUCCESSFUL]\t[add_some_ints-be1OQ-r000@invocation-1-task-sum-with-oom]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,045\tINFO workflow_executor.py:284 -- Task status [SUCCESSFUL]\t[add_some_ints-be1OQ-r000@invocation-0-task-sum-with-logs]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":job_id:01000000",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":actor_name:WorkflowManagementActor",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,071\tINFO pip.py:286 -- Creating virtualenv at /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv, current python dir /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,072\tINFO utils.py:76 -- Run cmd[1] ['/usr/local/bin/python', '-m', 'virtualenv', '--app-data', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv_app_data', '--reset-app-data', '--no-periodic-update', '--system-site-packages', '--no-download', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv']",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,554\tINFO utils.py:97 -- Output of cmd[1]: created virtual environment CPython3.9.16.final.0-64 in 342ms",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  creator CPython3Posix(dest=/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv, clear=False, no_vcs_ignore=False, global=True)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  seeder FromAppData(download=False, pip=bundle, setuptools=bundle, wheel=bundle, via=copy, app_data_dir=/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv_app_data)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    added seed packages: pip==23.1.2, setuptools==67.7.2, wheel==0.40.0",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  activators BashActivator,CShellActivator,FishActivator,NushellActivator,PowerShellActivator,PythonActivator",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:55,554\tINFO utils.py:76 -- Run cmd[2] ['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-c', '\\nimport ray\\nwith open(r\"/tmp/check_ray_version_tempfilea5y2kca7/ray_version.txt\", \"wt\") as f:\\n    f.write(ray.__version__)\\n    f.write(\" \")\\n    f.write(ray.__path__[0])\\n                    ']",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:56,133\tINFO utils.py:99 -- No output for cmd[2]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:56,133\tINFO pip.py:197 -- try to write ray version information in: /tmp/check_ray_version_tempfilea5y2kca7/ray_version.txt",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:56,134\tINFO pip.py:335 -- Installing python requirements to /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:56,134\tINFO utils.py:76 -- Run cmd[3] ['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt']",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:57,359\tINFO utils.py:97 -- Output of cmd[3]: Collecting git+ssh://****@github.com/zapatacomputing/evangelism-workflows.git@alexj/ORQSDK-776/sample-mlflow-tracking (from -r /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt (line 1))",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  Cloning ssh://****@github.com/zapatacomputing/evangelism-workflows.git (to revision alexj/ORQSDK-776/sample-mlflow-tracking) to /tmp/pip-req-build-e11rxzx8",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  Running command git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-e11rxzx8",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  error: cannot run ssh: No such file or directory",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  fatal: unable to fork",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-e11rxzx8 did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="× git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-e11rxzx8 did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="│ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:57,359\tINFO pip.py:377 -- Delete incomplete virtualenv: /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:57,394\tERROR pip.py:379 -- Failed to install pip packages.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Traceback (most recent call last):",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 361, in _run',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await self._install_pip_packages(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 337, in _install_pip_packages',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await check_output_cmd(pip_install_cmd, logger=logger, cwd=cwd, env=pip_env)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/utils.py", line 101, in check_output_cmd',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    raise SubprocessCalledProcessError(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="ray._private.runtime_env.utils.SubprocessCalledProcessError: Run cmd[3] failed with the following details.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Command '['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt']' returned non-zero exit status 1.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Last 50 lines of stdout:",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    Collecting git+ssh://****@github.com/zapatacomputing/evangelism-workflows.git@alexj/ORQSDK-776/sample-mlflow-tracking (from -r /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt (line 1))",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      Cloning ssh://****@github.com/zapatacomputing/evangelism-workflows.git (to revision alexj/ORQSDK-776/sample-mlflow-tracking) to /tmp/pip-req-build-e11rxzx8",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      Running command git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-e11rxzx8",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      error: cannot run ssh: No such file or directory",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      fatal: unable to fork",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-e11rxzx8 did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-e11rxzx8 did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:58,397\tINFO pip.py:286 -- Creating virtualenv at /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv, current python dir /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:58,397\tINFO utils.py:76 -- Run cmd[4] ['/usr/local/bin/python', '-m', 'virtualenv', '--app-data', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv_app_data', '--reset-app-data', '--no-periodic-update', '--system-site-packages', '--no-download', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv']",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:58,866\tINFO utils.py:97 -- Output of cmd[4]: created virtual environment CPython3.9.16.final.0-64 in 335ms",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  creator CPython3Posix(dest=/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv, clear=False, no_vcs_ignore=False, global=True)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  seeder FromAppData(download=False, pip=bundle, setuptools=bundle, wheel=bundle, via=copy, app_data_dir=/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv_app_data)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    added seed packages: pip==23.1.2, setuptools==67.7.2, wheel==0.40.0",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  activators BashActivator,CShellActivator,FishActivator,NushellActivator,PowerShellActivator,PythonActivator",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:58,866\tINFO utils.py:76 -- Run cmd[5] ['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-c', '\\nimport ray\\nwith open(r\"/tmp/check_ray_version_tempfilewokh34jn/ray_version.txt\", \"wt\") as f:\\n    f.write(ray.__version__)\\n    f.write(\" \")\\n    f.write(ray.__path__[0])\\n                    ']",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:59,471\tINFO utils.py:99 -- No output for cmd[5]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:59,471\tINFO pip.py:197 -- try to write ray version information in: /tmp/check_ray_version_tempfilewokh34jn/ray_version.txt",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:59,472\tINFO pip.py:335 -- Installing python requirements to /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:02:59,472\tINFO utils.py:76 -- Run cmd[6] ['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt']",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:00,695\tINFO utils.py:97 -- Output of cmd[6]: Collecting git+ssh://****@github.com/zapatacomputing/evangelism-workflows.git@alexj/ORQSDK-776/sample-mlflow-tracking (from -r /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt (line 1))",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  Cloning ssh://****@github.com/zapatacomputing/evangelism-workflows.git (to revision alexj/ORQSDK-776/sample-mlflow-tracking) to /tmp/pip-req-build-plczpshp",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  Running command git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-plczpshp",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  error: cannot run ssh: No such file or directory",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  fatal: unable to fork",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-plczpshp did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="× git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-plczpshp did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="│ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:00,695\tINFO pip.py:377 -- Delete incomplete virtualenv: /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:00,730\tERROR pip.py:379 -- Failed to install pip packages.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Traceback (most recent call last):",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 361, in _run',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await self._install_pip_packages(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 337, in _install_pip_packages',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await check_output_cmd(pip_install_cmd, logger=logger, cwd=cwd, env=pip_env)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/utils.py", line 101, in check_output_cmd',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    raise SubprocessCalledProcessError(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="ray._private.runtime_env.utils.SubprocessCalledProcessError: Run cmd[6] failed with the following details.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Command '['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt']' returned non-zero exit status 1.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Last 50 lines of stdout:",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    Collecting git+ssh://****@github.com/zapatacomputing/evangelism-workflows.git@alexj/ORQSDK-776/sample-mlflow-tracking (from -r /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt (line 1))",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      Cloning ssh://****@github.com/zapatacomputing/evangelism-workflows.git (to revision alexj/ORQSDK-776/sample-mlflow-tracking) to /tmp/pip-req-build-plczpshp",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      Running command git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-plczpshp",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      error: cannot run ssh: No such file or directory",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      fatal: unable to fork",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-plczpshp did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-plczpshp did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:01,733\tINFO pip.py:286 -- Creating virtualenv at /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv, current python dir /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:01,733\tINFO utils.py:76 -- Run cmd[7] ['/usr/local/bin/python', '-m', 'virtualenv', '--app-data', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv_app_data', '--reset-app-data', '--no-periodic-update', '--system-site-packages', '--no-download', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv']",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:02,202\tINFO utils.py:97 -- Output of cmd[7]: created virtual environment CPython3.9.16.final.0-64 in 338ms",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  creator CPython3Posix(dest=/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv, clear=False, no_vcs_ignore=False, global=True)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  seeder FromAppData(download=False, pip=bundle, setuptools=bundle, wheel=bundle, via=copy, app_data_dir=/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv_app_data)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    added seed packages: pip==23.1.2, setuptools==67.7.2, wheel==0.40.0",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  activators BashActivator,CShellActivator,FishActivator,NushellActivator,PowerShellActivator,PythonActivator",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:02,203\tINFO utils.py:76 -- Run cmd[8] ['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-c', '\\nimport ray\\nwith open(r\"/tmp/check_ray_version_tempfilewp1acht1/ray_version.txt\", \"wt\") as f:\\n    f.write(ray.__version__)\\n    f.write(\" \")\\n    f.write(ray.__path__[0])\\n                    ']",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:02,803\tINFO utils.py:99 -- No output for cmd[8]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:02,803\tINFO pip.py:197 -- try to write ray version information in: /tmp/check_ray_version_tempfilewp1acht1/ray_version.txt",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:02,804\tINFO pip.py:335 -- Installing python requirements to /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:02,804\tINFO utils.py:76 -- Run cmd[9] ['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt']",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:04,016\tINFO utils.py:97 -- Output of cmd[9]: Collecting git+ssh://****@github.com/zapatacomputing/evangelism-workflows.git@alexj/ORQSDK-776/sample-mlflow-tracking (from -r /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt (line 1))",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  Cloning ssh://****@github.com/zapatacomputing/evangelism-workflows.git (to revision alexj/ORQSDK-776/sample-mlflow-tracking) to /tmp/pip-req-build-aemut7zh",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  Running command git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-aemut7zh",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  error: cannot run ssh: No such file or directory",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  fatal: unable to fork",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-aemut7zh did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="× git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-aemut7zh did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="│ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:04,017\tINFO pip.py:377 -- Delete incomplete virtualenv: /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:04,052\tERROR pip.py:379 -- Failed to install pip packages.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Traceback (most recent call last):",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 361, in _run',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await self._install_pip_packages(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 337, in _install_pip_packages',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await check_output_cmd(pip_install_cmd, logger=logger, cwd=cwd, env=pip_env)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/utils.py", line 101, in check_output_cmd',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    raise SubprocessCalledProcessError(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="ray._private.runtime_env.utils.SubprocessCalledProcessError: Run cmd[9] failed with the following details.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Command '['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt']' returned non-zero exit status 1.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Last 50 lines of stdout:",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    Collecting git+ssh://****@github.com/zapatacomputing/evangelism-workflows.git@alexj/ORQSDK-776/sample-mlflow-tracking (from -r /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt (line 1))",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      Cloning ssh://****@github.com/zapatacomputing/evangelism-workflows.git (to revision alexj/ORQSDK-776/sample-mlflow-tracking) to /tmp/pip-req-build-aemut7zh",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      Running command git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-aemut7zh",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      error: cannot run ssh: No such file or directory",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      fatal: unable to fork",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-aemut7zh did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-aemut7zh did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:05,055\tERROR workflow_executor.py:306 -- Task status [FAILED] due to a system error.\t[add_some_ints-be1OQ-r000@invocation-3-task-sum-git]",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="2023-05-24 12:03:05,057\tERROR workflow_executor.py:352 -- Workflow 'add_some_ints-be1OQ-r000' failed due to Failed to set up runtime environment.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Traceback (most recent call last):",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/dashboard/modules/runtime_env/runtime_env_agent.py", line 355, in _create_runtime_env_with_retry',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    runtime_env_context = await asyncio.wait_for(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/asyncio/tasks.py", line 479, in wait_for',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    return fut.result()",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/dashboard/modules/runtime_env/runtime_env_agent.py", line 310, in _setup_runtime_env',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await create_for_plugin_if_needed(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/plugin.py", line 252, in create_for_plugin_if_needed',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    size_bytes = await plugin.create(uri, runtime_env, context, logger=logger)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 473, in create',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    return await task",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 455, in _create_for_hash',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await PipProcessor(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 361, in _run',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await self._install_pip_packages(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/pip.py", line 337, in _install_pip_packages',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    await check_output_cmd(pip_install_cmd, logger=logger, cwd=cwd, env=pip_env)",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log='  File "/usr/local/lib/python3.9/site-packages/ray/_private/runtime_env/utils.py", line 101, in check_output_cmd',
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    raise SubprocessCalledProcessError(",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="ray._private.runtime_env.utils.SubprocessCalledProcessError: Run cmd[9] failed with the following details.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Command '['/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt']' returned non-zero exit status 1.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="Last 50 lines of stdout:",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    Collecting git+ssh://****@github.com/zapatacomputing/evangelism-workflows.git@alexj/ORQSDK-776/sample-mlflow-tracking (from -r /tmp/ray/session_2023-05-24_12-02-48_421340_10/runtime_resources/pip/5edb5eab4ee308ed6ad72cb27360331971af26a7/requirements.txt (line 1))",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      Cloning ssh://****@github.com/zapatacomputing/evangelism-workflows.git (to revision alexj/ORQSDK-776/sample-mlflow-tracking) to /tmp/pip-req-build-aemut7zh",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      Running command git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-aemut7zh",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      error: cannot run ssh: No such file or directory",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      fatal: unable to fork",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-aemut7zh did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="  ",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="      note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    error: subprocess-exited-with-error",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    × git clone --filter=blob:none --quiet 'ssh://****@github.com/zapatacomputing/evangelism-workflows.git' /tmp/pip-req-build-aemut7zh did not run successfully.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    │ exit code: 128",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    ╰─> See above for output.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log="    note: This error originates from a subprocess, and is likely not a problem with pip.",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ee5ed11c87d2b8c6396261ac63b5d832cbebd2fc6355a13acd341057-01000000-212.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.out"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
        Message(
            log=":task_name:load_task_output_from_storage",
            ray_filename=RayFilename(
                "/tmp/ray/session_latest/logs/worker-ea7208e2d0e663fe52567ef158ca00ba0169783997d5317c36a3cec2-01000000-292.err"
            ),
            tag="workflow.logs.ray.add_some_ints-be1OQ-r000",
        ),
    ]
