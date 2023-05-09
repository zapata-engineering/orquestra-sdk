################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from orquestra import sdk


@sdk.task(
    source_import=sdk.InlineImport(),
    dependency_imports=[
        sdk.GitImport(
            repo_url="git@github.com:zapatacomputing/orquestra-workflow-sdk.git",
            git_ref="ORQSDK-823-python-api-for-reading-current-x-orquestra-id",
        ),
    ],
)
def task_1() -> list[int]:
    return sdk.get_backend_ids()


@sdk.workflow
def wf_return_single_packed_value():
    a = task_1()
    return a


# Run the wf on a bunch of runtimes to test consistency

# Try it on Ray
print("\nRAY:")
wf_r = wf_return_single_packed_value().run("ray")
try:
    print(wf_r.get_results(wait=True))
except Exception as e:
    print("-" * 80)
    print("Exception:", e)
    for line in wf_r.get_logs()["invocation-0-task-task-1"]:
        print(line)
    print("-" * 80)

# Try in on In Process
print("\nIN PROCESS:")
wf_ip = wf_return_single_packed_value().run("in_process")
try:
    print(wf_ip.get_results(wait=True))
except Exception as e:
    print("-" * 80)
    print("Exception:", e)
    for line in wf_ip.get_logs()["invocation-0-task-task-1"]:
        print(line)
    print("-" * 80)

# # Try in on QE
# print("\nQE")
# wf_qe = wf_return_single_packed_value().run("prod-d")
# try:
#     print(wf_qe.get_results(wait=True))
# except Exception as e:
#     print("-"*80)
#     print("Exception:", e)
#     for line in wf_qe.get_logs()["invocation-0-task-task-1"]:
#         print(line)
#     print("-"*80)

# # Try in on QE
# print("\nCE")
# wf_ce = wf_return_single_packed_value().run("moreau")
# try:
#     print(wf_ce.get_results(wait=True))
# except Exception as e:
#     print("-"*80)
#     print("Exception:", e)
#     for line in wf_ce.get_logs()["invocation-0-task-task-1"]:
#         print(line)
#     print("-"*80)
