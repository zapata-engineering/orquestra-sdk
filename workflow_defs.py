import orquestra.sdk as sdk
import other_file


@sdk.task
def task(*_):
    other_file.l.append(0)
    return len(other_file.l)


@sdk.workflow
def wf():
    output = task()
    for i in range(100):
        output = task(output)

    different_output = task()
    for i in range(100):
        different_output = task(different_output)

    return output, different_output


wf_run = wf().run("prod-d")
wf_run.wait_until_finished()

print(wf_run.get_results())
