################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from typing import Callable, Iterable, Optional, Union

import orquestra.sdk as sdk
from orquestra.sdk.schema.ir import Import, Resources, TaskDef


class OrqPool:
    def __init__(
        self,
        config: str,
        workspace_id: Optional[str] = None,
        project_id: Optional[str] = None,
        resources: Optional[Resources] = None,
        dependency_imports: Union[Iterable[Import], Import, None] = None,
    ):
        self.config: str = config
        self.workspace_id: Optional[str] = workspace_id
        self.project_id: Optional[str] = project_id
        self.resources: Optional[Resources] = resources
        self.dependency_imports: Union[
            Iterable[Import], Import, None
        ] = dependency_imports

    def map(
        self, function: Callable, iterable: Iterable, chunksize: Optional[int] = None
    ):
        # Convert the callable function into a task definition
        task: TaskDef = sdk.task(
            function,
            source_import=sdk.InlineImport(),
            dependency_imports=self.dependency_imports,
            # resources=self.resources,
        )

        @sdk.workflow()
        def wf():
            return [task(x) for x in iterable]

        return wf().run(
            self.config, workspace_id=self.workspace_id, project_id=self.project_id
        )


config = "prod-d"
dependency_imports = None
resources = sdk.Resources()

# ======================================================================================
# Current SDK version


@sdk.task(
    source_import=sdk.InlineImport(),
    dependency_imports=dependency_imports,
    # resources=resources,
)
def my_decorated_func(x):
    return x * x


@sdk.workflow()
def my_decorated_workflow():
    return [my_decorated_func(x) for x in [1, 2, 3, 4]]


decorated_wf = my_decorated_workflow().run(config)


# ======================================================================================
# Simplified version


def my_func(x):
    return x * x


orq_pool = OrqPool(config, dependency_imports=dependency_imports)
undecorated_wf = orq_pool.map(my_func, [1, 2, 3, 4])

# ======================================================================================
# Comparison

results_decorated = decorated_wf.get_results(wait=True)
results_undecorated = undecorated_wf.get_results(wait=True)


print("Full workflow run results:", results_decorated)
print("Simplified run results:   ", results_undecorated)
assert results_undecorated == results_decorated
# ======================================================================================
