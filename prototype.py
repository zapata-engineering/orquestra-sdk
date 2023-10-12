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

    @property
    def _task_decorator_kwargs(self) -> dict:
        kwargs = {
            "source_import": sdk.InlineImport(),
        }
        if self.dependency_imports:
            kwargs["dependency_imports"] = self.dependency_imports
        if self.resources:
            kwargs["resources"] = self.resources
        return kwargs

    @property
    def _workflow_decorator_kwargs(self) -> dict:
        kwargs = {}
        return kwargs

    @property
    def _workflow_run_args(self) -> tuple:
        args = (self.config,)
        return args

    @property
    def _workflow_run_kwargs(self) -> dict:
        kwargs = {}
        if self.workspace_id:
            kwargs["workspace_id"] = self.workspace_id
        if self.project_id:
            kwargs["project_id"] = self.project_id
        return kwargs

    def map(self, function: Callable, iterable: Iterable):
        return self.map_async(function, iterable).get_results(wait=True)

    def map_async(
        self, function: Callable, iterable: Iterable, name: Optional[str] = None
    ):
        task: TaskDef = sdk.task(function, **self._task_decorator_kwargs)

        @sdk.workflow(**self._workflow_decorator_kwargs, custom_name=name)
        def wf():
            return [task(x) for x in iterable]

        return wf().run(*self._workflow_run_args, **self._workflow_run_kwargs)

    def starmap(self, function: Callable, iterable: Iterable[Iterable]):
        return self.starmap_async(function, iterable).get_results(wait=True)

    def starmap_async(
        self,
        function: Callable,
        iterable: Iterable[Iterable],
        name: Optional[str] = None,
    ):
        task: TaskDef = sdk.task(function, **self._task_decorator_kwargs)

        @sdk.workflow(**self._workflow_decorator_kwargs, custom_name=name)
        def wf():
            return [task(*x) for x in iterable]

        return wf().run(*self._workflow_run_args, **self._workflow_run_kwargs)


# ====================================== DEMO ==========================================
orq_pool = OrqPool(
    "local",
    workspace_id="my_workspace",
    project_id="my_project",
    resources=sdk.Resources(cpu=1),
    dependency_imports=[],
)


# Map
def single_input_func(x):
    return x * x


print(orq_pool.map(single_input_func, [1, 2, 3, 4]))  # prints (1,4,9,16)


# Starmap
def multiple_input_func(x, y):
    return x * y


print(
    orq_pool.starmap(multiple_input_func, [[1, 2], [3, 4], [5, 6], [7, 8]])
)  # prints (2, 12, 30, 56)

# Map async
map_async_run = orq_pool.map_async(
    single_input_func, [1, 2, 3, 4], name="map_async_run"
)
print(map_async_run.get_results(wait=True))  # prints (1,4,9,16)

# Starmap async
starmap_async_run = orq_pool.starmap_async(
    multiple_input_func, [[1, 2], [3, 4], [5, 6], [7, 8]]
)
print(starmap_async_run.get_results(wait=True))  # prints (2, 12, 30, 56)

# ======================================================================================
