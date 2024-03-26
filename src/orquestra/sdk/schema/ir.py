################################################################################
# © Copyright 2021 - 2024 Zapata Computing Inc.
################################################################################
"""Workflow Intermediate Representation.

The classes here are used only for purposes of schema definition. Every data
structure here is JSON-serializable.
"""

import enum
import typing as t
import warnings

import pydantic
from typing_extensions import Annotated

from .._base._storage import BaseModel, GpuResourceType, field_validator

ImportId = str
SecretNodeId = str


class SecretNode(BaseModel):
    """A reference to a secret stored in an external secret/config service."""

    # Workflow-scope unique ID used to refer from task invocations
    id: SecretNodeId

    # Serialized value
    secret_name: str

    # Secret config
    # This is only used locally, and we expect this to be None (or ignored) on remote
    # runtimes.
    secret_config: t.Optional[str] = None

    # Workspace ID
    # This is used locally and remotely to get a secret from a specific workspace
    workspace_id: t.Optional[str] = None


class GitURL(BaseModel):
    original_url: str
    protocol: str
    user: t.Optional[str] = None
    password: t.Optional[SecretNode] = None
    host: str
    port: t.Optional[int] = None
    path: str
    query: t.Optional[str] = None


class GitImport(BaseModel):
    id: ImportId
    repo_url: GitURL
    git_ref: str

    # we need this in the JSON to know which class to use when deserializing
    type: t.Literal["GIT_IMPORT"] = "GIT_IMPORT"

    @field_validator("repo_url", mode="before")
    def _backwards_compatible_repo_url(cls, v):
        """Allows older models with a string URL to be imported."""
        # Prevent circular imports
        from .._base._git_url_utils import parse_git_url

        if not isinstance(v, str):
            return v

        return parse_git_url(v)


class LocalImport(BaseModel):
    """Used to specify that the source code is only available locally.

    (e.g. not committed to any git repo).
    """

    id: ImportId

    # we need this in the JSON to know which class to use when deserializing
    type: t.Literal["LOCAL_IMPORT"] = "LOCAL_IMPORT"


class InlineImport(BaseModel):
    id: ImportId
    type: t.Literal["INLINE_IMPORT"] = "INLINE_IMPORT"


class PackageSpec(BaseModel):
    # noqa E501
    """Representation of single package import.

    The fields in this class are based on:
    https://packaging.pypa.io/en/latest/requirements.html#packaging.requirements.Requirement

    Look there for more information about valid string values.
    """

    name: str
    extras: t.List[str]
    version_constraints: t.List[str]
    environment_markers: str


class PythonImports(BaseModel):
    """List of imports for given task."""

    id: ImportId

    # List of pip packages
    packages: t.List[PackageSpec]
    # List of pip options to put at start of the requirements
    pip_options: t.List[str]

    type: t.Literal["PYTHON_IMPORT"] = "PYTHON_IMPORT"


# If we need more import types, add them here.
Import = t.Union[GitImport, LocalImport, PythonImports, InlineImport]


TaskDefId = str


class ModuleFunctionRef(BaseModel):
    # Required to dereference function for execution.
    module: str
    function_name: str

    # Needed by Orquestra Studio to power jump-to-definition.
    file_path: t.Optional[str] = None
    line_number: t.Optional[int] = None

    # We need this in the JSON to know which class to use when deserializing
    type: t.Literal["MODULE_FUNCTION_REF"] = "MODULE_FUNCTION_REF"


class FileFunctionRef(BaseModel):
    # Required to dereference function for execution.
    file_path: str
    function_name: str

    # Needed by Orquestra Studio to power jump-to-definition. `file_path` can
    # be used for both execution and jump-to-definition.
    line_number: t.Optional[int] = None

    # We need this in the JSON to know which class to use when deserializing
    type: t.Literal["FILE_FUNCTION_REF"] = "FILE_FUNCTION_REF"


class InlineFunctionRef(BaseModel):
    function_name: str
    # Required to dereference function for execution. The function object is serialized
    # using `dill`, base64-encoded, and chunked to workaround JSON string length limits.
    encoded_function: t.List[str]

    # We need this in the JSON to know which class to use when deserializing
    type: t.Literal["INLINE_FUNCTION_REF"] = "INLINE_FUNCTION_REF"


FunctionRef = t.Union[ModuleFunctionRef, FileFunctionRef, InlineFunctionRef]


class Resources(BaseModel):
    cpu: t.Optional[str] = None
    memory: t.Optional[str] = None
    disk: t.Optional[str] = None
    gpu: GpuResourceType = None

    # nodes should be a positive integer representing the number of nodes assigned
    # to a workflow. If None, the runtime will choose.
    # This only applies to workflows and not tasks.
    nodes: t.Optional[int] = None


class DataAggregation(BaseModel):
    run: t.Optional[bool] = None
    resources: t.Optional[Resources] = None


ParameterName = str


class ParameterKind(str, enum.Enum):
    # Currently this uses the same naming as Python.
    # We only use a subset of Python's kinds, however.
    # See: https://docs.python.org/3/library/inspect.html#inspect.Parameter.kind
    POSITIONAL_OR_KEYWORD = "POSITIONAL_OR_KEYWORD"
    VAR_POSITIONAL = "VAR_POSITIONAL"
    VAR_KEYWORD = "VAR_KEYWORD"


class TaskParameter(BaseModel):
    name: ParameterName
    kind: ParameterKind
    # If we need more metadata related to parameters, like type hints or default values,
    # it should be added here.


class TaskOutputMetadata(BaseModel):
    """Information about the data shape returned by a task function."""

    # If yes, it's possible to unpack the output in the workflow like:
    # foo, bar = my_task()
    #
    # Separate from `n_outputs` to handle cases like:
    # foo, = my_task()
    is_subscriptable: bool

    # Number of artifacts we can populate with the task results.
    n_outputs: int


class TaskDef(BaseModel):
    # workflow-unique ID used to refer from task invocations
    id: TaskDefId

    fn_ref: FunctionRef

    # `source_import_id` & `dependency_import_ids` are references to imports defined in
    # Workflow.imports.

    # List of abstract inputs that the task requires, but without the values yet.
    # Kinda like function signature.
    # None means we do not know the parameters for this Task (e.g. an external task)
    # An empty list [] means a Task with no parameters
    parameters: t.Optional[t.List[TaskParameter]] = None

    # Statically inferred from the task function. See also `TaskOutputMetadata`'s
    # docstring.
    # 'None' for IRs generated with orquestra-sdk<=0.45.1. Not empty for newer SDK
    # versions.
    output_metadata: t.Optional[TaskOutputMetadata] = None

    # ID of the import that contains the callable function
    source_import_id: ImportId

    # IDs of the imports that are needed at runtime to allow running the callable
    dependency_import_ids: t.Optional[t.List[ImportId]] = None

    resources: t.Optional[Resources] = None

    max_retries: t.Optional[int] = None
    # Hints the runtime to run this task in a docker container with this image. Has no
    # effect if the runtime doesn't support it.
    custom_image: t.Optional[str] = None


class ArtifactFormat(str, enum.Enum):
    """List of formats we allow for serializing constants & artifacts."""

    # Try to figure out best serialization format based on object type at run time.
    AUTO = "AUTO"

    # Pickle -> Base64 string
    ENCODED_PICKLE = "ENCODED_PICKLE"

    # The artifact is serialized to a JSON string. Artifact value must be
    # JSON-serializable.
    JSON = "JSON"

    # Unsupported yet. This is for demonstration purposes only at the moment.
    NUMPY_ARRAY = "NUMPY_ARRAY"


TaskInvocationId = str
TaskNodeId = str
ArtifactNodeId = str
ConstantNodeId = str


class ArtifactNode(BaseModel):
    # Workflow-scope unique ID used to refer from task invocations. If the task has
    # multiple outputs they will have distinct `id`s.
    id: ArtifactNodeId

    # artifact metadata below

    # Optional name that can be used if human-readable strings are needed. If present,
    # it's an addition to `id`, doesn't replace it.
    custom_name: t.Optional[str] = None

    # Tells runtime how to serialize artifact value after task invocation returns it.
    # At the moment it's unknown where exactly the artifacts will be persisted, but some
    # serialization will likely be needed somewhere.
    serialization_format: ArtifactFormat = ArtifactFormat.AUTO

    # Index of the variable when destructuring task result in the workflow function. If
    # the workflow contained `foo, bar = my_task()`, `foo`'s index is 0 and `bar`'s
    # index is 1. This is used by some runtimes to extract the artifact value from the
    # output tuple.
    #
    # `None` if the task result isn't destructured in the workflow function.
    artifact_index: t.Optional[int] = None


class ConstantNodeJSON(BaseModel):
    """Piece of data that already exists at workflow submission time.

    The value is directly embedded in the workflow. To support arbitrary data shapes we
    keep the value in a serialized form. This Node contains only JSON-serializable
    values.
    """

    # Workflow-scope unique ID used to refer from task invocations
    id: ConstantNodeId

    # Serialized value
    value: str
    serialization_format: t.Literal[ArtifactFormat.JSON] = ArtifactFormat.JSON

    # Human-readable string that can be rendered on the UI to represent the value.
    value_preview: pydantic.constr(max_length=12)  # type: ignore


class ConstantNodePickle(BaseModel):
    """Piece of data that already exists at workflow submission time.

    The value is directly embedded in the workflow. To support arbitrary data shapes we
    keep the value in a serialized form. Pickling constants is a fallback for storing
    values that are not JSON-serializable.
    """

    # Workflow-scope unique ID used to refer from task invocations
    id: ConstantNodeId

    # Serialized value
    chunks: t.List[str]
    serialization_format: t.Literal[
        ArtifactFormat.ENCODED_PICKLE
    ] = ArtifactFormat.ENCODED_PICKLE

    # Human-readable string that can be rendered on the UI to represent the value.
    value_preview: pydantic.constr(max_length=12)  # type: ignore


# General ConstantNode that can hold constants that are not JSON-serializable
ConstantNode = t.Union[ConstantNodeJSON, ConstantNodePickle]
# ID of a node that can be a task argument. Multiple node types can be task inputs.
# This is contrary to the outputs; only artifact nodes can be task outputs.
ArgumentId = t.Union[ArtifactNodeId, ConstantNodeId, SecretNodeId]


class TaskInvocation(BaseModel):
    id: TaskInvocationId

    # What task should be executed.
    task_id: TaskDefId

    args_ids: t.List[ArgumentId]
    # Key: task parameter name. Should match one of parameters of the `Task` referenced
    #     by `task_id`.
    # Value: id of the constant or artifact that will contain the value at runtime.
    kwargs_ids: t.Dict[ParameterName, ArgumentId]

    # Where to store the returned values.
    output_ids: t.List[ArtifactNodeId]

    # TaskInvocation specific resources
    resources: t.Optional[Resources] = None

    # Specification for custom image more scoped than TaskDef custom_image field
    # if not set, will fall back to TaskDef custom_image
    custom_image: t.Optional[str] = None


WorkflowDefName = str


class Version(BaseModel):
    original: str
    major: int
    minor: int
    patch: int
    is_prerelease: bool


class WorkflowMetadata(BaseModel):
    sdk_version: Version
    python_version: Version


class WorkflowDef(BaseModel):
    """The main data structure for intermediate workflow representation.

    The structure is as flat as possible with relation based on "id"s, e.g. a single
    task invocation has a relation to the task it invokes, argument artifacts or
    constants and returned artifacts.
    """

    name: WorkflowDefName

    # Used by Orquestra Studio to power jump-to-definition (file_path + line_number).
    fn_ref: FunctionRef

    # nodes with IDs & metadata
    imports: t.Dict[ImportId, Import]
    tasks: t.Dict[TaskDefId, TaskDef]
    artifact_nodes: t.Dict[ArtifactNodeId, ArtifactNode]
    constant_nodes: t.Dict[ConstantNodeId, ConstantNode]
    secret_nodes: t.Dict[SecretNodeId, SecretNode] = {}

    # The actual graph in form node clusters ("invocations"). Each task invocation is a
    # small subgraph with TaskNode in the center and connections to data nodes
    # (ArtifactNode or ConstantNode). This representation allows to easily specify order
    # of arguments or returned values.
    task_invocations: t.Dict[TaskInvocationId, TaskInvocation]

    # IDs of the nodes that are considered as workflow outputs. The referenced nodes
    # can be constants or artifacts.
    output_ids: t.List[ArgumentId]

    data_aggregation: t.Optional[DataAggregation] = None

    # Metadata defaults to None to allow older JSON to be loaded
    metadata: Annotated[
        t.Optional[WorkflowMetadata], pydantic.Field(validate_default=True)
    ] = None

    # The resources that are available for the workflow to use.
    # If none, the runtime will decide.
    resources: t.Optional[Resources] = None

    @field_validator("metadata", mode="after")
    def sdk_version_up_to_date(cls, v: t.Optional[WorkflowMetadata]):
        # Workaround for circular imports
        from orquestra.sdk import exceptions
        from orquestra.sdk.packaging import _versions
        from orquestra.sdk.schema import _compat

        current_version = _versions.get_current_sdk_version()

        if v is None:
            warnings.warn(
                exceptions.VersionMismatch(
                    (
                        "Attempting to read a workflow definition generated with an "
                        "old version of Orquestra Workflow SDK. Please consider "
                        "re-running your workflow or downgrading orquestra-sdk. "
                        "For more information visit: https://docs.orquestra.io/docs/core/sdk/guides/version-compatibility.html"  # noqa: E501
                    ),
                    actual=current_version,
                    needed=None,
                )
            )
            return v

        if not _compat.versions_are_compatible(
            generated_at=v.sdk_version, current=current_version
        ):
            warnings.warn(
                exceptions.VersionMismatch(
                    (
                        "Attempting to read a workflow definition generated with a "
                        "different version of Orquestra Workflow SDK. "
                        "Please consider re-running your workflow or installing "
                        f"'orquestra-sdk=={v.sdk_version.original}'. "
                        "For more information visit: https://docs.orquestra.io/docs/core/sdk/guides/version-compatibility.html"  # noqa: E501
                    ),
                    actual=current_version,
                    needed=v.sdk_version,
                )
            )

        return v
