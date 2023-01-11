################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
"""Schema that reflects syntax of yaml workflows used in Orquestra v1.

Based on Quantum Engine docs:
- https://docs.orquestra.io/quantum-engine/workflow-basics/
- https://docs.orquestra.io/quantum-engine/steps/
- https://docs.orquestra.io/quantum-engine/advanced-workflows/

Class & field names are taken from the docs whenever possible.
"""


import typing as t
from enum import Enum

from pydantic import BaseModel


class ImportType(str, Enum):
    GIT = "git"
    PYTHON = "python-requirements"


class GitImportParameters(BaseModel):
    repository: str
    # One of the following options are required:
    # The commit that introduced this validation in QE is:
    # https://github.com/zapatacomputing/quantum-engine/commit/6ed0c2c999c38acb579b694bf56f96756b06af89  # noqa: E501
    branch: t.Optional[str] = None
    commit: t.Optional[str] = None
    tag: t.Optional[str] = None


class PythonImportParameters(BaseModel):
    requirements: str


ImportName = str

ImportParameters = t.Union[GitImportParameters, PythonImportParameters]


class Import(BaseModel):
    name: ImportName
    type: ImportType
    parameters: ImportParameters


class RuntimeLanguage(str, Enum):
    PYTHON3 = "python3"


class RuntimeParameters(BaseModel):
    file: str
    function: str


class Runtime(BaseModel):
    language: RuntimeLanguage
    # if None, QE will use "zapatacomputing/z-quantum-default:latest" by default
    customImage: t.Optional[str]
    imports: t.List[ImportName]
    parameters: RuntimeParameters


class Resources(BaseModel):
    cpu: t.Optional[str] = None
    memory: t.Optional[str] = None
    disk: t.Optional[str] = None
    # The gpu field specifies the number of GPU computing units
    gpu: t.Optional[str] = None


class DataAggregation(BaseModel):
    run: t.Optional[bool] = None
    resources: t.Optional[Resources] = None


class StepConfig(BaseModel):
    runtime: Runtime
    resources: t.Optional[Resources] = None


# Type name provided by the workflow creator
ArtifactType = str

# At the time of writing this mypy doesn't support recursive types. See status at
# https://github.com/python/mypy/issues/731
ArtifactLiteral = t.Union[
    str,
    int,
    float,
    # this this would be t.List[ArtifactLiteral] if mypy supported it
    t.List,
    # this would be t.Dict[str, ArtifactLiteral] if mypy supported it
    t.Dict[str, t.Any],
]

# Reference to another step's artifact with a special syntax ((STEP_NAME.OUTPUT_NAME))
# like `((greeting.welcome))`
ArtifactRef = str

# A special kind of dict with two key-value pairs:
# 1. <function parameter name>: t.Union[ArtifactLiteral, ArtifactRef]
# 2. "type": ArtifactType
StepInput = t.Dict[str, t.Any]


class StepOutput(BaseModel):
    # Contrary to inputs, in step outputs the set of keys doesn't depend on user's
    # choice.
    name: str
    type: ArtifactType


# Step names need to be valid Kubernetes label names. The only allowed
# characters are "[a-zA-Z0-9-]". See more details at:
# https://kubernetes.io/docs/concepts/overview/working-with-objects/names
# Unfortunately using pydantic's constr() would make our code mypy-incompatible.
StepName = str


class Step(BaseModel):
    name: StepName
    config: StepConfig
    inputs: t.Optional[t.List[StepInput]] = None
    outputs: t.Optional[t.List[StepOutput]] = None
    passed: t.Optional[t.List[StepName]] = None


# Workflow names need to be valid Kubernetes subdomain names. The only allowed
# characters are "[a-zA-Z0-9-.]". See more details at:
# https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
# Unfortunately using pydantic's constr() would make our code mypy-incompatible.
WorkflowName = str


class Workflow(BaseModel):
    apiVersion: str
    name: WorkflowName
    imports: t.List[Import]
    steps: t.List[Step]
    types: t.List[ArtifactType]
    dataAggregation: t.Optional[DataAggregation]
