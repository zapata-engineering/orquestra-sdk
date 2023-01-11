################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import importlib.metadata
import re
import typing as t
from functools import singledispatchmethod

from orquestra.sdk.schema import ir, yaml_model

from .. import serde
from . import _ids

POSSIBLE_IMPORTS = (ir.PythonImports, ir.GitImport)
ORQ_SDK_URL = "git@github.com:zapatacomputing/orquestra-workflow-sdk.git"

# From https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string  # noqa:E501
# with addition of `\.` before prerelease to match setuptools_scm format
SEMVER_REGEX = r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:[-\.](?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"  # noqa:E501


class ImportTranslator:
    """
    Translates "Orquestra imports" from IR (orquestra.sdk.schema.ir) to YAML
    (orquestra.sdk.schema.yaml_model).

    All the computation is done in the __init__, including interacting with git repos
    in a subprocess to determine the versions of SDK libraries.
    """

    def __init__(
        self,
        ir_imports: t.Mapping[ir.ImportId, ir.Import],
        orq_sdk_git_ref: t.Optional[str],
    ):
        # Doing all the computation inside __init__ makes this class independent on the
        # order of calling its methods. Yay!

        # 1. Ensure the imports include orquestra-sdk. They're
        # needed to execute tasks on QE.

        # IR imports extended to include orquestra-sdk.
        extended_ir_imports = dict(ir_imports)

        # Ensuring orq-sdk
        if (
            sdk_import := _find_git_import(ir_imports.values(), url=ORQ_SDK_URL)
        ) is None:
            sdk_import = ir.GitImport(
                id="__orq_sdk",
                repo_url=ORQ_SDK_URL,
                git_ref=orq_sdk_git_ref or _get_package_version_tag("orquestra-sdk"),
                type="GIT_IMPORT",
            )
            extended_ir_imports[sdk_import.id] = sdk_import

        # 2. Generate short IDs
        name_ir_imps = _ids.generate_yaml_ids(extended_ir_imports.values())

        # 3. Translate
        self._yaml_imports = {
            name: yaml_import
            for name, imp in name_ir_imps.items()
            if (yaml_import := self._translate_Import(imp, name)) is not None
        }

        # 4. Get the mapping
        self._ir_yaml_id_map = {imp.id: name for name, imp in name_ir_imps.items()}

        # 5. Save the import names for orq-sdk and orq-wf. This is needed in other
        # translators.
        self._orq_sdk_import_name = self._ir_yaml_id_map[sdk_import.id]

    @property
    def yaml_imports(self) -> t.Mapping[yaml_model.ImportName, yaml_model.Import]:
        return self._yaml_imports

    @property
    def ir_yaml_id_map(self) -> t.Mapping[ir.ImportId, yaml_model.ImportName]:
        return self._ir_yaml_id_map

    @property
    def orq_sdk_import_name(self) -> yaml_model.ImportName:
        return self._orq_sdk_import_name

    # --- private methods ---

    @singledispatchmethod
    def _translate_Import(
        self, imp: ir.Import, yaml_name: yaml_model.ImportName
    ) -> t.Optional[yaml_model.Import]:  # pragma: no cover
        raise TypeError(f"Unsupported import type: {imp}")

    @_translate_Import.register
    def _translate_GitImport(self, imp: ir.GitImport, yaml_name) -> yaml_model.Import:
        return yaml_model.Import(
            name=yaml_name,
            type="git",
            parameters=yaml_model.GitImportParameters(
                repository=imp.repo_url,
                commit=imp.git_ref,
            ),
        )

    @_translate_Import.register
    def _translate_InlineImport(
        self, imp: ir.InlineImport, yaml_name
    ) -> t.Optional[yaml_model.Import]:
        # In QE, inline imports are just inlined pickles inside "step". There's no
        # object in the "imports" section.
        return None

    @_translate_Import.register
    def _translate_PythonImports(
        self, imp: ir.PythonImports, yaml_name
    ) -> yaml_model.Import:
        return yaml_model.Import(
            name=yaml_name,
            type="python-requirements",
            parameters=yaml_model.PythonImportParameters(
                requirements="\n".join(
                    [serde.stringify_package_spec(spec) for spec in imp.packages]
                )
            ),
        )


def _find_git_import(
    imports: t.Iterable[ir.Import], url: str
) -> t.Optional[ir.GitImport]:
    it: t.Iterable[t.Any] = imports
    it = filter(lambda imp: isinstance(imp, ir.GitImport), it)
    it = filter(lambda imp: imp.repo_url == url, it)
    return next(it, None)


class PackageVersionError(Exception):
    def __init__(self, msg: str, package_name: str, version: t.Optional[str] = None):
        self.package_name = package_name
        self.version = version
        super().__init__(msg)


def _get_package_version_tag(package_name: str) -> str:
    installed_version = importlib.metadata.version(package_name)
    # We're following the setuptools_scm approach. see:
    # https://github.com/pypa/setuptools_scm/blob/ae62aab5bc99fe7c21bab6d0640313f615b437a0/README.rst#default-versioning-scheme=
    # A "tag" is a normal semver `major.minor.patch` version number and is what we are
    # considering a valid "package version".
    # If we don't match a semver regex, fail.
    # If there is any "distance" or the repo is "dirty", then we fail.
    match = re.match(SEMVER_REGEX, installed_version)
    if match is None:
        raise PackageVersionError(
            "Version string doesn't match expected SemVer.",
            package_name,
            installed_version,
        )
    groups = match.groupdict()
    if groups["prerelease"] is not None:
        raise PackageVersionError(
            "There have been revisions since the last tagged version.",
            package_name,
            installed_version,
        )
    if groups["buildmetadata"] is not None:
        raise PackageVersionError(
            "The workdir is not clean.",
            package_name,
            installed_version,
        )
    # If we're here, we have a SemVer major.minor.patch version!
    # The tag is prefixed with "v"
    return f"v{installed_version}"
