################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################
include subtrees/z_quantum_actions/Makefile

# We need to override test commands in 'make test' and 'make coverage' to
# specify PYTHONPATH & install required deps. This is needed to test scripts in
# "./bin".

# (override)
test:
	PYTHONPATH="." $(PYTHON) -m pytest \
		-m "not needs_separate_project" \
		--ignore=tests/runtime/performance \
		--ignore=tests/sdk/v2/typing \
		--durations=10 \
		docs/examples/tests \
		tests


# Option explanation:
# - '--cov=src' - turn on measuring code coverage. It outputs the results in a
#    '.coverage' binary file. We're not using it, but it can be input to other
#    tools like 'python -m coverage report'
# - '--cov-report xml' - in addition, generate an XML report and store it in
#    coverage.xml file. It's required to upload stats to codecov.io.
#
# (override)
coverage:
	PYTHONPATH="." $(PYTHON) -m pytest \
		-m "not needs_separate_project" \
		--cov=src \
		--cov-fail-under=$(MIN_COVERAGE) \
		--cov-report xml \
		--no-cov-on-fail \
		--ignore=tests/runtime/performance \
		--ignore=tests/sdk/v2/typing \
		--durations=10 \
		docs/examples/tests \
		tests \
		&& echo Code coverage Passed the $(MIN_COVERAGE)% mark!

# Reads the code coverage stats from '.coverage' file and prints a textual,
# human-readable report to stdout.
#
# (no override)
show-coverage-text-report:
	$(PYTHON) -m coverage report --show-missing

BASE_BRANCH := $(if $(GITHUB_BASE_REF),$(GITHUB_BASE_REF),main)
github-actions-coverage-report:
	@$(PYTHON) -m coverage report --show-missing
	@$(PYTHON) -m diff_cover.diff_cover_tool coverage.xml --compare-branch=origin/$(BASE_BRANCH)

# We need to set PATH here because performance test calls the `orq` CLI in a subprocess.
performance:
	PATH="${VENV_NAME}/bin:${PATH}" $(PYTHON) -m pytest --durations=0 tests/runtime/performance


# This is NOT mypy checking, it is ensuring the Workflow SDK has correct type hints for our users
user-typing:
	$(PYTHON) -m pytest tests/sdk/v2/typing


# (override)
github_actions:
	${PYTHON_EXE} -m venv ${VENV_NAME}
	${VENV_NAME}/${VENV_BINDIR}/${PYTHON_EXE} -m pip install --upgrade pip
	${VENV_NAME}/${VENV_BINDIR}/${PYTHON_EXE} -m pip install -e '.[dev]'

# Install deps required to build wheel. Used for release automation. See also:
# https://github.com/zapatacomputing/cicd-actions/blob/67dd6765157e0baefee0dc874e0f46ccd2075657/.github/workflows/py-wheel-build-and-push.yml#L26
#
# (override)
build-system-deps:
	$(PYTHON) -m pip install wheel


# The maketargets for codestyle we inherit from the subtree run "clean" each
# time. This is bad for local development, because it breaks the editable
# installations, and invalidates cache, so mypy takes a very long time to run.
# The temporary fix is to override these targets here, and eventually propagate
# it to the subtree repo.

# (override)
flake8:
	$(PYTHON) -m flake8 --ignore=E203,E266,F401,W503 --max-line-length=88 src tests docs/examples

# (override)
black:
	$(PYTHON) -m black --check src tests docs/examples

# (override)
isort:
	$(PYTHON) -m isort --check src tests docs/examples

# For mypy we additionally need to exclude one of the testing directories.
# (override)
mypy:
	$(PYTHON) -m mypy src tests

# (override)
style: flake8 black isort mypy
	@echo This project passes style!


# (no override)
style-fix:
	black src tests docs/examples
	isort --profile=black src tests docs/examples

# (no override)
# Run tests, but discard the ones that exceptionally slow to run locally.
test-fast:
	PYTHONPATH="." $(PYTHON) -m pytest \
		-m "not needs_separate_project and not slow" \
		--ignore=tests/runtime/performance \
		--ignore=tests/sdk/v2/typing \
		--durations=10 \
		docs/examples/tests \
		tests
