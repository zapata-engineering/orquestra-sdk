################################################################################
# © Copyright 2021-2023 Zapata Computing Inc.
################################################################################

# Use just "python" as the interpreter for all make tasks. It will use your
# virtual environment if you activate it before running make. You can override
# the interpreter path like:
# make test PYTHON=/tmp/other/python/version
PYTHON="python"


clean:
	find . -regex '^.*\(__pycache__\|\.py[co]\)$$' -delete;
	find . -type d -name __pycache__ -exec rm -r {} \+
	find . -type d -name '*.egg-info' -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -r {} \+
	rm -rf .pytest_cache;
	rm -rf tests/.pytest_cache;
	rm -rf dist build
	rm -f .coverage*


test:
	$(PYTHON) -m pytest \
		--ignore=tests/runtime/performance \
		--ignore=tests/sdk/typing \
		--durations=10 \
		docs/examples/tests \
		tests


# Min code-test coverage measured for the whole project required for CI checks to pass.
MIN_COVERAGE=75


# Option explanation:
# - '--cov=src' - turn on measuring code coverage. It outputs the results in a
#    '.coverage' binary file. We're not using it, but it can be input to other
#    tools like 'python -m coverage report'
# - '--cov-report xml' - in addition, generate an XML report and store it in
#    coverage.xml file. It's required to upload stats to codecov.io.
coverage:
	$(PYTHON) -m pytest \
		--cov=src \
		--cov-fail-under=$(MIN_COVERAGE) \
		--cov-report xml \
		--no-cov-on-fail \
		--ignore=tests/runtime/performance \
		--ignore=tests/sdk/typing \
		--durations=10 \
		docs/examples/tests \
		tests \
		&& echo Code coverage Passed the $(MIN_COVERAGE)% mark!

# Reads the code coverage stats from '.coverage' file and prints a textual,
# human-readable report to stdout.
show-coverage-text-report:
	$(PYTHON) -m coverage report --show-missing

BASE_BRANCH := $(if $(GITHUB_BASE_REF),$(GITHUB_BASE_REF),main)
github-actions-coverage-report:
	@$(PYTHON) -m coverage report --show-missing
	@$(PYTHON) -m diff_cover.diff_cover_tool coverage.xml --compare-branch=origin/$(BASE_BRANCH)

# We need to set PATH here because performance test calls the `orq` CLI in a subprocess.
performance:
	$(PYTHON) -m pytest --durations=0 tests/runtime/performance


# This is NOT mypy checking, it is ensuring the Workflow SDK has correct type hints for our users
user-typing:
	$(PYTHON) -m pytest tests/sdk/typing


github_actions:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e '.[dev]'

# Install deps required to build wheel. Used for release automation. See also:
# https://github.com/zapatacomputing/cicd-actions/blob/67dd6765157e0baefee0dc874e0f46ccd2075657/.github/workflows/py-wheel-build-and-push.yml#L26
.PHONY: build-system-deps
build-system-deps:
	$(PYTHON) -m pip install wheel

.PHONY: flake8
flake8:
	$(PYTHON) -m flake8 \
	--style=google \
	--arg-type-hints-in-docstring=False \
	--ignore=E203,E266,DOC301,W503 \
	--max-line-length=88 \
	src tests docs/examples

.PHONY: black
black:
	$(PYTHON) -m black --check src tests docs/examples

.PHONY: isort
isort:
	$(PYTHON) -m isort --check src tests docs/examples

.PHONY: pymarkdown
pymarkdown:
	$(PYTHON) -m pymarkdown scan CHANGELOG.md

.PHONY: mypy
mypy:
	$(PYTHON) -m mypy src tests


# Type check the project. Alternative to mypy. We'll eventually make it
# required but we need to gradually improve ourcodebase.
.PHONY:
pyright:
	$(PYTHON) -m pyright src tests

.PHONY:
style:
	@$(MAKE) pymarkdown
	@$(MAKE) flake8
	@$(MAKE) black
	@$(MAKE) isort
	@$(MAKE) mypy
	@echo This project passes style!


.PHONY:
style-fix:
	black src tests docs/examples
	isort --profile=black src tests docs/examples

# Run tests, but discard the ones that exceptionally slow to run locally.
test-fast:
	$(PYTHON) -m pytest \
		-m "not slow" \
		--ignore=tests/runtime/performance \
		--ignore=tests/sdk/typing \
		--durations=10 \
		docs/examples/tests \
		tests
