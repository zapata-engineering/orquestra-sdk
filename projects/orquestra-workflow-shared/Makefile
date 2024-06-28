################################################################################
# © Copyright 2021-2024 Zapata Computing Inc.
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
		--durations=10 \
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
		--durations=10 \
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
	src tests

.PHONY: black
black:
	$(PYTHON) -m black --check src tests

.PHONY: isort
isort:
	$(PYTHON) -m isort --check src tests

.PHONY: ruff
ruff:
	$(PYTHON) -m ruff check --preview src tests

.PHONY: pymarkdown
pymarkdown:
	$(PYTHON) -m pymarkdown scan CHANGELOG.md

.PHONY: pyright
pyright:
	$(PYTHON) -m pyright src tests

.PHONY: mypy
mypy:
	$(PYTHON) -m mypy src tests

.PHONY: style
style:
	@$(MAKE) pymarkdown
	@$(MAKE) ruff
	@$(MAKE) flake8
	@$(MAKE) black
	@$(MAKE) isort
	@$(MAKE) pyright
	@$(MAKE) mypy
	@echo This project passes style!


.PHONY: style-fix
style-fix:
	black src tests
	isort --profile=black src tests
	ruff check --preview --fix src tests

# Run tests, but discard the ones that exceptionally slow to run locally.
.PHONY: test-fast
test-fast:
	$(PYTHON) -m pytest \
		-m "not slow" \
		--durations=10 \
		tests


SPHINX_OPTS = "-W"


.PHONY: docs
docs:
	sphinx-build ./docs "$(DOCS_BUILD_DIR)" $(SPHINX_OPTS)
