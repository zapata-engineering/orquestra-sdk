# Contributing to Orquestra Workflow SDK

🎶 Thanks so much for your interest in contributing to Orquestra! 🎶

We at Zapata's quantum software team value contributions to Orquestra, so we created this document to guide you from planning your code all the way to making a PR. But if you have more questions, please feel free to email our engineers [Alex](mailto:alexander.juda@zapatacomputing.com) or [James](mailto:james.clark@zapatacomputing.com) for some guidance.

## Code of Conduct

Contributors to the Orquestra project are expected to follow the Orquestra [code of conduct](https://github.com/zapatacomputing/orquestra-core/blob/main/CODE_OF_CONDUCT.md). Please report violations to either of our engineers listed above.

## How to Contribute

### Choosing an Issue

For new contributors, the good-first-issue tag is usually a great place to start!

### Installation

After cloning the repository, run `pip install -e .[dev]` from the main directory to install the development version. Note that in certain environments you might need to add quotes: `pip install -e '.[dev]'`. This command will ensure you have all the dependencies you need in order to contribute to Orquestra.

### Commits

Zapata's QS team uses the [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/#specification) style for commit messages so that we can automatically generate release notes.

### Comments

We use [Google-style](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html) docstring format. If you'd like to specify types please use [PEP 484](https://www.python.org/dev/peps/pep-0484/) type hints instead adding them to docstrings.

### Style

Orquestra contributors are required to use `black`, `flake8`, `mypy` and `isort` to run automated checks for code style. Make sure to install the development version (described above) so you can run these commands using the `make style` command or [pre-commit hooks](https://pre-commit.com/).

### Tests

All new contributions to Orquestra should be accompanied by deterministic unit tests. This means each test should focus on a specific aspect of the new code and shouldn't include any probabilistic elements. If you are unsure how to remove randomness in your tests, mention it in your PR, and we'll be happy to assist. 💪

Tests for this project can be run using the `make coverage` or `pytest tests` commands from the main directory.

### Making a PR

Before submitting your PR, run `make muster` from the main directory. This ensures your code will pass tests and style once submitted.

When your code passes muster, ensure your changes are on a new branch whose name describes the issue you are solving.

Then you can make a PR directly to the `main` branch by filling out the PR submission template and clicking create pull request.

When you make your PR, GitHub will automatically run style and test checks on different Python versions using [Github Actions](.github/workflows/style.yml). Your PR should automatically request the [CODEOWNERS](CODEOWNERS) to review, but if this hasn't happened, request review from `zapatacomputing/sdk-team`.

Once you have made the requested changes and your PR is approved, you can click merge and delete your branch. 🎉

### CLA

After making your PR, email Alex or James (emails above) so that we can send you a copy of our CLA. If you wish to view the CLA before contributing, our you can find [here](https://github.com/zapatacomputing/orquestra-core/blob/main/docs/_static/ZapataCLA.pdf).

### Bug Reporting

If you'd like to report a bug/issue please create a new issue in this repository.

## Code Guidelines

### Purpose of These Guidelines

The orquestra-sdk repository is utilized within both client and server contexts. 
To streamline the development and release processes, we organize code functionalities 
into subdirectories.

### Context

"The orquestra-sdk repository is installed via pip by users on their local machines. 
Additionally, it is installed on our CE backend when running workflows. 
Due to frequent issues with version mismatches, a strict interface between the client 
and server is necessary. This approach allows us to decouple our PyPi releases of 
this repository from server-side releases."

### Where Should I Place My Code?

This monorepo has the following packages:
 - `projects/orquestra-sdk`
 
Inside `orquestra-sdk`, there are three main directories within the codebase:

- _shared
- _runtime
- _client

The remaining directories serve as pure shims with the sole purpose of exposing the API to the public.

The primary guideline for determining where to write your code involves asking yourself two questions:

1. Will my code be used on the server side or the client side?
   - Considerations:
        - Will my code be utilized by the CE backend?
        - Is my code exclusively for end-users installing this package?
2. Does my code involve an interface between the client and server?
   - Considerations:
        - If someone modifies your code in the future, will we need to synchronize versions between `pip install`'ed `orquestra-sdk` and CE?
        - Can we release a new PyPi version without the need for a corresponding server-side release?"


If your code is exclusively for the client side, place it in the _client directory.
If your code is intended for the server side and does not contain interfaces (such as shared JSON models), place it in the _runtime directory.
If your code involves an interface that must match between the client and server, utilize the _shared directory.

## Ending Note 🎵

Thanks again for contributing!

We're hiring! So check out our job listings on our [website](https://www.zapatacomputing.com/quantum-computing-careers/)!

🔥 Can't wait to see your PRs 🔥
