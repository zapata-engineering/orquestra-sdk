# Orquestra Workflow SDK Monorepo

## Monorepo

This monorepo contains the different packages required to write and run workflows. Each package is located in `projects/`

The following packages are included:

- `orquestra-sdk`: the main client library for Orquestra Workflow SDK.

## What is Orquestra Workflow SDK?

`orquestra-sdk` is a Python library for expressing and executing computational workflows locally and on the [Orquestra](https://www.zapatacomputing.com/orquestra) platform.

Please see `projects/orquestra-sdk` for more information.

## Get started

Orquestra Workflow SDK is published to PyPI and should be installed from there via `pip`. This will pull in any additional packages required to write and run workflows locally and remotely.

```bash
pip install "orquestra-sdk[all]"
```

Please refer to the [Orquestra Workflow SDK docs](https://docs.orquestra.io/docs/core/sdk/).

## Bug Reporting

If you'd like to report a bug/issue please create a [new issue using one of the templates](https://github.com/zapata-engineering/orquestra-sdk/issues).

## Contributing

Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for more information on contributing to Orquestra Workflow SDK.
