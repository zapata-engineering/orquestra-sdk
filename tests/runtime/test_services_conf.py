################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Unit tests for ORQ services configuration.
"""
import pathlib

import yaml

import orquestra.sdk._base._services_conf


def templates_path():
    return pathlib.Path(orquestra.sdk._base._services_conf.__file__).resolve().parent


def test_templates_exists():
    assert templates_path()
    assert (templates_path() / ".env").is_file()
    assert (templates_path() / "docker-compose.yaml").is_file()
    assert (templates_path() / "fluent-bit.conf").is_file()


def test_docker_compose_config():
    docker_compose_file = templates_path() / "docker-compose.yaml"
    with open(docker_compose_file) as f:
        config = yaml.load(f, yaml.BaseLoader)

    assert "fluent-bit" in config["services"]
