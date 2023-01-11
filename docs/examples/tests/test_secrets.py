################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Parts of this file are embedded in the "secrets" tutorial.
The snippets are extracted between "<snippet my-snippet-name></snippet>".

This file is a coarse-grained integration test. Ideally we would use end-to-end test,
but it requires more set up.

Test's boundary:
[SDK's public Python API] -> [http request]
                          -> [config.json]
"""


from unittest.mock import Mock

import pytest
import responses

import orquestra.sdk._base._config
import orquestra.sdk._base._exec_ctx
import orquestra.sdk.secrets._client
from orquestra.sdk.schema import configs


class Snippets:
    """
    Contains snippets of code that will be embedded inside the tutorial page.
    """

    @staticmethod
    def get_secret_from_repl():
        # <snippet get-secret-from-repl>
        from orquestra import sdk

        # should print: ABC123abc64534
        print(sdk.secrets.get("ibmq-token", config_name="prod-d"))
        # </snippet>

    @staticmethod
    def set_secret_from_repl():
        # <snippet set-secret-from-repl>
        from orquestra import sdk

        sdk.secrets.set("ibmq-token", "ABC123abc64534", config_name="prod-d")
        # </snippet>

    @staticmethod
    def get_secret_from_task():
        # <snippet get-secret-from-task>
        from orquestra import sdk

        def access_external_api(api_token: str):
            # [send an http request to some external system]

            # Only for demo purposes. Normally, you shouldn't print your API tokens.
            print(api_token)

        @sdk.task
        def use_ext_api():
            token: str = sdk.secrets.get("ibmq-token", config_name="prod-d")
            # At this point, 'token' is a regular string. You can pass it to other
            # functions that require credentials.
            access_external_api(api_token=token)
            ...

        # </snippet>

        # Used in the tests to invoke the task code
        return use_ext_api


class TestSecrets:
    @staticmethod
    @pytest.fixture
    def base_uri():
        return "http://example.com"

    @staticmethod
    @pytest.fixture
    def secret_value():
        # This is assumed to match the string used inside ``Snippets`` class.
        return "ABC123abc64534"

    @staticmethod
    @pytest.fixture
    def config_entry(monkeypatch, base_uri):
        monkeypatch.setattr(
            orquestra.sdk._base._config,
            "read_config",
            Mock(
                return_value=configs.RuntimeConfiguration(
                    config_name="mocked",
                    runtime_name=configs.RuntimeName.QE_REMOTE,
                    runtime_options={"uri": base_uri, "token": "mocked_token"},
                )
            ),
        )

    @staticmethod
    @pytest.fixture
    def mocked_responses():
        # This is the place to tap into response mocking machinery, if needed.
        with responses.RequestsMock() as mocked_responses:
            yield mocked_responses

    @staticmethod
    def test_get_secret_from_repl(
        capsys, mocked_responses, base_uri, config_entry, secret_value
    ):
        # Given
        # Mock the http request
        mocked_responses.add(
            responses.GET,
            f"{base_uri}/api/config/secrets/ibmq-token",
            json={
                "data": {
                    "details": {
                        "name": "ibmq-token",
                        "value": secret_value,
                    }
                },
            },
        )

        # When
        Snippets.get_secret_from_repl()

        # Then
        captured = capsys.readouterr()
        assert captured.out == f"{secret_value}\n"

    @staticmethod
    def test_set_secret_from_repl(
        monkeypatch, mocked_responses, base_uri, config_entry, secret_value
    ):
        # Given
        # Mock the http request
        mocked_responses.add(
            responses.POST,
            f"{base_uri}/api/config/secrets",
            json={
                "data": {
                    "name": "ibmq-token",
                    "value": secret_value,
                },
            },
        )

        # When
        Snippets.set_secret_from_repl()

        # Then
        # Assertion is made by the HTTP response mock. The test will fail if the
        # request mock isn't exercised at least once.

    @staticmethod
    @pytest.fixture
    def qe_exec_context():
        with orquestra.sdk._base._exec_ctx.platform_qe():
            yield

    @staticmethod
    def test_get_secret_from_task(
        tmp_path,
        monkeypatch,
        capsys,
        qe_exec_context,
        mocked_responses,
        secret_value,
    ):
        # Given
        # 1. Mock the passport file
        passport_path = tmp_path / "passport.txt"
        passport_path.write_text("mocked-passport-token")

        monkeypatch.setenv("ORQUESTRA_PASSPORT_FILE", str(passport_path))

        # 2. Mock the http request
        mocked_responses.add(
            responses.GET,
            "http://config-service.config-service:8099/api/config/secrets/ibmq-token",
            json={
                "data": {
                    "details": {
                        "name": "ibmq-token",
                        "value": secret_value,
                    }
                },
            },
        )

        # When
        task_def = Snippets.get_secret_from_task()
        task_def._TaskDef__sdk_task_body()

        # Then
        captured = capsys.readouterr()
        assert captured.out == f"{secret_value}\n"
