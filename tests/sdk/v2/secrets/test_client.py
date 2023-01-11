################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Tests for orquestra.sdk._base._secrets._client.
During the prototype phase, they interact with the live system. This should be
refactored to be unit tests before going on with the target implementation.
"""
import pytest
import responses

from orquestra.sdk.secrets import _exceptions, _models
from orquestra.sdk.secrets._client import SecretsClient

from . import resp_mocks


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


class TestClient:
    @pytest.fixture
    def base_uri(self):
        return "https://shouldnt.matter.example.com"

    @pytest.fixture
    def token(self):
        return "shouldn't matter"

    @pytest.fixture
    def client(self, base_uri, token):
        return SecretsClient.from_token(base_uri=base_uri, token=token)

    @pytest.fixture
    def secret_name(self):
        return "my-deepest-secret"

    @pytest.fixture
    def secret_value(self):
        return "I like turtles!"

    # ------ queries ------

    class TestGet:
        @staticmethod
        @pytest.fixture
        def endpoint_mocker(mocked_responses, base_uri: str, secret_name: str):
            """
            Returns a helper for mocking requests. Assumes that most of the tests
            inside this class contain a very similar set up.
            """

            def _mocker(**responses_kwargs):
                mocked_responses.add(
                    responses.GET,
                    f"{base_uri}/api/config/secrets/{secret_name}",
                    **responses_kwargs,
                )

            return _mocker

        @staticmethod
        def test_response_parsing(
            endpoint_mocker, client: SecretsClient, secret_name, secret_value
        ):
            """
            Verifies that the response is correctly deserialized.
            """
            endpoint_mocker(
                json=resp_mocks.make_get_response(
                    secret_name=secret_name,
                    secret_value=secret_value,
                ),
            )

            secret = client.get_secret(secret_name)

            assert secret.name == secret_name
            assert secret.value == secret_value

        @staticmethod
        def test_sets_auth(
            endpoint_mocker, client: SecretsClient, token, secret_name, secret_value
        ):
            endpoint_mocker(
                match=[
                    responses.matchers.header_matcher(
                        {"Authorization": f"Bearer {token}"}
                    )
                ],
                json=resp_mocks.make_get_response(
                    secret_name=secret_name, secret_value=secret_value
                ),
            )

            client.get_secret(secret_name)

            # The assertion is done by mocked_responses

        @staticmethod
        def test_invalid_name(endpoint_mocker, client: SecretsClient, secret_name):
            endpoint_mocker(
                # Specified in:
                # https://github.com/zapatacomputing/config-service/blob/3c4a86851bbe9f8848bb27107cdcfd168603802a/openapi/src/resources/secret.yaml#L20
                status=404,
            )

            with pytest.raises(_exceptions.SecretNotFoundError):
                _ = client.get_secret(secret_name)

        @staticmethod
        def test_unauthorized(endpoint_mocker, client: SecretsClient, secret_name):
            endpoint_mocker(
                # Based on manual testing.
                status=401,
            )

            with pytest.raises(_exceptions.InvalidTokenError):
                _ = client.get_secret(secret_name)

    class TestList:
        @pytest.fixture
        def secret_names(self):
            return ["my-secret-1", "my-secret-2"]

        @staticmethod
        @pytest.fixture
        def endpoint_mocker(mocked_responses, base_uri: str):
            """
            Returns a helper for mocking requests. Assumes that most of the tests
            inside this class contain a very similar set up.
            """

            def _mocker(**responses_kwargs):
                mocked_responses.add(
                    responses.GET,
                    f"{base_uri}/api/config/secrets",
                    **responses_kwargs,
                )

            return _mocker

        @staticmethod
        def test_response_parsing(endpoint_mocker, client: SecretsClient, secret_names):
            """
            Verifies that the response is correctly deserialized.
            """
            endpoint_mocker(
                json=resp_mocks.make_list_response(secret_names=secret_names)
            )

            secrets = client.list_secrets()

            assert [s.name for s in secrets] == secret_names

        @staticmethod
        def test_sets_auth(endpoint_mocker, client: SecretsClient, token, secret_names):
            endpoint_mocker(
                match=[
                    responses.matchers.header_matcher(
                        {"Authorization": f"Bearer {token}"}
                    )
                ],
                json=resp_mocks.make_list_response(secret_names=secret_names),
            )

            client.list_secrets()

            # The assertion is done by mocked_responses

        @staticmethod
        def test_unauthorized(endpoint_mocker, client: SecretsClient):
            endpoint_mocker(
                # Based on manual testing.
                status=401,
            )

            with pytest.raises(_exceptions.InvalidTokenError):
                _ = client.list_secrets()

    # ------ mutations ------

    class TestCreate:
        @staticmethod
        @pytest.fixture
        def endpoint_mocker(mocked_responses, base_uri: str):
            """
            Returns a helper for mocking requests. Assumes that most of the tests
            inside this class contain a very similar set up.
            """

            def _mocker(**responses_kwargs):
                mocked_responses.add(
                    responses.POST,
                    f"{base_uri}/api/config/secrets",
                    **responses_kwargs,
                )

            return _mocker

        @staticmethod
        def test_params_encoding(
            endpoint_mocker, client: SecretsClient, secret_name, secret_value
        ):
            """
            Verifies that params are correctly sent to the server.
            """
            endpoint_mocker(
                json=resp_mocks.make_create_response(secret_name=secret_name),
                match=[
                    responses.matchers.json_params_matcher(
                        {
                            "data": {"name": secret_name, "value": secret_value},
                        }
                    )
                ],
                # Based on:
                # https://github.com/zapatacomputing/config-service/blob/3f275a52149fb2b74c6a8c01726cce4f390a1533/openapi/src/resources/secrets.yaml#L39
                status=201,
            )

            client.create_secret(
                _models.SecretDefinition(name=secret_name, value=secret_value)
            )

            # The assertion is done by mocked_responses

        @staticmethod
        def test_sets_auth(
            endpoint_mocker, client: SecretsClient, token, secret_name, secret_value
        ):
            endpoint_mocker(
                match=[
                    responses.matchers.header_matcher(
                        {"Authorization": f"Bearer {token}"}
                    )
                ],
                # Based on:
                # https://github.com/zapatacomputing/config-service/blob/3f275a52149fb2b74c6a8c01726cce4f390a1533/openapi/src/resources/secrets.yaml#L39
                status=201,
                json=resp_mocks.make_create_response(secret_name=secret_name),
            )

            client.create_secret(
                _models.SecretDefinition(name=secret_name, value=secret_value)
            )

            # The assertion is done by mocked_responses

        @staticmethod
        def test_conflicting_name(
            endpoint_mocker, client: SecretsClient, secret_name, secret_value
        ):
            endpoint_mocker(
                # Specified in:
                # https://github.com/zapatacomputing/config-service/blob/3c4a86851bbe9f8848bb27107cdcfd168603802a/openapi/src/resources/secrets.yaml#L41
                status=400,
            )

            with pytest.raises(_exceptions.SecretAlreadyExistsError):
                client.create_secret(
                    _models.SecretDefinition(name=secret_name, value=secret_value)
                )

        @staticmethod
        def test_unauthorized(
            endpoint_mocker, client: SecretsClient, secret_name, secret_value
        ):
            endpoint_mocker(
                # Based on manual testing.
                status=401,
            )

            with pytest.raises(_exceptions.InvalidTokenError):
                client.create_secret(
                    _models.SecretDefinition(name=secret_name, value=secret_value)
                )

    class TestUpdate:
        @staticmethod
        @pytest.fixture
        def endpoint_mocker(mocked_responses, base_uri: str, secret_name):
            """
            Returns a helper for mocking requests. Assumes that most of the tests
            inside this class contain a very similar set up.
            """

            def _mocker(**responses_kwargs):
                mocked_responses.add(
                    responses.POST,
                    f"{base_uri}/api/config/secrets/{secret_name}",
                    **responses_kwargs,
                )

            return _mocker

        @staticmethod
        def test_params_encoding(
            endpoint_mocker, client: SecretsClient, secret_name, secret_value
        ):
            """
            Verifies that params are correctly sent to the server.
            """
            endpoint_mocker(
                json=resp_mocks.make_update_response(secret_value=secret_value),
                match=[
                    responses.matchers.json_params_matcher(
                        {
                            "data": {"value": secret_value},
                        }
                    )
                ],
            )

            client.update_secret(name=secret_name, value=secret_value)

            # The assertion is done by mocked_responses

        @staticmethod
        def test_sets_auth(
            endpoint_mocker, client: SecretsClient, token, secret_name, secret_value
        ):
            endpoint_mocker(
                match=[
                    responses.matchers.header_matcher(
                        {"Authorization": f"Bearer {token}"}
                    )
                ],
                json=resp_mocks.make_update_response(secret_value=secret_value),
            )

            client.update_secret(name=secret_name, value=secret_value)

            # The assertion is done by mocked_responses

        @staticmethod
        def test_invalid_name(
            endpoint_mocker, client: SecretsClient, secret_name, secret_value
        ):
            endpoint_mocker(
                # Specified in:
                # https://github.com/zapatacomputing/config-service/blob/3c4a86851bbe9f8848bb27107cdcfd168603802a/openapi/src/resources/secret.yaml#L61
                status=404,
            )

            with pytest.raises(_exceptions.SecretNotFoundError):
                _ = client.update_secret(secret_name, secret_value)

        @staticmethod
        def test_unauthorized(
            endpoint_mocker, client: SecretsClient, secret_name, secret_value
        ):
            endpoint_mocker(
                # Based on manual testing.
                status=401,
            )

            with pytest.raises(_exceptions.InvalidTokenError):
                _ = client.update_secret(secret_name, secret_value)

    class TestDelete:
        @staticmethod
        @pytest.fixture
        def endpoint_mocker(mocked_responses, base_uri: str, secret_name):
            """
            Returns a helper for mocking requests. Assumes that most of the tests
            inside this class contain a very similar set up.
            """

            def _mocker(**responses_kwargs):
                mocked_responses.add(
                    responses.DELETE,
                    f"{base_uri}/api/config/secrets/{secret_name}",
                    **responses_kwargs,
                )

            return _mocker

        @staticmethod
        def test_sets_auth(endpoint_mocker, client: SecretsClient, token, secret_name):
            endpoint_mocker(
                match=[
                    responses.matchers.header_matcher(
                        {"Authorization": f"Bearer {token}"}
                    )
                ],
            )

            client.delete_secret(secret_name)

            # The assertion is done by mocked_responses

        @staticmethod
        def test_invalid_name(endpoint_mocker, client: SecretsClient, secret_name):
            endpoint_mocker(
                # Specified in:
                # https://github.com/zapatacomputing/config-service/blob/3c4a86851bbe9f8848bb27107cdcfd168603802a/openapi/src/resources/secret.yaml#L61
                status=404,
            )

            with pytest.raises(_exceptions.SecretNotFoundError):
                _ = client.delete_secret(secret_name)

        @staticmethod
        def test_unauthorized(endpoint_mocker, client: SecretsClient, secret_name):
            endpoint_mocker(
                # Based on manual testing.
                status=401,
            )

            with pytest.raises(_exceptions.InvalidTokenError):
                _ = client.delete_secret(secret_name)
