################################################################################
# © Copyright 2023-2024 Zapata Computing Inc.
################################################################################
from .._base import _env
from ._env_var_reader import EnvVarReader
from ._flight_facade import FlightCallOptions, FlightClient, FlightDescriptor


class DremioClient:
    """Reads dataframes from Dremio instance running on Orquestra.

    Requires specifying connection details as environment variables:

        * ``ORQ_DREMIO_URI``: the full dremio URI, with protocol and port. Example
          value: ``grpc+tls://ws2-12de2f.test-cluster.dremio-client.orquestra.io``
        * ``ORQ_DREMIO_USER``: your Dremio account email.
        * ``ORQ_DREMIO_PASS``: your Dremio account password.

    To obtain the values described above, follow these steps:

    #. Open Orquestra Portal—open your cluster's URI in a web browser.
    #. Select workspace.
    #. Open "Connectors".
    #. Click "Connect" on the "Dremio" tab.
    #. Click "Copy Flight Endpoint". This is the value for your ``ORQ_DREMIO_URI``.
    #. Click "Launch".
    #. Inside Dremio, go to settings and configure your user account.
       ``ORQ_DREMIO_USER`` is your Dremio account email.
       ``ORQ_DREMIO_PASS`` is your Dremio account password.
    """

    @classmethod
    def from_env_vars(cls) -> "DremioClient":
        uri_reader = EnvVarReader(_env.ORQ_DREMIO_URI)
        user_reader = EnvVarReader(_env.ORQ_DREMIO_USER)
        pass_reader = EnvVarReader(_env.ORQ_DREMIO_PASS)

        flight_client = FlightClient(f"{uri_reader.read()}")

        return cls(
            flight_client=flight_client,
            user_reader=user_reader,
            pass_reader=pass_reader,
        )

    def __init__(
        self,
        flight_client: FlightClient,
        user_reader: EnvVarReader,
        pass_reader: EnvVarReader,
    ):
        self._flight_client = flight_client
        self._user_reader = user_reader
        self._pass_reader = pass_reader

    def read_query(self, query: str):
        flight_desc = FlightDescriptor.for_command(query)
        options = self._get_call_options()
        ticket = self._get_ticket(descriptor=flight_desc, call_options=options)

        # Retrieve the result set as a stream of Arrow record batches.
        reader = self._flight_client.do_get(ticket, options)
        return reader.read_pandas()

    def _get_ticket(
        self, descriptor: FlightDescriptor, call_options: FlightCallOptions
    ):
        flight_info = self._flight_client.get_flight_info(descriptor, call_options)
        return flight_info.endpoints[0].ticket

    def _get_call_options(self):
        user = self._user_reader.read()
        password = self._pass_reader.read()
        token = self._flight_client.authenticate_basic_token(user, password)
        return FlightCallOptions(headers=[token])
