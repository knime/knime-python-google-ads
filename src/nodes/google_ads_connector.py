# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

import logging
import knime.extension as knext
from google.oauth2.credentials import Credentials
import google_ads_ext
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v15.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
import pandas as pd
from google.ads.googleads.v15.services.types.google_ads_service import GoogleAdsRow
from google.ads.googleads.v15.services.services.customer_service.client import (
    CustomerServiceClient,
)
from google.ads.googleads.v15.services.types.customer_service import (
    ListAccessibleCustomersResponse,
)
from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)
from google.ads.googleads.errors import GoogleAdsException

LOGGER = logging.getLogger(__name__)

"""
class DeveloperTokenRetrieval(knext.EnumParameterOptions):
    MANUALLY = (
        "Manual",
        "Write down the developer token directly into the node configuration.",
    )
    CREDENTIALS = (
        "Credentials",
        "Reads the developer token from the credentials configuration node.",
    )


def manager_customer_ids(ctx: knext.DialogCreationContext):
    # Accessing access token from input credential port via DialogCreationContext
    specs = ctx.get_input_specs()
    auth_spec = specs[0] if specs else None
    credentials = Credentials(token=auth_spec.auth_parameters)

    # Accessing credentials password:
    credentials_name = ctx.get_credential_names()
    developer_token = ctx.get_credentials(credentials_name[0]).password

    # Building the object to make the request: list accessible customers giving the current authorization

    client: GoogleAdsClient
    client = GoogleAdsClient(credentials=credentials, developer_token=developer_token)
    customer_service: CustomerServiceClient
    customer_service = client.get_service("CustomerService")

    # Accessing customer IDs
    accessible_customers: ListAccessibleCustomersResponse
    accessible_customers = customer_service.list_accessible_customers()
    resource_names = accessible_customers.resource_names

    # Extract numerical IDs from resource names
    customer_ids = [str(name.split("/")[-1]) for name in resource_names]

    return customer_ids


# [END list_accessible_customers]


def retrieve_customer_ids(ctx: knext.DialogCreationContext) -> list[str]:
    query = """"""
    SELECT 
        customer_client.id
    FROM customer_client """"""

    # Accessing access token from input credential port via DialogCreationContext
    specs = ctx.get_input_specs()
    auth_spec = specs[0] if specs else None
    credentials = Credentials(token=auth_spec.auth_parameters)

    # Accessing credentials password:
    credentials_name = ctx.get_credential_names()
    developer_token = ctx.get_credentials(credentials_name[0]).password

    # Building the object to make the request: Google ads service giving the current authorization
    client: GoogleAdsClient
    client = GoogleAdsClient(credentials=credentials, developer_token=developer_token)
    ga_service: GoogleAdsServiceClient
    ga_service = client.get_service("GoogleAdsService")

    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = ""
    search_request.query = query
    LOGGER.warning("Setting query customer id done.")

    df = pd.DataFrame()
    try:
        response_stream = ga_service.search_stream(search_request)
        data = []
        header_array = []
        for batch in response_stream:
            header_array = [field for field in batch.field_mask.paths]
            for row in batch.results:
                data_row = []
                row: GoogleAdsRow
                for field in batch.field_mask.paths:
                    LOGGER.warning(
                        f"trying to understand {field}"
                    )  # OUTPUT: customer_client.id
                    # Split the attribute_name string into parts
                    attribute_parts = field.split(".")
                    LOGGER.warning(
                        f"what's happening here? {attribute_parts}"
                    )  # OUTPUT: ['customer_client', 'id']
                    # Initialize the object to start the traversal
                    attribute_value = row
                    # Traverse the attribute parts and access the attributes
                    LOGGER.warning(
                        f"Second Loop print {attribute_value}"
                    )
                    for part in attribute_parts:
                        attribute_value = getattr(attribute_value, part)
                    data_row.append(str(attribute_value))
                    LOGGER.warning(f"third loop print {attribute_value}")  # OUPTUT:
                data.append(data_row)

        df = pd.DataFrame(data, columns=header_array)

    except GoogleAdsException as ex:
        LOGGER.warning(  # TODO New error message # NOSONAR
            "Google Ads API request failed. Please check your query and credentials."
        )
        LOGGER.warning(ex)
    df_list = pd.DataFrame(df)["customer_client.id"].tolist()
    LOGGER.warning("customer id list")
    LOGGER.warning(df_list)
    return df_list


def _create_specific_manager_customer_ids_list() -> knext.StringParameter:
    return knext.StringParameter(
        label="Manager Customer Id",
        description="The login-customer-id is equivalent to choosing an account in the Google Ads UI after signing in or clicking on your profile image at the top right.",
        choices=lambda c: manager_customer_ids(c),
        default_value="Unselected",
        is_advanced=False,
    )


def _create_specific_customer_ids_list() -> knext.StringParameter:
    return knext.StringParameter(
        label="Customer Id",
        description="",
        choices=lambda c: retrieve_customer_ids(c),
        default_value="Unselected",
        is_advanced=False,
    )


@knext.parameter_group(label="")
class GAdsconnectorLoaderInputSettings:
    manager_customer_id = _create_specific_manager_customer_ids_list()


@knext.parameter_group(label="")
class GAdsconnectorLoaderCustomerIDs:
    customer_id_retrieval = _create_specific_customer_ids_list()
"""

@knext.node(
    name="Google Ads Connector",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons\google_ads_connector_logo.png",
    category=google_ads_ext.main_category,
)
@knext.input_port(
    name="Credentials",
    description="Google Ads credentials",
    port_type=knext.PortType.CREDENTIAL,
)
@knext.output_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)
class GoogleAdsConnector:
    """Takes credentials and adds the customer ID so downstream nodes can access the Google Ads API.

    Long description of the node.
    Can be multiple lines.
    """
    """
    dev_token_retrieval = knext.EnumParameter(
        "Connection method",
        "Input the necessary parameters to stablish a connection manually or using a Credentials Configuration node.",
        DeveloperTokenRetrieval.MANUALLY.name,
        DeveloperTokenRetrieval,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    )

    LOGGER.warning(f"testing a warning here {dev_token_retrieval}")
    """
    developer_token = knext.StringParameter(
        label="Developer Token",
        description="The Google developer token is needed to connect to the Google Ads API. It can be obtained following [this docucmentation](https://developers.google.com/google-ads/api/docs/get-started/dev-token?hl=en).",
        default_value="",
    )#.rule(knext.OneOf(dev_token_retrieval, [DeveloperTokenRetrieval.MANUALLY.name]),knext.Effect.SHOW,)

    manager_customer_id = knext.StringParameter(
        label="Manager Customer Id",
        description="The login-customer-id is equivalent to choosing an account in the Google Ads UI after signing in or clicking on your profile image at the top right.",
        default_value="",
    )#.rule(knext.OneOf(dev_token_retrieval, [DeveloperTokenRetrieval.MANUALLY.name]),knext.Effect.SHOW,)

    account_id = knext.StringParameter(
        label="Account Id",
        description="The account id of your target campaigns.",
        default_value="",
    )#.rule(knext.OneOf(dev_token_retrieval, [DeveloperTokenRetrieval.MANUALLY.name]),knext.Effect.SHOW,)

    """
    input_settings = GAdsconnectorLoaderInputSettings().rule(
        knext.OneOf(dev_token_retrieval, [DeveloperTokenRetrieval.CREDENTIALS.name]),
        knext.Effect.SHOW,
    )
    LOGGER.warning(f"Showing up the input setting string {input_settings}")

    customer_id_settings = GAdsconnectorLoaderCustomerIDs().rule(
        knext.OneOf(input_settings, [0])
        and knext.OneOf(dev_token_retrieval, [DeveloperTokenRetrieval.MANUALLY.name]),
        knext.Effect.HIDE,
    )
    """
    def configure(
        self,
        configuration_context: knext.ConfigurationContext,
        credential_port: knext.PortObjectSpec,
    ):
        return GoogleAdObjectSpec("", [])

    def execute(
        self, exec_context: knext.ExecutionContext, credential: knext.PortObject
    ):
        # Combine credentials with customer ID
        # Use the access token provided in the input port. The token gets automatically refreshed by the upstream Google Authenticator node.
        credentials = Credentials(token=str(credential.spec.auth_parameters))
        LOGGER.warning(f"access_token: {credential.spec.auth_parameters}")
        LOGGER.warning("auth_parameter")
        LOGGER.warning(dir(credential.spec))
        LOGGER.warning(f"What developer token I am passing here {self.developer_token}")

        client = GoogleAdsClient(
            credentials=credentials,
            developer_token=self.developer_token.strip(),
            login_customer_id=cleanup_ids(self.manager_customer_id),
        )
        LOGGER.warning(f" GoogleAdsClient object: {dir(client)}")

        campaign_ids = get_campaigns_id(client, cleanup_ids(self.account_id))
       
        test_connection(client)

        # test_customer_id(self.customer_id)

        LOGGER.warning(
            f"Retrieving connection information...\nDeveloper token: {client.developer_token}\nCustomer ID: {self.account_id}"
        )

        port_object = GoogleAdConnectionObject(
            GoogleAdObjectSpec(account_id=cleanup_ids(self.account_id), campaign_ids=campaign_ids),
            client=client,
        )
        return port_object

def cleanup_ids(id: str)->str:
    if id.strip() == "":
        raise knext.InvalidParametersError("Please review your Manager Customer Id and your Account Id")
    return id.replace("-","").strip()


def test_connection(client: GoogleAdsClient):
    # TODO Implement
    #     1. Check whether client can access API, else throw error
    #     1.a Refresh token can be falsy, the following errors could appear on KNIME console
    #         WARN  Google Ads Query     3:5        Execute failed: No connection data found. Re-execute the upstream node to refresh the connection.
    #         ERROR Google Ads Query     3:5        Execute failed: Error while sending a command.
    pass


def test_customer_id(account_id: str):
    # TODO Implement
    pass


def get_campaigns_id(client: GoogleAdsClient, account_id: str) -> list[str]:
    query = """
    SELECT
        campaign.id,
        campaign.name
    FROM campaign
    ORDER BY campaign.id"""

    ga_service: GoogleAdsServiceClient
    ga_service = client.get_service("GoogleAdsService")

    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = account_id
    search_request.query = query
    LOGGER.warning("Setting query done.")

    df = pd.DataFrame()
    try:
        response_stream = ga_service.search_stream(search_request)
        data = []
        header_array = []
        for batch in response_stream:
            header_array = [field for field in batch.field_mask.paths]
            for row in batch.results:
                data_row = []
                row: GoogleAdsRow
                for field in batch.field_mask.paths:
                    # Split the attribute_name string into parts
                    attribute_parts = field.split(".")
                    # Initialize the object to start the traversal
                    attribute_value = row
                    # Traverse the attribute parts and access the attributes
                    for part in attribute_parts:
                        attribute_value = getattr(attribute_value, part)
                    data_row.append(attribute_value)
                data.append(data_row)

        df = pd.DataFrame(data, columns=header_array)

    except GoogleAdsException as ex:
        status_error = ex.error.code().name
        error_messages = ""
        for error in ex.failure.errors:
            error_messages = " ".join([error.message])
        error_first_part= " ".join(["Failed with status",status_error,])
        error_second_part = " ".join([error_messages])
        error_to_raise = ". ".join([error_first_part,error_second_part])
        raise knext.InvalidParametersError(error_to_raise)
    
    df_list = pd.DataFrame(df)["campaign.id"].tolist()
    LOGGER.warning(df_list)
    return df_list
