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

# Check if it necessary to import this class to handle the authentication via service account.
from google.oauth2.service_account import Credentials as Credentials_service

import google_ads_ext
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v18.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
import pandas as pd
from google.ads.googleads.v18.services.types.google_ads_service import GoogleAdsRow
from google.ads.googleads.v18.services.services.customer_service.client import (
    CustomerServiceClient,
)
from google.ads.googleads.v18.services.types.customer_service import (
    ListAccessibleCustomersResponse,
)
from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)
from google.ads.googleads.errors import GoogleAdsException

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Google Ads Connector (Labs)",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/google_ads_connector_logo.png",
    category=google_ads_ext.main_category,
    keywords=[
        "Google",
        "Google Ads",
        "Ads",
        "Google Authenticator",
        "Ads Authentication",
        "Google API Auth",
    ],
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
    """

    This node connects with the Google Ads API, enabling interaction with Google Ads services.

    ### **Configuration and Usage**

    **Mandatory Upstream Node**: The Google Authenticator node is required upstream. Ensure you provide the scope `https://www.googleapis.com/auth/adwords` there if it is not listed.

    ### **User Requirements**

    1. **Developer Token**: Available in the API Center section under Tools & Settings.
    2. **Manager Account ID**: Found in the top right corner of the Google Ads dashboard when logged in as a manager.
    3. **Account ID**: Displayed in the account overview section.

    ### **Account Requirements**

    1. A Google Ads account with a Manager account is necessary to use this connector.
    2. Request a developer token using the [Google Ads API](https://developers.google.com/google-ads/api/docs/get-started/dev-token) link.

    """

    developer_token = knext.StringParameter(
        label="Developer Token",
        description="The Google developer token is needed to connect to the Google Ads API. It can be obtained following [this docucmentation](https://developers.google.com/google-ads/api/docs/get-started/dev-token?hl=en).",
        default_value="",
    )  # .rule(knext.OneOf(dev_token_retrieval, [DeveloperTokenRetrieval.MANUALLY.name]),knext.Effect.SHOW,)

    manager_customer_id = knext.StringParameter(
        label="Manager Customer Id",
        description="The login-customer-id is equivalent to choosing an account in the Google Ads UI after signing in or clicking on your profile image at the top right.",
        default_value="",
    )  # .rule(knext.OneOf(dev_token_retrieval, [DeveloperTokenRetrieval.MANUALLY.name]),knext.Effect.SHOW,)

    account_id = knext.StringParameter(
        label="Account Id",
        description="The account id of your target campaigns.",
        default_value="",
    )  # .rule(knext.OneOf(dev_token_retrieval, [DeveloperTokenRetrieval.MANUALLY.name]),knext.Effect.SHOW,)

    def configure(
        self,
        configuration_context: knext.ConfigurationContext,
        credential_port: knext.PortObjectSpec,
    ):
        return GoogleAdObjectSpec("", [])

    def execute(self, exec_context: knext.ExecutionContext, credential: knext.PortObject):
        # Combine credentials with customer ID
        # Use the access token provided in the input port.
        # Token refresh is handled by the provided refresh handler that requests the token from the input port.
        credentials = Credentials(
            token=str(credential.spec.auth_parameters),
            expiry=credential.spec.expires_after,
            refresh_handler=get_refresh_handler(credential.spec),
        )

        # TODO Implement a method to use the service account access to make calls to the Google Ads api
        # https://developers.google.com/identity/protocols/oauth2/service-account#python_1

        client = GoogleAdsClient(
            credentials=credentials,
            developer_token=self.developer_token.strip(),
            login_customer_id=cleanup_ids(self.manager_customer_id),
        )

        # Leaving this Logger because it is useful for testing the authentication via service account.
        mcc = manager_customer_ids(client)
        LOGGER.warning(f" testing client built {mcc[0]}")

        campaign_ids = get_campaigns_id(client, cleanup_ids(self.account_id))

        test_connection(client)

        port_object = GoogleAdConnectionObject(
            GoogleAdObjectSpec(account_id=cleanup_ids(self.account_id), campaign_ids=campaign_ids),
            client=client,
        )
        return port_object


def get_refresh_handler(spec: knext.CredentialPortObjectSpec) -> callable:
    """Returns a function that returns the access token and the expiration time of the access token."""
    return lambda request, scopes: (spec.auth_parameters, spec.expires_after)


def cleanup_ids(id: str) -> str:
    if id.strip() == "":
        raise knext.InvalidParametersError("Please review your Manager Customer Id and your Account Id")
    return id.replace("-", "").strip()


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


# This method is useful to get the connection because we are perfoming a query to get the campaign ids.
# So we use the client object (build with the developer token the google auth credentials and the Manager Customer ID) and the account id to get the campaign ids.)
# In case of failure we raise a meaningful error message.
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
        error_first_part = " ".join(
            [
                "Failed with status",
                status_error,
            ]
        )
        error_second_part = " ".join([error_messages])
        error_to_raise = ". ".join([error_first_part, error_second_part])
        raise knext.InvalidParametersError(error_to_raise)

    df_list = pd.DataFrame(df)["campaign.id"].tolist()
    return df_list


# this function is to test the authentication via service account, delete after implementation.
# using it because we don't need to use the account id to perfomr the query. Only the dev tokent and the test manager account id.
def manager_customer_ids(client):
    # Accessing access token from input credential port via DialogCreationContext
    customer_service: CustomerServiceClient
    customer_service = client.get_service("CustomerService")

    # Accessing customer IDs
    accessible_customers: ListAccessibleCustomersResponse
    accessible_customers = customer_service.list_accessible_customers()

    resource_names = accessible_customers.resource_names

    # Extract numerical IDs from resource names
    customer_ids = [str(name.split("/")[-1]) for name in resource_names]

    return customer_ids
