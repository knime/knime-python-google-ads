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
import google_ads_ext
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v14.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Google Ads Connector",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/gads-icon.png",
    category=google_ads_ext.main_category,
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

    login_customer_id = knext.StringParameter(
        label="Input your Manager Customer Id",
        description="The login-customer-id is equivalent to choosing an account in the Google Ads UI after signing in or clicking on your profile image at the top right",
        default_value="",
    )

    customer_id = knext.StringParameter(
        label="The campaigns Customer Id",
        description="The operating customer is the customer ID in the request payload",
        default_value="",
    )

    def configure(self, configure_context):
        return GoogleAdObjectSpec("")

    def execute(self, exec_context):
        # Combine credentials with customer ID
        # FIXME HARD-CODED for now. Need to be replaced by a credentials object once AP-20908 is supported. #NOSONAR
        # TODO Dive into obtaining of refresh token, what are the possibilities to obtain one? Are there different ones in term of life time?
        REFRESH_TOKEN = ""
        DEVELOPER_TOKEN = ""
        CLIENT_ID = ""
        CLIENT_SECRET = ""

        credentials = {
            "developer_token": DEVELOPER_TOKEN,
            "refresh_token": REFRESH_TOKEN,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "use_proto_plus": True,  # TODO double-check what this is and als double-check which other parameters we could set
            "login_customer_id": self.login_customer_id,  # Manager ID
        }
        # END FIXME

        client = GoogleAdsClient.load_from_dict(credentials)

        test_connection(client)

        test_customer_id(self.customer_id)

        LOGGER.warning(
            f"Retrieving connection information...\nDeveloper token: {client.developer_token}\nCustomer ID: {self.customer_id}"
        )

        port_object = GoogleAdConnectionObject(
            GoogleAdObjectSpec(customer_id=self.customer_id), client=client
        )
        return port_object


def test_connection(client: GoogleAdsClient):
    # TODO Implement
    #     1. Check whether client can access API, else throw error
    #     1.a Refresh talken can be falsy, the following errors could appear on KNIME console
    #         WARN  Google Ads Query     3:5        Execute failed: No connection data found. Re-execute the upstream node to refresh the connection.
    #         ERROR Google Ads Query     3:5        Execute failed: Error while sending a command.
    pass


def test_customer_id(customer_id: str):
    # TODO Implement
    pass
