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
import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)
from google.ads.googleads.v14.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
from google.ads.googleads.errors import GoogleAdsException

from google.ads.googleads.v14.services.types.google_ads_service import GoogleAdsRow

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Google Ads Query",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/gads-icon.png",
    category=google_ads_ext.main_category,
)
@knext.input_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)
@knext.output_table(name="Output Data", description="KNIME table with query results")
class GoogleAdsQuery:
    """Fetch data from Google Adwords for a given query.

    Long description of the node.
    Can be multiple lines.
    """

    query = knext.MultilineStringParameter(
        label="Query",
        description="Build your query using [Google Ads Query Builder](https://developers.google.com/google-ads/api/fields/v12/overview_query_builder), then validate it with [Google Ads Query Validator](https://developers.google.com/google-ads/api/fields/v12/query_validator) for desired results.",
        default_value="",
        number_of_lines = 10,
    )

    def configure(self, configure_context, spec: GoogleAdObjectSpec):
        # TODO Check and throw config error maybe if spec.customer_id is not a string or does not have a specific format
        pass

    def execute(self, exec_context, port_object: GoogleAdConnectionObject):
        client: GoogleAdsClient
        client = port_object.client
        customer_id = port_object.spec.customer_id

        ####################
        # [START QUERY TEST]
        ####################
        # TODO Implement config window with a query builder
        DEFAULT_QUERY = """
        SELECT
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
        FROM campaign"""

        if self.query == "":
            LOGGER.warning("Using default query")
            self.query = DEFAULT_QUERY

        ga_service: GoogleAdsServiceClient
        ga_service = client.get_service("GoogleAdsService")

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = customer_id
        search_request.query = self.query
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
            LOGGER.warning(
                "Google Ads API request failed. Please check your query and credentials."
            )
            LOGGER.warning(ex)
        ##################
        # [END QUERY TEST]
        ##################

        return knext.Table.from_pandas(pd.DataFrame(df))
