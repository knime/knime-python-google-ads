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
import util.pre_built_ad_queries as pb_queries
from google.ads.googleads.v18.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v18 import enums as google_ads_enums
from google.ads.googleads.v18.services.types.google_ads_service import GoogleAdsRow
from google.protobuf.internal.containers import RepeatedScalarFieldContainer
from google.protobuf.pyext import _message
import util.utils as utils


LOGGER = logging.getLogger(__name__)


class QueryBuilderMode(knext.EnumParameterOptions):
    PREBUILT = (
        "Pre-built",
        "These pre-built queries provide a set of queries in Google Ads Query Language that return the same data "
        "as the screens in the [Google Ads UI](https://developers.google.com/google-ads/api/docs/query/cookbook).",
    )

    MANUALLY = (
        "Custom",
        "Build your query using [Google Ads Query Builder](https://developers.google.com/google-ads/api/fields/v12/overview_query_builder),"
        "then validate it with [Google Ads Query Validator](https://developers.google.com/google-ads/api/fields/v12/query_validator) for desired results.",
    )


@knext.node(
    name="Google Ads Query (Labs)",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/google_ads_query_logo.png",
    category=google_ads_ext.main_category,
    keywords=[
        "Google",
        "Google Ads",
        "Ads",
        "Ads Query",
        "Ads Data Fetch",
        "Keyword Reports",
    ],
)
@knext.input_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)
@knext.output_table(name="Output Data", description="KNIME table with query results")
class GoogleAdsQuery:
    """

    The Google Ads Query node allows the user to fetch data from their Google Ads account to build reports, analyze, and share highlights.

    **Configuration and Usage**

    There are two modes to interact with the node: pre-built and custom queries.

    1. **Pre-built Queries**

        - These queries mirror the reports in the Google Ads dashboard, allowing you to quickly import your data into the KNIME Analytics Platform.
        - For example, the _Search Terms_ report can be fine-tuned for different time ranges.

    2. **Custom Queries**

        - If you are comfortable with SQL, you can use the custom mode to leverage the Google Ads Query Language (GAQL) to fetch your data.
        - This mode is versatile and gives greater control over the data you retrieve. For more information, refer to the [Google Ads Query Language Guide](https://developers.google.com/google-ads/api/docs/query/overview).
        - Additionally, you can validate your queries to ensure they are correct. Refer to the [Query Validation Guide](https://developers.google.com/google-ads/api/fields/v18/query_validator).

    **Advanced Settings**

    - You can increase the timeout for large queries by **unhiding** the advanced settings and adjusting the timeout value accordingly.

    **Mandatory Upstream Node**

    - You need to connect to the Google Ads Connector node to execute the queries.

    **Output**

    - *The output port outputs the fetched data for further processing or analysis in KNIME Analytics Platform.
    """

    query_mode = knext.EnumParameter(
        "Query mode",
        "You can choose from **pre-built** queries or create a **custom** one from scratch.",
        QueryBuilderMode.PREBUILT.name,
        QueryBuilderMode,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    )

    query_custom = knext.MultilineStringParameter(
        label="Custom query:",
        description="Input your query below, replacing the default query.",
        default_value="""SELECT
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
        FROM campaign""",
        number_of_lines=10,
    ).rule(
        knext.OneOf(query_mode, [QueryBuilderMode.MANUALLY.name]),
        knext.Effect.SHOW,
    )

    query_prebuilt_name = knext.EnumParameter(
        label="Pre-built queries:",
        description="Select an available pre-built query to be used.",
        default_value=pb_queries.HardCodedQueries.CAMPAIGNS.name,
        enum=pb_queries.HardCodedQueries,
    ).rule(
        knext.OneOf(query_mode, [QueryBuilderMode.PREBUILT.name]),
        knext.Effect.SHOW,
    )

    date_start_query = knext.DateTimeParameter(
        label="Start date",
        description="Define the start date for the selected pre-built query.",
        is_advanced=False,
        show_date=True,
        show_time=False,
        show_seconds=False,
        show_milliseconds=False,
    ).rule(
        knext.OneOf(query_mode, [QueryBuilderMode.PREBUILT.name]),
        knext.Effect.SHOW,
    )

    date_end_query = knext.DateTimeParameter(
        label="End date",
        description="End date for the selected pre-built query",
        is_advanced=False,
        show_date=True,
        show_time=False,
        show_seconds=False,
        show_milliseconds=False,
    ).rule(
        knext.OneOf(query_mode, [QueryBuilderMode.PREBUILT.name]),
        knext.Effect.SHOW,
    )

    custom_timeout = knext.IntParameter(
        label="Timeout (seconds)",
        description='When making a request, you can set a "timeout" parameter to specify a client-side response deadline in seconds. If you don\'t set it, the default timeout for the Google Ads API SearchStream method is five minutes.',
        default_value=300,
        min_value=1,
        is_advanced=True,
    )

    def configure(self, configuration_context, spec: GoogleAdObjectSpec):
        # TODO Check and throw config error maybe if spec.customer_id is not a string or does not have a specific format
        if hasattr(spec, "account_id") == False:
            raise knext.InvalidParametersError("Connect to the Google Ads Connector node.")
        pass  # Which configuration I need to pass?? explain better the configure method, not 100% clear.

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
    ):
        # counter to build the progress bar during execution
        # i = 0
        # Build the client Object
        client: GoogleAdsClient
        client = port_object.client
        account_id = port_object.spec.account_id

        ####################
        # [START QUERY]
        ####################
        # TODO Implement config window with a query builder
        execution_query = self.define_query()

        DEFAULT_QUERY = """
        SELECT
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
        FROM campaign"""

        if execution_query == "":
            exec_context.set_warning("Used default query because you didn't provide one.")
            execution_query = DEFAULT_QUERY

        ga_service: GoogleAdsServiceClient
        ga_service = client.get_service("GoogleAdsService")

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = execution_query

        df = pd.DataFrame()
        try:
            response_stream = ga_service.search_stream(search_request, timeout=self.custom_timeout)
            # Initialize the necessary variables
            data = []
            header_array = []
            all_batches = []

            # First pass: Collect all batches and count them
            for batch in response_stream:
                all_batches.append(batch)

            number_of_batches = len(all_batches)

            if number_of_batches == 0:
                exec_context.set_warning("No data was returned from the query.")
            else:
                # Initialize the iteration counter to use during the set up of the progress bar
                i = 0

                # Process each batch
                for i, batch in enumerate(all_batches, start=0):
                    utils.check_canceled(exec_context)

                    header_array = [field for field in batch.field_mask.paths]

                    for row in batch.results:
                        # Cancel the execution if the user cancels the node execution
                        utils.check_canceled(exec_context)
                        data_row = []
                        row: GoogleAdsRow
                        for field in batch.field_mask.paths:
                            utils.check_canceled(exec_context)

                            # Split the attribute_name string into parts
                            attribute_parts = field.split(".")

                            # Initialize the object to start the traversal
                            attribute_value = row

                            # Traverse the attribute parts and access the attributes
                            for part in attribute_parts:
                                # query-fix for ADGROUP and AD queries: we are iterating over the attribute_value (type = class) line
                                # and using the field name splitted to access the values with the getattr(method),
                                # when trying to use 'type' there is not any attr called like this
                                # in the class attribute_value, so adding and underscore fix this.
                                # temp fix: we don't know how to check before the attr name of the class attribute value
                                if part == "type":
                                    part = part + "_"

                                attribute_value = getattr(attribute_value, part)

                                # query-fix for AD query. Explanation for the below if: when fetching the field "final_urls" from the response_stream, it returned a [] type that was not in any Python readable type.
                                # indeed the type was this protobuf RepeatedScalarFieldContainer. The goal of the if clause is to convert the empty list to empty strings and extract the RepeatedScalarFieldContainer( similar to list type) element
                                # for reference https://googleapis.dev/python/protobuf/latest/google/protobuf/internal/containers.html
                                if type(attribute_value) is _message.RepeatedScalarContainer:
                                    attribute_value: RepeatedScalarFieldContainer
                                    if len(attribute_value) == 0:
                                        attribute_value = ""
                                    else:
                                        attribute_value = attribute_value.pop(0)
                            data_row.append(attribute_value)
                        data.append(data_row)

                    # Set up the progress bar taking the toal number of batches and the batch iteration counter (1 batch = 10.000 rows)
                    exec_context.set_progress(
                        i / number_of_batches,
                        str(i * 10000) + " rows processed. We are preparing your data \U0001f468\u200d\U0001f373",
                    )
                # Create a pandas dataframe with the data and the header
                df = pd.DataFrame(data, columns=header_array)
                df.columns = [col.replace(".", " ").replace("_", " ").title() for col in df.columns]

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
        ##################
        # [END QUERY]
        ##################

        inspector = pb_queries.FieldInspector(client, google_ads_enums, LOGGER)

        # Mutate df in-place
        inspector.process_dataframe(df, execution_query)

        return knext.Table.from_pandas(pd.DataFrame(df))

    def define_query(self):
        query = ""

        if self.query_mode == "MANUALLY":
            query = self.query_custom
        elif self.query_mode == "PREBUILT":
            query = pb_queries.get_query(self.query_prebuilt_name, self.date_start_query, self.date_end_query)
        return query
