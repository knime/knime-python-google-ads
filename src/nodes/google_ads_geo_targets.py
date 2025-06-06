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

#####################

# As the node reference https://developers.google.com/google-ads/api/fields/v18/geo_target_constant

#####################
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
from google.ads.googleads.errors import GoogleAdsException

from google.protobuf.internal.containers import RepeatedScalarFieldContainer
from google.protobuf.pyext import _message
import util.utils as utils
import util.geo_target_queries as geo_queries
from util.google_ads_version import GOOGLE_ADS_API_VERSION
import importlib

# Dynamically import the GoogleAdsRow type based on the API version
# This allows the code to adapt to different versions of the Google Ads API without hardcoding the version.
google_ads_types_module = importlib.import_module(
    f"google.ads.googleads.{GOOGLE_ADS_API_VERSION}.services.types.google_ads_service"
)
GoogleAdsRow = getattr(google_ads_types_module, "GoogleAdsRow")

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Google Ads Geo Targets (Labs)",
    node_type=knext.NodeType.SOURCE,
    icon_path="icons/google_ads_geo_targets_logo.png",
    category=google_ads_ext.main_category,
    keywords=[
        "Google",
        "Google Ads",
        "Ads",
        "Geo Targeting",
        "Location Targeting",
        "Ads Geo Data",
    ],
)
@knext.input_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)
@knext.output_table(name="Output Data", description="KNIME table with query results")
class GoogleAdsGeoTargets:
    """

    This node was designed as a support node for the Google Ads Query and Keyword Ideas nodes. It allows the user to output the geo target locations available for Google Ads.
    To know more about [location targeting](https://support.google.com/google-ads/answer/1722043?hl=en) in Google Ads, please visit the official documentation.

    **Configuration and Usage**

    1. **Selecting Geo Target Locations**

    - You select one country to output the geo target information for each node instance. If you need more countries, drag and drop additional Geo Targets nodes or use loops over the node.
    - Once the user has selected the country, you can apply the target filter, specifying the type of geo target location you want.

    2. **Examples**

    - **Example One**: List _all_ the geo target locations of a country, and then use the native KNIME Analytics Platform ETL nodes to filter and manipulate them.
    - **Example Two**: Target a specific level of the geo target hierarchy. For example, selecting the state level in the US will output only the states (e.g., California, Texas, New York, Florida, Illinois).
    - **Example Three**: To reach the entire audience of a country, you can select the country and as target _country_ to output only the country.

    **Advanced Settings**

    - Similar to the Google Ads Query node, by **unhiding** the advanced settings, you can increase the timeout if the node is failing due to this reason.

    **Mandatory Upstream Node**

    - You need to connect to the Google Ads Connector node to be able to execute the queries.

    **Output**

    - Outputs the geo target locations for further processing or analysis in KNIME Analytics Platform. You can combine this output with other KNIME extension, such as the [Geospatial Analytics Extension](https://hub.knime.com/center%20for%20geographic%20analysis%20at%20harvard%20university/extensions/sdl.harvard.features.geospatial/latest/).
    """

    country_selection = knext.EnumParameter(
        label="Country Selection",
        description="Select the countries you want to target",
        default_value=geo_queries.CountryOptions.US.name,
        enum=geo_queries.CountryOptions,
    )

    target_type = knext.EnumParameter(
        label="Target Type",
        description="Select the target type",
        default_value=geo_queries.TargetTypeOptions.CITY.name,
        enum=geo_queries.TargetTypeOptions,
    )

    custom_timeout = knext.IntParameter(
        label="Timeout (seconds)",
        description='When making a request, you can set a "timeout" parameter to specify a client-side response deadline in seconds. If you don\'t set it, the default timeout for the Google Ads API SearchStream method is five minutes.',
        default_value=300,
        min_value=1,
        is_advanced=True,
    )

    def configure(
        self,
        config_context: knext.ConfigurationContext,
        spec: GoogleAdObjectSpec,
    ):
        # TODO Check and throw config error maybe if spec.customer_id is not a string or does not have a specific format
        if hasattr(spec, "account_id") == False:
            raise knext.InvalidParametersError("Connect to the Google Ads Connector node.")

        # Define columns
        columns = [
            knext.Column(ktype=knext.string(), name="Country Code"),
            knext.Column(ktype=knext.string(), name="Name"),
            knext.Column(ktype=knext.string(), name="Canonical Name"),
            knext.Column(ktype=knext.int64(), name="ID"),
            knext.Column(ktype=knext.string(), name="Resource Name"),
            knext.Column(ktype=knext.string(), name="Target Type"),
            knext.Column(ktype=knext.string(), name="Parent ID"),
        ]

        # Return the schema
        return knext.Schema.from_columns(columns)

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
    ):
        # Build the client object
        client: GoogleAdsClient
        client = port_object.client
        account_id = port_object.spec.account_id

        primary_query = geo_queries.get_country_type_query(self.country_selection, self.target_type)

        ga_service = client.get_service("GoogleAdsService")

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = primary_query

        df = pd.DataFrame()
        ##################
        # [START PRIMARY QUERY]
        ##################
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
            if number_of_batches != 0:
                # Initialize the iteration counter
                i = 0

                # Process each batch
                for i, batch in enumerate(all_batches, start=0):
                    utils.check_canceled(exec_context)

                    header_array = [field for field in batch.field_mask.paths]

                    for row in batch.results:
                        # cancel the execution if the user cancels the execution
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
                                utils.check_canceled(exec_context)
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

                    # Set up the progress bar
                    exec_context.set_progress(
                        i / number_of_batches,
                        str(i * 10000) + " rows processed. We are preparing your data \U0001f468\u200d\U0001f373",
                    )
                # Create a DataFrame from the collected data
                df = pd.DataFrame(data, columns=header_array)
                df.columns = [
                    "Country Code",
                    "Name",
                    "Canonical Name",
                    "ID",
                    "Resource Name",
                    "Target Type",
                    "Parent ID",
                ]
            else:
                column_types = {
                    "Country Code": "string",
                    "Name": "string",
                    "Canonical Name": "string",
                    "ID": "int64",
                    "Resource Name": "string",
                    "Target Type": "string",
                    "Parent ID": "string",
                }

                # Create an empty DataFrame with the specified column names and types to avoid data spec warnings
                df = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in column_types.items()})
                exec_context.set_warning(
                    "No data was returned from the query. The target type is not supported for the selected country. Please try another combination."
                )

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
        # [END PRIMARY QUERY]
        ##################

        return knext.Table.from_pandas(pd.DataFrame(df))
