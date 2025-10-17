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
from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)
from google.ads.googleads.client import GoogleAdsClient
from datetime import date, datetime
from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta
import util.keyword_ideas_utils as keyword_ideas_utils
from util.utils import check_column, pick_default_column, create_type_filer
from util.google_ads_version import GOOGLE_ADS_API_VERSION
import importlib

# Importing the necessary enums from the Google Ads API versioned modules
# The enums are dynamically imported based on the GOOGLE_ADS_API_VERSION.
# This allows for flexibility in case the API version changes.

keyword_plan_competition_level_enum_module = importlib.import_module(
    f"google.ads.googleads.{GOOGLE_ADS_API_VERSION}.enums.types.keyword_plan_competition_level"
)
KeywordPlanCompetitionLevelEnum = getattr(keyword_plan_competition_level_enum_module, "KeywordPlanCompetitionLevelEnum")
HISTORICAL_METRICS_OPTIONS_URL = (
    f"https://developers.google.com/google-ads/api/reference/rpc/v{GOOGLE_ADS_API_VERSION}/HistoricalMetricsOptions"
)

LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Google Ads Keyword Ideas (Labs)",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/google_ads_keyword_ideas_logo.png",
    category=google_ads_ext.main_category,
    keywords=[
        "Google",
        "Google Ads",
        "Ads",
        "Keyword Ideas",
        "Keyword Research",
        "Ads Keyword Metrics",
    ],
)
@knext.input_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)
@knext.input_table(
    name="Keywords",
    description="KNIME table that contains a list of keywords to generate ideas from",
)
@knext.input_table(
    name="Location IDs",
    description="KNIME table that contains a list of location IDs to target",
)
@knext.output_table(
    name="Keywords Ideas Aggregated data",
    description="KNIME table with keyword ideas and the aggregated data such as the average monthly searches, competition, average CPC, and seasonality",
)
@knext.output_table(
    name="Keywords Ideas Historical Monthly Search Volumes",
    description="KNIME table with keyword ideas and the monthly search volumes to check trends and seasonality. The data is aggregated by month for the selected period (default is 12 months).",
)
class GoogleAdsKwdIdeas(knext.PythonNode):
    """

    This node generates **keyword ideas** from a list of _keywords_ or _webpage URLs_ and returns the **aggregated data** such as the average monthly searches, competition, average CPC, and seasonality.
    The node also returns the **historical monthly search volumes** to check _trends and seasonality_. The data is aggregated by month for the selected period (default is 12 months).
    For an overview of the Keyword Ideas service, see the [Google Ads API support page](https://support.google.com/google-ads/answer/6325025?sjid=12334900779237463092-EU).

    **Configuration and Usage**

    **General Settings**:

    - Specify the **language** and **location** that users are using to perform Google searches to determine the results of _keyword idea metrics_. For instance, if you select English as the language and Alaska, US as the location, and your keyword is in Spanish, such as "playa," the search volume of the generated ideas will be low.

    **Mandatory Upstream Nodes**

    1. **Google Ads Connector**: A connection to the Google Ads Connector node is required.
    2. **Seed Data**: A KNIME table with the seed column "keywords" or "Webpages URLs".
    3. **Geo Target Locations**: Another table with the geo target location ID. The _Google Ads Geo Targets_ node can be used to input a column with the IDs.

    **Node Processing Logic**

    - **Cost and Complexity**: Due to cost and complexity, the Planning service methods are subject to separate limits from other types of requests. See the link: [Google Ads API Quotas](https://developers.google.com/google-ads/api/docs/best-practices/quotas#planning_services).
    - **Processing Chunks**: For the above reason, the node processes **keywords in chunks of 20** and **locations in chunks of 1** by default. The location chunks settings can be modified by unhiding the advanced settings (max 10 chunks).
    - **Output Columns**: The output includes **two additional columns** with the iteration ID and the location for which the keyword ideas were generated and their metrics.
    - **Processing Time**: Due to the chunks and API limit, large datasets might take a significant amount of time to be processed.

    **Advanced Settings**

    - Retrieve data up to 4 years.
    - Option to include average CPC.
    - Option to include adult keywords in the results.

    **Output**

    1. **Keyword Ideas with Aggregated Metrics**: Provides the keyword ideas with aggregated metrics for the selected chunks of location and in the determined language.
    2. **Keyword Ideas with Historical Metrics**: Provides the keyword ideas with historical metrics for the selected chunks of locations. You can use downstream KNIME components to analyze seasonality, for example: [KNIME Seasonality Analysis](https://hub.knime.com/-/spaces/-/~YStBnJ-9lhpx4txe/current-state/).

    """

    keyword_ideas_mode = knext.EnumParameter(
        label="Keyword Ideas Input Mode",
        description="Choose to generate new keyword ideas from **keywords** OR **webpage URLs** and select the input column accordingly.",
        default_value=keyword_ideas_utils.NewKeywordIdeasMode.KEYWORDS.name,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
        enum=keyword_ideas_utils.NewKeywordIdeasMode,
        is_advanced=False,
    )

    keywords_column = knext.ColumnParameter(
        "Seed Column",
        "KNIME table column containing the kewyords or webpage URLs from which fetch the ideas with the search volume. Only columns with string type are allowed.",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        since_version=None,
        column_filter=create_type_filer(knext.string()),
    )

    locations_column = knext.ColumnParameter(
        "Locations Column",
        "KNIME table column containing the location IDs to target. Only columns with int64 type are allowed.",
        port_index=2,
        include_row_key=False,
        include_none_column=False,
        since_version=None,
        column_filter=create_type_filer(knext.int64()),
    )
    # Select language parameter group hardcoded from the CSV in the data folder and built as class (EnumParameterOptions) in the data_utils.py file.

    language_selection = knext.EnumParameter(
        label="Language",
        description="Select the language to target.",
        default_value=keyword_ideas_utils.LanguageSelection.ENGLISH.name,
        enum=keyword_ideas_utils.LanguageSelection,
        style=knext.EnumParameter.Style.DROPDOWN,
    )

    # Default value for the start date is thirteen months ago from the current date, because by default the historical metrics are set up for the last twelve (not including the current one) months.

    thirteen_months_ago = date.today() - relativedelta(months=13)
    default_start_value = thirteen_months_ago.replace(day=1)

    # Here is the website with the reference for managing the dates in the Google Ads API:
    # {HISTORICAL_METRICS_OPTIONS_URL}
    date_start = knext.DateTimeParameter(
        label="Start date",
        description="Define the start date for the keywords historical metrics. The default is 13 months ago from the current date. The maximum date range is 4 years.",
        is_advanced=True,
        show_date=True,
        show_time=False,
        show_seconds=False,
        show_milliseconds=False,
        default_value=default_start_value,
    )

    one_month_ago = date.today().replace(day=1) - relativedelta(months=1)
    default_end_value = one_month_ago.replace(day=1)

    date_end = knext.DateTimeParameter(
        label="End date",
        description="Define the end date for the keywords historical metrics.",
        is_advanced=True,
        show_date=True,
        show_time=False,
        show_seconds=False,
        show_milliseconds=False,
        default_value=default_end_value,
    )

    rows_per_chunk = knext.IntParameter(
        label="Rows per Chunk",
        description="Number of rows per chunk to send to the Google Ads API. Maximumn number of rows per chunk is 10 and the minimum 1. Note that a list of row values will be added as new column in the output.",
        default_value=1,
        min_value=1,
        max_value=10,
        is_advanced=True,
    )

    include_adult_keywords = knext.BoolParameter(
        label="Include Adult Keywords",
        description="Include adult keywords in the keyword ideas results. Default is False.",
        default_value=False,
        is_advanced=True,
    )
    include_average_cpc = knext.BoolParameter(
        label="Include Average CPC",
        description="Indicates whether to include average cost per click value. Average CPC is provided only for legacy support. Default is True.",
        default_value=True,
        is_advanced=True,
    )

    def configure(
        self,
        configure_context: knext.ConfigurationContext,
        spec: GoogleAdObjectSpec,
        input_table_schema: knext.Schema,
        location_table_schema: knext.Schema,
    ):
        """Configuration-time validation for column and date parameters.
        Only performs checks that are timeless and structural.
        """
        # --- Normalize parameters to date objects for consistent comparisons ---
        try:
            start_date = keyword_ideas_utils.ensure_date(self.date_start, "start date")
            end_date = keyword_ideas_utils.ensure_date(self.date_end, "end date")
        except ValueError as e:
            raise knext.InvalidParametersError(str(e))

        # --- Cross-field (timeless) validation ---
        if end_date < start_date:
            raise knext.InvalidParametersError(
                "The end date cannot be earlier than the start date. "
                "Please select an end date later than or equal to the start date."
            )

        if self.keywords_column:
            check_column(input_table_schema, self.keywords_column, knext.string(), "seed data")
        else:
            self.keywords_column = pick_default_column(input_table_schema, knext.string())

        if self.locations_column:
            check_column(
                location_table_schema,
                self.locations_column,
                knext.int64(),
                "location IDs",
            )
        else:
            self.locations_column = pick_default_column(location_table_schema, knext.int64())
        return None, None

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
        input_table: knext.Table,
        location_table: knext.Table,
    ) -> knext.Table:
        # --- Normalize parameters to date objects (do not rely on validators mutating) ---
        try:
            start_date = keyword_ideas_utils.ensure_date(self.date_start, "start date")
            end_date = keyword_ideas_utils.ensure_date(self.date_end, "end date")
        except ValueError as e:
            # Surface a friendly, user-visible error in KNIME
            raise knext.InvalidParametersError(str(e))

        # --- Time-dependent rules (checked at run time) ---
        today = date.today()

        # Block current month for start
        if start_date.year == today.year and start_date.month == today.month:
            raise knext.InvalidParametersError("Start date cannot be set to the current month.")

        # Block current month for end
        if end_date.year == today.year and end_date.month == today.month:
            raise knext.InvalidParametersError("End date cannot be set to the current month.")

        # Limit lookback window to last 4 years (start and end)
        if keyword_ideas_utils.datediff_in_years(start_date, today) > 4:
            raise knext.InvalidParametersError("Start date must be within the last four years.")

        if keyword_ideas_utils.datediff_in_years(end_date, today) > 4:
            raise knext.InvalidParametersError("End date must be within the last four years.")

        # --- Parameters are now normalized and validated ---

        exec_context.set_warning
        # Get the language id from the language selection enumparameter
        language_id = keyword_ideas_utils.get_criterion_id(self.language_selection)

        # Get the location IDs from the location table
        location_ids_column = location_table.to_pandas()
        location_ids_list = location_ids_column[self.locations_column].tolist()

        # Get the keywords from the input table
        keywords_column = self.keywords_column
        if self.keywords_column is not None:
            keyword_texts_df = input_table.to_pandas()
        else:
            exec_context.set_warning("No column selected")

        keyword_texts = keyword_texts_df[keywords_column].tolist()

        # Creating the Google Ads Client object
        client: GoogleAdsClient
        client = port_object.client
        account_id = port_object.spec.account_id

        keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")

        # This is a system for measuring a keyword's level of competition in ad placement.
        # It's based on the number of advertisers bidding on that keyword compared to all other keywords on Google.
        # The level can vary depending on location and Search Network targeting options.
        # UNSPECIFIED = 0;UNKNOWN = 1;LOW = 2;MEDIUM = 3;HIGH = 4

        keyword_competition_level_enum = KeywordPlanCompetitionLevelEnum

        # Container for enumeration of keyword plan forecastable network types
        keyword_plan_network = client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS  # type: ignore List the location IDs
        location_rns = keyword_ideas_utils.map_locations_ids_to_resource_names(client, location_ids_list)

        # Returns a fully-qualified language_constant string.
        language_rn_get_service = client.get_service("GoogleAdsService")
        language_rn = language_rn_get_service.language_constant_path(language_id)

        # Do the Keyword Ideas generation and return the table

        df_keyword_ideas_aggregated, df_monthly_search_volumes = (
            keyword_ideas_utils.generate_keywords_ideas_with_chunks(
                self,
                location_rns,
                account_id,
                client,
                keyword_plan_idea_service,
                keyword_texts,
                language_rn,
                keyword_plan_network,
                self.include_adult_keywords,
                self.date_start,
                self.date_end,
                self.include_average_cpc,
                self.keyword_ideas_mode,
                exec_context,
                self.rows_per_chunk,
            )
        )

        df_keyword_ideas_aggregated = keyword_ideas_utils.extract_first_item_if_all_chunk_numbers_are_1(
            self.rows_per_chunk, df_keyword_ideas_aggregated
        )
        df_monthly_search_volumes = keyword_ideas_utils.extract_first_item_if_all_chunk_numbers_are_1(
            self.rows_per_chunk, df_monthly_search_volumes
        )

        return knext.Table.from_pandas(df_keyword_ideas_aggregated), knext.Table.from_pandas(df_monthly_search_volumes)

        # [END generate_keyword_ideas]
