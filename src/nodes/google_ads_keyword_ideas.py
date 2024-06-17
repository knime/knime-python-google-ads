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
from google.ads.googleads.v16.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
from google.ads.googleads.v16.services.services.keyword_plan_idea_service.client import (
    KeywordPlanIdeaServiceClient,
)
from google.ads.googleads.v16.services.services.geo_target_constant_service.client import (
    GeoTargetConstantServiceClient,
)
from google.ads.googleads.v16.services.types.keyword_plan_idea_service import (
    GenerateKeywordIdeasRequest,
)
from google.ads.googleads.v16.enums.types.keyword_plan_competition_level import (
    KeywordPlanCompetitionLevelEnum,
)
from google.ads.googleads.v16.enums.types.keyword_plan_network import (
    KeywordPlanNetworkEnum,
)
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from google.ads.googleads.errors import GoogleAdsException

import util.keyword_ideas_utils as keyword_ideas_utils


LOGGER = logging.getLogger(__name__)


@knext.node(
    name="Google Ads Keyword Ideas (Labs)",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/google_ads_keyword_ideas_logo.png",
    category=google_ads_ext.main_category,
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
        "KNIME table column containing the kewyords or webpage URLs from which fetch the ideas with the search volume",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        since_version=None,
    )

    locations_column = knext.ColumnParameter(
        "Locations Column",
        "KNIME table column containing the location IDs to target",
        port_index=2,
        include_row_key=False,
        include_none_column=False,
        since_version=None,
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
    LOGGER.warning(f"this is the default start value: {default_start_value}")

    # Here is the website with the reference for the managing the dates in the Google Ads API: https://developers.google.com/google-ads/api/reference/rpc/v16/HistoricalMetricsOptions
    date_start = knext.DateTimeParameter(
        label="Start date",
        description="Define the start date for the historical metrics.",
        is_advanced=True,
        show_date=True,
        show_time=False,
        show_seconds=False,
        show_milliseconds=False,
        default_value=default_start_value,
    )

    @date_start.validator
    def validate_date_start(value):
        if isinstance(value, str):
            try:
                value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").date()
            except ValueError:
                value = datetime.strptime(value, "%Y-%m-%dZ").date()
        if value.month == date.today().month:
            raise ValueError(
                "The start date cannot be set up for the current month. Please set a start date at least one month ahead."
            )
        elif keyword_ideas_utils.datediff_in_years(value, date.today()) > 4:
            raise ValueError(
                "The start date cannot be set up for a date greater than four years from the current date. Please set a start date within the last four years."
            )

    one_month_ago = date.today().replace(day=1, month=date.today().month - 1)
    default_end_value = one_month_ago.replace(day=1)
    LOGGER.warning(f"this is the default end value: {default_end_value}")

    date_end = knext.DateTimeParameter(
        label="End date",
        description="Define the end date for the historical metrics.",
        is_advanced=True,
        show_date=True,
        show_time=False,
        show_seconds=False,
        show_milliseconds=False,
        default_value=default_end_value,
    )

    @date_end.validator
    def validate_date_end(value):
        if isinstance(value, str):
            try:
                value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").date()
            except ValueError:
                value = datetime.strptime(value, "%Y-%m-%dZ").date()
        if value.month == date.today().month:
            raise ValueError(
                "The end date cannot be set up for the current month. Please set an end date at least one month ahead."
            )
        elif keyword_ideas_utils.datediff_in_years(value, date.today()) > 4:
            raise ValueError(
                "The end date cannot be set up for a date greater than four years from the current date. Please set an end date within the last four years."
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
        # TODO Check and throw config error maybe if spec.customer_id is not a string or does not have a specific format NOSONAR
        # return input_table_schema.append(knext.Column(knext.string(), "Keyword Ideas"))
        if self.date_end < self.date_start:
            raise ValueError(
                "The end date cannot be set up for a date earlier than the start date. Please set an end date later than the start date."
            )

        if self.keywords_column is None:
            raise knext.InvalidParametersError("No input column with Keywords selected")

        return None, None

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
        input_table: knext.Table,
        location_table: knext.Table,
    ) -> knext.Table:

        # Get the language id from the language selection enumparameter
        language_id = keyword_ideas_utils.get_criterion_id(self.language_selection)

        # TODO make optional the page url provider to generate ideas also from there! NOSONAR

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

        LOGGER.warning(f"Keyword texts: {keyword_texts}")

        # Creating the Google Ads Client object
        client: GoogleAdsClient
        client = port_object.client
        account_id = port_object.spec.account_id

        keyword_plan_idea_service: KeywordPlanIdeaServiceClient
        keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")

        # This is a system for measuring a keyword's level of competition in ad placement.
        # It's based on the number of advertisers bidding on that keyword compared to all other keywords on Google.
        # The level can vary depending on location and Search Network targeting options.
        # UNSPECIFIED = 0;UNKNOWN = 1;LOW = 2;MEDIUM = 3;HIGH = 4

        keyword_competition_level_enum: KeywordPlanCompetitionLevelEnum
        keyword_competition_level_enum = client.enums.KeywordPlanCompetitionLevelEnum

        # Container for enumeration of keyword plan forecastable network types
        keyword_plan_network: KeywordPlanNetworkEnum
        keyword_plan_network = (
            client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS
        )

        # List the location IDs
        location_rns = keyword_ideas_utils.map_locations_ids_to_resource_names(
            client, location_ids_list
        )

        # Returns a fully-qualified language_constant string.
        language_rn_get_service: GoogleAdsServiceClient
        language_rn_get_service = client.get_service("GoogleAdsService")
        language_rn = language_rn_get_service.language_constant_path(language_id)
        LOGGER.warning(f"Language id: {language_rn}")

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
            )
        )
        return knext.Table.from_pandas(
            df_keyword_ideas_aggregated
        ), knext.Table.from_pandas(df_monthly_search_volumes)

        # [END generate_keyword_ideas]
