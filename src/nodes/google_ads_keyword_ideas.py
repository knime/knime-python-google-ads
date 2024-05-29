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

import util.utils as utils

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
@knext.output_table(name="Output Data", description="KNIME table with keyword ideas")
class GoogleAdsKwdIdeas(knext.PythonNode):

    selected_column = knext.ColumnParameter(
        "Keywords Column",
        "KNIME table column containing the kewyords from which fetch the ideas with the search volume",
        port_index=1,
        include_row_key=False,
        include_none_column=True,
        since_version=None,
    )

    location_id = knext.StringParameter(
        label="Location ID",
        description="Input your location ID",
        default_value="1023191",
        is_advanced=False,
    )

    language_id = knext.StringParameter(
        label="Language Id",
        description="Input the Language ID",
        default_value="1000",
        is_advanced=False,
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
        elif datediff_in_years(value, date.today()) > 4:
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
        elif datediff_in_years(value, date.today()) > 4:
            raise ValueError(
                "The end date cannot be set up for a date greater than four years from the current date. Please set an end date within the last four years."
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
    ):
        # TODO Check and throw config error maybe if spec.customer_id is not a string or does not have a specific format NOSONAR
        # We will add one column of type double to the table
        # return input_table_schema.append(knext.Column(knext.string(), "Keyword Ideas"))
        if self.date_end < self.date_start:
            raise ValueError(
                "The end date cannot be set up for a date earlier than the start date. Please set an end date later than the start date."
            )

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
        input_table: knext.Table,
    ) -> knext.Table:
        # TODO make optional the page url provider to generate ideas also from there! NOSONAR

        location_ids = [self.location_id]
        selected_column = self.selected_column
        if self.selected_column is not None:
            keyword_texts_df = input_table.to_pandas()
        else:
            exec_context.set_warning("No column selected")

        keyword_texts = keyword_texts_df[selected_column].tolist()

        LOGGER.warning(msg="check the keyword_texts object")
        LOGGER.warning(type(keyword_texts))

        # Creating the Google Ads Client object
        LOGGER.warning(f"the googleadsclient object:{type(GoogleAdsClient)}")
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
        location_rns = self.map_locations_ids_to_resource_names(client, location_ids)
        LOGGER.warning(f"location_rns= {location_rns}")

        # Returns a fully-qualified language_constant string.
        language_rn_get_service: GoogleAdsServiceClient
        language_rn_get_service = client.get_service("GoogleAdsService")
        language_rn = language_rn_get_service.language_constant_path(self.language_id)
        LOGGER.warning(f"Language id: {language_rn}")

        # [Preparing the request]
        # Only one of the fields "url_seed", "keyword_seed", or
        # "keyword_and_url_seed" can be set on the request, depending on whether
        # keywords, a page_url or both were passed to this function.
        request: GenerateKeywordIdeasRequest
        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = account_id
        request.language = language_rn
        request.geo_target_constants.extend(location_rns)
        request.include_adult_keywords = False
        request.keyword_plan_network = keyword_plan_network
        request.include_adult_keywords = self.include_adult_keywords

        # Properly create and set the year_month_range within historical_metrics_options
        historical_metrics_options = client.get_type("HistoricalMetricsOptions")
        year_month_range = historical_metrics_options.year_month_range

        year_month_range.start.year = self.date_start.year
        # The month is 1-based, so we need to add 1 to the month to get the correct value.
        year_month_range.start.month = self.date_start.month + 1
        year_month_range.end.year = self.date_end.year
        year_month_range.end.month = self.date_end.month + 1

        LOGGER.warning(f"this the date range type:{type(historical_metrics_options)}")
        LOGGER.warning(f"this is the range itself: {historical_metrics_options}")

        request.historical_metrics_options.CopyFrom(historical_metrics_options)
        request.historical_metrics_options.include_average_cpc = (
            self.include_average_cpc
        )

        # TODO admit a website URL as input NOSONAR
        # To generate keyword ideas with only a page_url and no keywords we need
        # to initialize a UrlSeed object with the page_url as the "url" field.
        #     request.url_seed.url = page_url

        # To generate keyword ideas with only a list of keywords and no page_url
        # we need to initialize a KeywordSeed object and set the "keywords" field
        # to be a list of StringValue objects.
        request.keyword_seed.keywords.extend(keyword_texts)

        # To generate keyword ideas using both a list of keywords and a page_url we
        # need to initialize a KeywordAndUrlSeed object, setting both the "url" and
        # "keywords" fields.
        #     request.keyword_and_url_seed.url = page_url
        #     request.keyword_and_url_seed.keywords.extend(keyword_texts)

        keyword_ideas = keyword_plan_idea_service.generate_keyword_ideas(
            request=request
        )
        LOGGER.warning("let'see what it is")
        LOGGER.warning(type(keyword_ideas))

        # Create empty lists to store data
        keywords = []
        avg_monthly_searches = []
        competition_values = []
        competition_index = []
        average_cpc_micros = []
        high_top_of_page_bid_micros = []
        low_top_of_page_bid_micros = []
        monthly_search_volumes = []
        total_search_volume = 0

        # Extract data and populate lists
        for idea in keyword_ideas:

            keywords.append(idea.text)
            avg_monthly_searches.append(idea.keyword_idea_metrics.avg_monthly_searches)
            competition_values.append(
                competition_to_text(idea.keyword_idea_metrics.competition)
            )
            competition_index.append(idea.keyword_idea_metrics.competition_index)
            average_cpc_micros.append(
                micros_to_currency(idea.keyword_idea_metrics.average_cpc_micros)
            )
            high_top_of_page_bid_micros.append(
                micros_to_currency(
                    idea.keyword_idea_metrics.high_top_of_page_bid_micros
                )
            )
            low_top_of_page_bid_micros.append(
                micros_to_currency(idea.keyword_idea_metrics.low_top_of_page_bid_micros)
            )
            monthly_search_volumes.append(
                idea.keyword_idea_metrics.monthly_search_volumes
            )
        # Create a DataFrame from the lists
        data = {
            "Keyword": keywords,
            # Approximate number of monthly searches on this query averaged for the
            # past 12 months.
            "Avg Monthly Searches": avg_monthly_searches,
            # The competition level for this search query.
            "Competition": competition_values,
            # The competition index for the query in the range [0, 100]. This shows
            # how competitive ad placement is for a keyword. The level of
            # competition from 0-100 is determined by the number of ad slots filled
            # divided by the total number of ad slots available. If not enough data
            # is available, undef will be returned.
            "Competition Index": competition_index,
            "Average Cost per Click": average_cpc_micros,
            # Top of page bid high range (80th percentile) in micros for the
            # keyword.
            "Top of Page Bid High Range (Currency) ": high_top_of_page_bid_micros,
            # Top of page bid low range (20th percentile) in micros for the keyword.
            "Top of Page Bid Low Range (Currency)": low_top_of_page_bid_micros,
            # Approximate number of searches on this query for the past twelve
            # months.
            # "Total Search Volume": monthly_search_volumes,TODO check this
        }
        LOGGER.warning(msg="check the data df")
        df = pd.DataFrame(data)
        LOGGER.warning(type(df))
        # Display the DataFrame
        return knext.Table.from_pandas(df)

        # [END generate_keyword_ideas]

    def map_locations_ids_to_resource_names(
        self, port_object: GoogleAdsClient, location_ids
    ):
        client = port_object

        build_resource_name_client: GeoTargetConstantServiceClient
        build_resource_name_client = client.get_service("GeoTargetConstantService")
        build_resource_name = build_resource_name_client.geo_target_constant_path
        return [build_resource_name(location_id) for location_id in location_ids]


# Function to use in the date_start ane date_end validators to check if the input date is greater than four years from the current date
def datediff_in_years(date1, date2):
    return abs(date1.year - date2.year)


def competition_to_text(competition_value):
    if competition_value == 0:
        return "Unspecified"
    elif competition_value == 1:
        return "Unknown"
    elif competition_value == 2:
        return "Low"
    elif competition_value == 3:
        return "Medium"
    elif competition_value == 4:
        return "High"
    else:
        return "Unknown"


def micros_to_currency(micros):
    return micros / 1_000_000
