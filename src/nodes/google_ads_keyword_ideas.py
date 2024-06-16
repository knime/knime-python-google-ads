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
import numpy as np
from itertools import islice
import time
import random
from collections import deque
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
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from google.ads.googleads.errors import GoogleAdsException
import util.utils as utils
from util.data_utils import (
    read_csv,
    get_criterion_id,
    convert_to_list,
    LanguageSelection,
)
from google.ads.googleads.v16.errors.types.quota_error import QuotaErrorEnum


LOGGER = logging.getLogger(__name__)


LOGGER.warning(f"print dictionaries: {read_csv()}")


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
# TODO: Add a new output table with the search volumes to check trends and seasonality
@knext.output_table(
    name="Keywords Ideas Aggregated data",
    description="KNIME table with keyword ideas and the aggregated data such as the average monthly searches, competition, average CPC, and seasonality",
)
@knext.output_table(
    name="Keywords Ideas Historical Monthly Search Volumes",
    description="KNIME table with keyword ideas and the monthly search volumes to check trends and seasonality. The data is aggregated by month for the selected period (default is 12 months).",
)
class GoogleAdsKwdIdeas(knext.PythonNode):

    keywords_column = knext.ColumnParameter(
        "Keywords Column",
        "KNIME table column containing the kewyords from which fetch the ideas with the search volume",
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
        default_value=LanguageSelection.ENGLISH.name,
        enum=LanguageSelection,
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

    rows_per_chunk = knext.IntParameter(
        label="Rows per Chunk",
        description="Number of rows per chunk to send to the Google Ads API. Maximumn number of rows per chunk is 10 and the minimum 1. Note that a list of row values will be added as new column in the output.",
        default_value=1,
        min_value=1,
        max_value=10,
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

        return input_table_schema, location_table_schema

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
        input_table: knext.Table,
        location_table: knext.Table,
    ) -> knext.Table:

        # Get the language id from the language selection enumparameter
        language_id = get_criterion_id(self.language_selection)

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
        location_rns = self.map_locations_ids_to_resource_names(
            client, location_ids_list
        )

        # Returns a fully-qualified language_constant string.
        language_rn_get_service: GoogleAdsServiceClient
        language_rn_get_service = client.get_service("GoogleAdsService")
        language_rn = language_rn_get_service.language_constant_path(language_id)
        LOGGER.warning(f"Language id: {language_rn}")

        # Do the Keyword Ideas generation and return the table

        df_table, df_monthly_search_volumes = self.generate_keywords_ideas_with_chunks(
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
        )
        return knext.Table.from_pandas(df_table), knext.Table.from_pandas(
            df_monthly_search_volumes
        )

        # [END generate_keyword_ideas]

    def map_locations_ids_to_resource_names(
        self, port_object: GoogleAdsClient, location_ids
    ):
        client = port_object

        build_resource_name_client: GeoTargetConstantServiceClient
        build_resource_name_client = client.get_service("GeoTargetConstantService")
        build_resource_name = build_resource_name_client.geo_target_constant_path
        return [build_resource_name(location_id) for location_id in location_ids]

    # Function to chunk the location IDs into groups of 10

    # Initialize a deque to keep track of request timestamps
    request_timestamps = deque(maxlen=60)

    # Define a function to retry the request with exponential backoff if a RESOURCE_EXHAUSTED error occurs.
    # Max requests per min are 60: 1 request per second
    # Quota reference website: https://developers.google.com/google-ads/api/docs/best-practices/quotas#planning_services

    def exponential_backoff_retry(self, func, max_attempts=5, initial_delay=5):
        delay = initial_delay
        LOGGER.warning(f"Initial delay: {delay}")

        for attempt in range(max_attempts):
            LOGGER.warning(f"Attempt {attempt+1} of {max_attempts}")
            try:
                # Rate limiting check
                if self.request_timestamps:
                    time_since_last_request = time.time() - self.request_timestamps[-1]
                    formatted_time = format_timestamp(time_since_last_request)
                    LOGGER.warning(f"Time since last request: {formatted_time}")
                    if time_since_last_request < 1:
                        sleep_time = 1 - time_since_last_request
                        formatted_sleep_time = format_timestamp(sleep_time)
                        LOGGER.warning(
                            f"Sleeping for {formatted_sleep_time} seconds due to rate limiting"
                        )
                        time.sleep(sleep_time)

                # Make the request and record the timestamp
                result = func()
                self.request_timestamps.append(time.time())
                formatted_request_timestamps = [
                    format_timestamp(ts) for ts in self.request_timestamps
                ]
                LOGGER.warning(f"Request timestamps: {formatted_request_timestamps}")
                LOGGER.warning(
                    f"Length of request timestamps: {len(self.request_timestamps)}"
                )
                return result

            except GoogleAdsException as ex:
                error_code = ex.failure.errors[0].error_code
                LOGGER.warning(f"Error code: {error_code.quota_error}")
                if (
                    error_code.quota_error == QuotaErrorEnum.RESOURCE_EXHAUSTED
                    or ex.error.code() == 8  # StatusCode.RESOURCE_EXHAUSTED
                ):
                    if attempt < max_attempts - 1:
                        LOGGER.warning(
                            f"Attempt {attempt+1} failed due to RESOURCE_EXHAUSTED. Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff
                        delay += random.uniform(
                            0, delay
                        )  # Add jitter to avoid thundering herd problem
                    else:
                        LOGGER.warning("Max attempts reached, raising the exception.")
                        raise
                else:
                    LOGGER.warning(f"Non-retryable error encountered: {ex}")
                    raise

    # Define a function to chunk the location
    def chunked(self, iterable, size):
        it = iter(iterable)
        return iter(lambda: tuple(islice(it, size)), ())

    # Function to parse monthly search volumes and convert to DataFrame
    def parse_monthly_search_volumes(self, monthly_search_volumes, keyword):
        rows = [
            {
                "keyword": keyword,
                "month": metrics.month,
                "year": metrics.year,
                "monthly searches": metrics.monthly_searches,
            }
            for metrics in monthly_search_volumes
        ]
        return pd.DataFrame(rows)

    # Function to generate keyword ideas with chunks
    def generate_keywords_ideas_with_chunks(
        self,
        location_rns,
        account_id,
        client,
        keyword_plan_idea_service,
        keyword_texts,
        language_rn,
        keyword_plan_network,
        include_adult_keywords,
        date_start,
        date_end,
        include_average_cpc,
    ):
        location_chunks = self.chunked(location_rns, self.rows_per_chunk)
        LOGGER.warning(f"Location chunks: {location_chunks}")
        all_keyword_ideas = []
        iteration_ids = []
        # Create empty lists to store list of location IDs used on each iteration
        location_ids = []

        # Clear the request timestamps for a new batch of requests
        self.request_timestamps.clear()
        LOGGER.warning(f"Request timestamps cleared: {self.request_timestamps}")

        for iteration_id, chunk in enumerate(location_chunks, start=1):

            def request_keyword_ideas(chunk):
                # [Preparing the request]
                # Only one of the fields "url_seed", "keyword_seed", or
                # "keyword_and_url_seed" can be set on the request, depending on whether
                # keywords, a page_url or both were passed to this function.
                request: GenerateKeywordIdeasRequest
                request = client.get_type("GenerateKeywordIdeasRequest")
                request.customer_id = account_id
                request.language = language_rn
                request.geo_target_constants.extend(chunk)
                request.keyword_plan_network = keyword_plan_network
                request.include_adult_keywords = include_adult_keywords

                # Properly create and set the year_month_range within historical_metrics_options
                historical_metrics_options = client.get_type("HistoricalMetricsOptions")
                year_month_range = historical_metrics_options.year_month_range

                year_month_range.start.year = date_start.year
                # The month is 1-based, so we need to add 1 to the month to get the correct value.
                year_month_range.start.month = date_start.month + 1
                year_month_range.end.year = date_end.year
                year_month_range.end.month = date_end.month + 1

                request.historical_metrics_options.CopyFrom(historical_metrics_options)
                request.historical_metrics_options.include_average_cpc = (
                    include_average_cpc
                )

                # TODO admit a website URL as input NOSONAR
                # To generate keyword ideas with only a page_url and no keywords we need
                # to initialize a UrlSeed object with the page_url as the "url" field.
                #     request.url_seed.url = page_url

                # To generate keyword ideas with only a list of keywords and no page_url
                # we need to initialize a KeywordSeed object and set the "keywords" field
                # to be a list of StringValue objects.
                request.keyword_seed.keywords.extend(keyword_texts)

                return keyword_plan_idea_service.generate_keyword_ideas(request=request)

            LOGGER.warning(f"Chunk: {chunk}")

            # Make the request with retry logic
            keyword_ideas_pager = self.exponential_backoff_retry(
                lambda c=chunk: request_keyword_ideas(c)
            )
            LOGGER.warning(f"Keyword ideas pager: {keyword_ideas_pager}")
            LOGGER.warning(f"Iteration ID: {iteration_id}")

            keyword_ideas = list(keyword_ideas_pager)
            all_keyword_ideas.extend(keyword_ideas)
            iteration_ids.extend([iteration_id] * len(keyword_ideas))
            # Append the location IDs (list) used on each iteration
            location_ids.extend([chunk] * len(keyword_ideas))

        # Create empty lists to store data
        keywords_ideas = []
        avg_monthly_searches = []
        competition_values = []
        competition_index = []
        average_cpc_micros = []
        high_top_of_page_bid_micros = []
        low_top_of_page_bid_micros = []
        search_volumes = []
        seasonality = []

        # create a list to store the monthly search volumes to output in a separate table

        monthly_search_volumes_dfs = []

        # Extract data and populate lists
        for idea in all_keyword_ideas:

            keywords_ideas.append(idea.text)
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
            monthly_search_volumes = [
                metrics.monthly_searches
                for metrics in idea.keyword_idea_metrics.monthly_search_volumes
            ]
            # Calculate the total search volume of the period
            search_volumes.append(sum(monthly_search_volumes))

            # Append the monthly search volumes to the list to output in a separate table

            monthly_df = self.parse_monthly_search_volumes(
                idea.keyword_idea_metrics.monthly_search_volumes, idea.text
            )
            monthly_search_volumes_dfs.append(monthly_df)

            # Calculate the seasonality of the search volumes
            if not monthly_search_volumes:
                adjusted_seasonality = None
            else:
                # Calculate trend line using linear regression
                x = np.arange(len(monthly_search_volumes))
                y = monthly_search_volumes
                coefficients = np.polyfit(x, y, 1)
                trend_line = np.polyval(coefficients, x)

                # Calculate residuals
                residuals = y - trend_line

                # Calculate standard deviation of residuals
                std_dev = np.std(residuals)

                # Adjust seasonality
                avg_search_volume = np.mean(monthly_search_volumes)
                adjusted_seasonality = std_dev / avg_search_volume
            seasonality.append(adjusted_seasonality)

        # Create a DataFrame from the lists and include the iteration ID
        data = {
            "Keyword Idea": keywords_ideas,
            # Approximate number of monthly searches on this query averaged for the selected period (default is 12 months)
            "Average Monthly Searches": avg_monthly_searches,
            # Approximate number of searches on this query for the past twelve months.
            "Total Searches of the Period": search_volumes,
            # Average cost per click for the query.
            "Average Cost per Click": average_cpc_micros,
            # Calculated the trend line, residuals, standard deviation of residuals, and adjusted seasonality for the provided monthly search volumes data.
            # Reference article: https://blog.startupstash.com/detect-seasonality-within-keyword-planner-data-in-google-sheets-eb9c3dabbe53
            "Searches Seasonality": seasonality,
            # The competition level for this search query.
            "Competition": competition_values,
            # The competition index for the query in the range [0, 100]. This shows
            # how competitive ad placement is for a keyword. The level of
            # competition from 0-100 is determined by the number of ad slots filled
            # divided by the total number of ad slots available. If not enough data
            # is available, undef will be returned.
            "Competition Index": competition_index,
            # Top of page bid high range (80th percentile) in micros for the
            # keyword.
            "Top of Page Bid High Range (Currency) ": high_top_of_page_bid_micros,
            # Top of page bid low range (20th percentile) in micros for the keyword.
            "Top of Page Bid Low Range (Currency)": low_top_of_page_bid_micros,
            "Chunk Number": iteration_ids,
            "Locations in Chunk": location_ids,
        }

        # Dataframe with the keyword ideas and the aggregated data for the first output table
        df = pd.DataFrame(convert_missing_to_zero(data))

        if include_average_cpc == False:
            df = df.drop(columns=["Average Cost per Click"])

        df_monthly_search_volumes = pd.concat(
            monthly_search_volumes_dfs, ignore_index=True
        )

        return df, df_monthly_search_volumes


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


# Function to log better the exponential backoff retry
def format_timestamp(timestamp):
    # Calculate the time difference from the current time
    delta = timedelta(seconds=(time.time() - timestamp))
    days, seconds = divmod(delta.total_seconds(), 86400)  # 86400 seconds in a day
    hours, seconds = divmod(seconds, 3600)  # 3600 seconds in an hour
    minutes, seconds = divmod(seconds, 60)
    microseconds = delta.microseconds
    return f"{int(days):02}:{int(hours):02}:{int(minutes):02}:{int(seconds):02}.{microseconds:06}"


def convert_missing_to_zero(data):
    # Convert missing values to 0
    for col in data:
        if isinstance(data[col][0], list):  # Check if the column contains arrays
            data[col] = [
                [0 if pd.isnull(item) else item for item in val] for val in data[col]
            ]
        else:
            data[col] = [0 if pd.isnull(val) else val for val in data[col]]

    df = pd.DataFrame(data)
    return df
