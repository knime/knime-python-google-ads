import logging
import csv
import os
import knime.extension as knext
import pandas as pd
from collections import deque
import time
import random
from itertools import islice
import numpy as np
from datetime import timedelta
from google.ads.googleads.v16.errors.types.quota_error import QuotaErrorEnum
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v16.services.types.keyword_plan_idea_service import (
    GenerateKeywordIdeasRequest,
)
from util.utils import check_canceled


LOGGER = logging.getLogger(__name__)


###########################################################
###### Utils for google_ads_keyword_ideas node############
###########################################################


# Read the CSV from the data folder. For now we Harcoded the language codes since they are not changing frequently
def read_csv():
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to the CSV file relative to the current script
    csv_file_path = os.path.join(script_dir, "../../data/language_codes.csv")

    language_name_to_criterion_id = {}

    with open(csv_file_path, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            language_name = row["Language name"]
            criterion_id = row["Criterion ID"]

            language_name_to_criterion_id[language_name] = criterion_id

    return language_name_to_criterion_id


# Reading the CSV file and creating dictionaries and list
language_name_to_criterion_id = read_csv()


# Function to get the criterion ID based on the language name
def get_criterion_id(language_name):
    language_name = language_name.title()
    return language_name_to_criterion_id.get(language_name, "Language name not found")


# Enum options for the language selection
class LanguageSelection(knext.EnumParameterOptions):
    """
    Subclass of knext.EnumParameterOptions to handle language options from a CSV file.
    """

    ARABIC = ("Arabic", "The Arabic language (1019)")
    BENGALI = ("Bengali", "The Bengali language (1056)")
    BULGARIAN = ("Bulgarian", "The Bulgarian language (1020)")
    CATALAN = ("Catalan", "The Catalan language (1038)")
    CHINESE_SIMPLIFIED = (
        "Chinese (simplified)",
        "The Chinese (simplified) language (1017)",
    )
    CHINESE_TRADITIONAL = (
        "Chinese (traditional)",
        "The Chinese (traditional) language (1018)",
    )
    CROATIAN = ("Croatian", "The Croatian language (1039)")
    CZECH = ("Czech", "The Czech language (1021)")
    DANISH = ("Danish", "The Danish language (1009)")
    DUTCH = ("Dutch", "The Dutch language (1010)")
    ENGLISH = ("English", "The English language (1000)")
    ESTONIAN = ("Estonian", "The Estonian language (1043)")
    FILIPINO = ("Filipino", "The Filipino language (1042)")
    FINNISH = ("Finnish", "The Finnish language (1011)")
    FRENCH = ("French", "The French language (1002)")
    GERMAN = ("German", "The German language (1001)")
    GREEK = ("Greek", "The Greek language (1022)")
    GUJARATI = ("Gujarati", "The Gujarati language (1072)")
    HEBREW = ("Hebrew", "The Hebrew language (1027)")
    HINDI = ("Hindi", "The Hindi language (1023)")
    HUNGARIAN = ("Hungarian", "The Hungarian language (1024)")
    ICELANDIC = ("Icelandic", "The Icelandic language (1026)")
    INDONESIAN = ("Indonesian", "The Indonesian language (1025)")
    ITALIAN = ("Italian", "The Italian language (1004)")
    JAPANESE = ("Japanese", "The Japanese language (1005)")
    KANNADA = ("Kannada", "The Kannada language (1086)")
    KOREAN = ("Korean", "The Korean language (1012)")
    LATVIAN = ("Latvian", "The Latvian language (1028)")
    LITHUANIAN = ("Lithuanian", "The Lithuanian language (1029)")
    MALAY = ("Malay", "The Malay language (1102)")
    MALAYALAM = ("Malayalam", "The Malayalam language (1098)")
    MARATHI = ("Marathi", "The Marathi language (1101)")
    NORWEGIAN = ("Norwegian", "The Norwegian language (1013)")
    PERSIAN = ("Persian", "The Persian language (1064)")
    POLISH = ("Polish", "The Polish language (1030)")
    PORTUGUESE = ("Portuguese", "The Portuguese language (1014)")
    PUNJABI = ("Punjabi", "The Punjabi language (1110)")
    ROMANIAN = ("Romanian", "The Romanian language (1032)")
    RUSSIAN = ("Russian", "The Russian language (1031)")
    SERBIAN = ("Serbian", "The Serbian language (1035)")
    SLOVAK = ("Slovak", "The Slovak language (1033)")
    SLOVENIAN = ("Slovenian", "The Slovenian language (1034)")
    SPANISH = ("Spanish", "The Spanish language (1003)")
    SWEDISH = ("Swedish", "The Swedish language (1015)")
    TAMIL = ("Tamil", "The Tamil language (1130)")
    TELUGU = ("Telugu", "The Telugu language (1131)")
    THAI = ("Thai", "The Thai language (1044)")
    TURKISH = ("Turkish", "The Turkish language (1037)")
    UKRAINIAN = ("Ukrainian", "The Ukrainian language (1036)")
    URDU = ("Urdu", "The Urdu language (1041)")
    VIETNAMESE = ("Vietnamese", "The Vietnamese language (1040)")


class NewKeywordIdeasMode(knext.EnumParameterOptions):
    KEYWORDS = (
        "Keywords",
        "Generate new keyword ideas using specific keywords, such as **meal delivery**, for a food delivery business. Find more information about [best practices for discovering new keywords](https://support.google.com/google-ads/answer/9247190?hl=en&_gl=1*1bj7r33*_ga*Mjk1NDA2MjUxLjE2OTU2NTE3ODY.*_ga_V9K47ZG8NP*MTcxODQ5MjkwNi4xNC4wLjE3MTg0OTI5MDYuNjAuMC4w)",
    )
    URL = (
        "Webpage URLs",
        "Generate new keyword ideas using a specific webpage URL, such as https://www.example.com.",
    )


###### START of methods to handle the execution in chunks to avoid resource exhaustion ######

# Initialize a deque to keep track of request timestamps
request_timestamps = deque(maxlen=60)

# Define a function to retry the request with exponential backoff if a RESOURCE_EXHAUSTED error occurs.
# Max requests per min are 60: 1 request per second
# Quota reference website: https://developers.google.com/google-ads/api/docs/best-practices/quotas#planning_services


def exponential_backoff_retry(func, max_attempts=5, initial_delay=5):
    delay = initial_delay
    LOGGER.warning(f"Initial delay: {delay}")

    for attempt in range(max_attempts):
        LOGGER.warning(f"Attempt {attempt+1} of {max_attempts}")
        try:
            # Rate limiting check
            if request_timestamps:
                time_since_last_request = time.time() - request_timestamps[-1]
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
            request_timestamps.append(time.time())
            formatted_request_timestamps = [
                format_timestamp(ts) for ts in request_timestamps
            ]
            LOGGER.warning(f"Request timestamps: {formatted_request_timestamps}")
            LOGGER.warning(f"Length of request timestamps: {len(request_timestamps)}")
            return result

        except GoogleAdsException as ex:
            error_code = ex.failure.errors[0].error_code
            LOGGER.warning(f"Error code: {error_code.quota_error}")
            if (
                error_code.quota_error == QuotaErrorEnum.QuotaError.RESOURCE_EXHAUSTED
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
                    max_attemps_error_mssg = (
                        "Max attempts reached, raising the exception."
                    )
                    raise knext.InvalidParametersError(max_attemps_error_mssg)
            else:
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


# Define a function to chunk the location
def chunked(iterable, size):
    it = iter(iterable)
    return iter(lambda: tuple(islice(it, size)), ())


# Function to parse monthly search volumes and convert to DataFrame
def parse_monthly_search_volumes(
    monthly_search_volumes, keyword, iteration_id, location_ids
):
    rows = [
        {
            "keyword": keyword,
            "month": metrics.month,
            "year": metrics.year,
            "monthly searches": metrics.monthly_searches,
            "iteration_id": iteration_id,
            "location_ids": location_ids,
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
    keyword_ideas_mode,
    exec_context,
    rows_per_chunk,
):
    location_chunks = chunked(location_rns, rows_per_chunk)
    LOGGER.warning(f"Location chunks: {location_chunks}")
    all_keyword_ideas = []
    iteration_ids = []
    # Create empty lists to store list of location IDs used on each iteration
    location_ids = []

    # Clear the request timestamps for a new batch of requests
    request_timestamps.clear()
    LOGGER.warning(f"Request timestamps cleared: {request_timestamps}")

    # Process data in smaller batches to avoid memory issues
    batch_size = 80000  # Adjust this size as needed

    aggregated_data = []
    aggregated_monthly_volumes = []

    for iteration_id, chunk in enumerate(location_chunks, start=1):
        # cancel the execution if the user cancels the execution
        check_canceled(exec_context)

        def request_keyword_ideas(chunk):
            # [Preparing the request]
            # Only one of the fields "url_seed", "keyword_seed" can be set on the request, depending on whether
            # keywords, a page_url were passed to this function.
            if keyword_ideas_mode == "URL":

                # Check for missing values in keyword_texts
                if any(url is None or pd.isna(url) for url in keyword_texts):
                    raise knext.InvalidParametersError(
                        "One or more URLs from the provided input table are missing values. The Google Ads API does not allow this. Tip: To handle missing values, add a Missing Value node upstream."
                    )

                for url in keyword_texts:
                    # Create a new request object for each URL
                    # cancel the execution if the user cancels the execution
                    check_canceled(exec_context)

                    request: GenerateKeywordIdeasRequest
                    request = client.get_type("GenerateKeywordIdeasRequest")
                    request.customer_id = account_id
                    request.language = language_rn
                    request.geo_target_constants.extend(chunk)
                    request.keyword_plan_network = keyword_plan_network
                    request.include_adult_keywords = include_adult_keywords

                    # Properly create and set the year_month_range within historical_metrics_options
                    historical_metrics_options = client.get_type(
                        "HistoricalMetricsOptions"
                    )
                    year_month_range = historical_metrics_options.year_month_range

                    year_month_range.start.year = date_start.year
                    # The month is 1-based, so we need to add 1 to the month to get the correct value.
                    year_month_range.start.month = date_start.month + 1
                    year_month_range.end.year = date_end.year
                    year_month_range.end.month = date_end.month + 1

                    request.historical_metrics_options.CopyFrom(
                        historical_metrics_options
                    )
                    request.historical_metrics_options.include_average_cpc = (
                        include_average_cpc
                    )

                    # Set the URL seed for this specific URL
                    request.url_seed.url = url

                    # Generate keyword ideas for the current URL
                    keyword_ideas_pager = (
                        keyword_plan_idea_service.generate_keyword_ideas(
                            request=request
                        )
                    )

                    # Collect keyword ideas into the result list
                    keyword_ideas = list(keyword_ideas_pager)
                    all_keyword_ideas.extend(keyword_ideas)

            elif keyword_ideas_mode == "KEYWORDS":
                # Check for missing values in keyword_texts
                if any(kw is None or pd.isna(kw) for kw in keyword_texts):
                    raise knext.InvalidParametersError(
                        "One or more keywords from the provided input table are missing values. The Google Ads API does not allow this. Tip: To handle missing values, add a Missing Value node upstream."
                    )

                # Split keyword_texts into chunks of 20 keywords each
                for i in range(0, len(keyword_texts), 20):
                    chunked_keywords = keyword_texts[i : i + 20]

                    # cancel the execution if the user cancels the execution
                    check_canceled(exec_context)
                    # Create a single request for all keyword texts
                    request = client.get_type("GenerateKeywordIdeasRequest")
                    request.customer_id = account_id
                    request.language = language_rn
                    request.geo_target_constants.extend(chunk)
                    request.keyword_plan_network = keyword_plan_network
                    request.include_adult_keywords = include_adult_keywords

                    # Properly create and set the year_month_range within historical_metrics_options
                    historical_metrics_options = client.get_type(
                        "HistoricalMetricsOptions"
                    )
                    year_month_range = historical_metrics_options.year_month_range
                    year_month_range.start.year = date_start.year
                    year_month_range.start.month = date_start.month + 1
                    year_month_range.end.year = date_end.year
                    year_month_range.end.month = date_end.month + 1
                    request.historical_metrics_options.CopyFrom(
                        historical_metrics_options
                    )
                    request.historical_metrics_options.include_average_cpc = (
                        include_average_cpc
                    )

                    # Set the keyword seed with all provided keywords
                    request.keyword_seed.keywords.extend(chunked_keywords)

                    # Generate keyword ideas for the list of keywords
                    keyword_ideas_pager = (
                        keyword_plan_idea_service.generate_keyword_ideas(
                            request=request
                        )
                    )

                    # Collect keyword ideas into the result list
                    keyword_ideas = list(keyword_ideas_pager)
                    all_keyword_ideas.extend(keyword_ideas)

            return all_keyword_ideas

        LOGGER.warning(f"Chunk: {chunk}")

        # Make the request with retry logic
        keyword_ideas_pager = exponential_backoff_retry(
            lambda c=chunk: request_keyword_ideas(c)
        )
        # LOGGER.warning(f"Keyword ideas pager: {keyword_ideas_pager}") is creating a lot of logs
        LOGGER.warning(f"Iteration ID: {iteration_id}")

        keyword_ideas = list(keyword_ideas_pager)
        all_keyword_ideas.extend(keyword_ideas)
        LOGGER.warning(f"len(all_keyword_ideas): {len(all_keyword_ideas)}")
        iteration_ids.extend([iteration_id] * len(keyword_ideas))
        # Append the location IDs (list) used on each iteration
        location_ids.extend([chunk] * len(keyword_ideas))
        # LOGGER.warning(f"Location IDs: {location_ids}") is creating a lot of logs

        # Process the batch if it reaches the batch size
        if len(all_keyword_ideas) >= batch_size:
            df_batch, df_monthly_batch = process_batch(
                all_keyword_ideas, iteration_ids, location_ids, include_average_cpc
            )
            aggregated_data.append(df_batch)
            aggregated_monthly_volumes.append(df_monthly_batch)
            all_keyword_ideas = []
            iteration_ids = []
            location_ids = []
    # Process any remaining keyword ideas
    if all_keyword_ideas:
        df_batch, df_monthly_batch = process_batch(
            all_keyword_ideas, iteration_ids, location_ids, include_average_cpc
        )
        aggregated_data.append(df_batch)
        aggregated_monthly_volumes.append(df_monthly_batch)

    df_keyword_ideas_aggregated = pd.concat(aggregated_data, ignore_index=True)
    df_monthly_search_volumes = pd.concat(aggregated_monthly_volumes, ignore_index=True)

    return df_keyword_ideas_aggregated, df_monthly_search_volumes


def process_batch(all_keyword_ideas, iteration_ids, location_ids, include_average_cpc):

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
    for idea, iteration_id, location_id in zip(
        all_keyword_ideas, iteration_ids, location_ids
    ):
        # for idea in all_keyword_ideas:

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
            micros_to_currency(idea.keyword_idea_metrics.high_top_of_page_bid_micros)
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

        monthly_df = parse_monthly_search_volumes(
            idea.keyword_idea_metrics.monthly_search_volumes,
            idea.text,
            iteration_id,
            location_id,
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

    df_monthly_search_volumes = pd.concat(monthly_search_volumes_dfs, ignore_index=True)
    return df, df_monthly_search_volumes


# Function to log better the exponential backoff retry


def format_timestamp(seconds):
    delta = timedelta(seconds=seconds)
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    microseconds = delta.microseconds
    return f"{int(days):02}:{int(hours):02}:{int(minutes):02}:{int(seconds):02}.{microseconds:06}"


###### END of the methods to handle the execution in chunks to avoid resource exhaustion ######


###### START of Basic methods for the google_ads_keyword_ideas node ######


# Function to map location ids to resource names
def map_locations_ids_to_resource_names(port_object, location_ids):
    client = port_object

    # build_resource_name_client: GeoTargetConstantServiceClient
    build_resource_name_client = client.get_service("GeoTargetConstantService")
    build_resource_name = build_resource_name_client.geo_target_constant_path
    return [build_resource_name(location_id) for location_id in location_ids]


# Function to use in the date_start ane date_end validators to check if the input date is greater than four years from the current date
def datediff_in_years(date1, date2):
    return abs(date1.year - date2.year)


# Kind of dictionary to map the competition values to text
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


# Convert micros to currency
def micros_to_currency(micros):
    return micros / 1_000_000


# Function to convert missing values to 0
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


###### END of basic methods for the google_ads_keyword_ideas node ######
