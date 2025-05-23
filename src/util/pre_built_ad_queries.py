import knime.extension as knext
import logging
import re
import importlib
import pandas as pd


Logger = logging.getLogger(__name__)


class HardCodedQueries(knext.EnumParameterOptions):
    CAMPAIGNS = ("Campaigns", "The default Campaigns overview screen in the UI.")
    ADGROUPS = ("Ad Groups", "The default Ad groups overview screen in the UI.")
    ADS = (
        "Ads",
        "The default Ads overview screen in the UI. Note that this particular query specifically fetches the individual components of an Expanded Text Ad, which are seen rendered together in the UI screen's **Ad** column.",
    )
    SEARCHKEYWORDS = ("Search Keywords", "The default Search keywords overview screen in the UI.")
    SEARCHTERMS = ("Search Terms", "The default Search terms overview screen in the UI.")
    AUDIENCE = (
        "Audiences",
        "The default Audiences overview screen in the UI. Note that the reporting API returns audiences by their criterion IDs. To get their display names, look up the IDs in the reference tables provided in the [Codes and formats page](https://developers.google.com/google-ads/api/data/codes-formats). You can key off the **ad_group_criterion.type** field to determine which criteria type table to use.",
    )
    AGE = ("Age (Demographics)", "The default Age demographics overview screen in the UI.")
    GENDER = ("Gender (Demographics)", "The default Gender demographics overview screen in the UI.")
    LOCATION = (
        "Locations",
        "The default Locations overview screen in the UI. Note that the reporting API returns locations by their criterion IDs. To get their display names, look up the **campaign_criterion.location.geo_target_constant** in the [geo target data](https://developers.google.com/google-ads/api/data/geotargets), or use the API to query the **geo_target_constant resource**.",
    )


def get_query(name, start_date, end_date) -> str:
    query = mapping_queries[name].replace("$$start_date$$", str(start_date)).replace("$$end_date$$", str(end_date))
    return query


mapping_queries = {
    "CAMPAIGNS": """SELECT campaign.name,
                campaign_budget.amount_micros,
                campaign.status,
                campaign.optimization_score,
                campaign.advertising_channel_type,
                metrics.clicks,
                metrics.impressions,
                metrics.ctr,
                metrics.average_cpc,
                metrics.cost_micros,
                campaign.bidding_strategy_type
            FROM campaign
            WHERE segments.date BETWEEN '$$start_date$$' AND '$$end_date$$'
            AND campaign.status != 'REMOVED' 
        """,
    "ADGROUPS": """     SELECT ad_group.name,
                        campaign.name,
                        ad_group.status,
                        ad_group.type ,
                        metrics.clicks,
                        metrics.impressions,
                        metrics.ctr,
                        metrics.average_cpc,
                        metrics.cost_micros
                FROM ad_group
                WHERE segments.date BETWEEN '$$start_date$$' AND '$$end_date$$'
                AND ad_group.status != 'REMOVED'
        """,
    "ADS": """ SELECT ad_group_ad.ad.expanded_text_ad.headline_part1,
                ad_group_ad.ad.expanded_text_ad.headline_part2,
                ad_group_ad.ad.expanded_text_ad.headline_part3,
                ad_group_ad.ad.final_urls,
                ad_group_ad.ad.expanded_text_ad.description,
                ad_group_ad.ad.expanded_text_ad.description2,
                campaign.name,
                ad_group.name,
                ad_group_ad.policy_summary.approval_status,
                ad_group_ad.ad.type,
                metrics.clicks,
                metrics.impressions,
                metrics.ctr,
                metrics.average_cpc,
                metrics.cost_micros
            FROM ad_group_ad
            WHERE segments.date BETWEEN '$$start_date$$' AND '$$end_date$$'
            AND ad_group_ad.status != 'REMOVED'
        """,
    "SEARCHKEYWORDS": """SELECT ad_group_criterion.keyword.text,
                campaign.name,
                ad_group.name,
                ad_group_criterion.system_serving_status,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.approval_status,
                ad_group_criterion.final_urls,
                metrics.clicks,
                metrics.impressions,
                metrics.ctr,
                metrics.average_cpc,
                metrics.cost_micros
            FROM keyword_view
            WHERE segments.date BETWEEN '$$start_date$$' AND '$$end_date$$'
            AND ad_group_criterion.status != 'REMOVED'
        """,
    "SEARCHTERMS": """SELECT search_term_view.search_term,
                    segments.keyword.info.match_type,
                    search_term_view.status,
                    campaign.name,
                    ad_group.name,
                    metrics.clicks,
                    metrics.impressions,
                    metrics.ctr,
                    metrics.average_cpc,
                    metrics.cost_micros,
                    campaign.advertising_channel_type
            FROM search_term_view
            WHERE segments.date BETWEEN '$$start_date$$' AND '$$end_date$$'
        """,
    "AUDIENCE": """SELECT ad_group_criterion.resource_name,
                ad_group_criterion.type,
                campaign.name,
                ad_group.name,
                ad_group_criterion.system_serving_status,
                ad_group_criterion.bid_modifier,
                metrics.clicks,
                metrics.impressions,
                metrics.ctr,
                metrics.average_cpc,
                metrics.cost_micros,
                campaign.advertising_channel_type
            FROM ad_group_audience_view
            WHERE segments.date BETWEEN '$$start_date$$' AND '$$end_date$$'
        """,
    "AGE": """SELECT ad_group_criterion.age_range.type,
                campaign.name,
                ad_group.name,
                ad_group_criterion.system_serving_status,
                ad_group_criterion.bid_modifier,
                metrics.clicks,
                metrics.impressions,
                metrics.ctr,
                metrics.average_cpc,
                metrics.cost_micros,
                campaign.advertising_channel_type
            FROM age_range_view
            WHERE segments.date BETWEEN '$$start_date$$' AND '$$end_date$$'
        """,
    "GENDER": """SELECT ad_group_criterion.gender.type,
                campaign.name,
                ad_group.name,
                ad_group_criterion.system_serving_status,
                ad_group_criterion.bid_modifier,
                metrics.clicks,
                metrics.impressions,
                metrics.ctr,
                metrics.average_cpc,
                metrics.cost_micros,
                campaign.advertising_channel_type
            FROM gender_view
            WHERE segments.date BETWEEN '$$start_date$$' AND '$$end_date$$'                  
        """,
    "LOCATION": """SELECT campaign_criterion.location.geo_target_constant,
                campaign.name,
                campaign_criterion.bid_modifier,
                metrics.clicks,
                metrics.impressions,
                metrics.ctr,
                metrics.average_cpc,
                metrics.cost_micros
            FROM location_view
            WHERE segments.date BETWEEN '$$start_date$$' AND '$$end_date$$'
            AND campaign_criterion.status != 'REMOVED'
        """,
}


class FieldInspector:
    def __init__(self, client, enums_module, logger):
        self.client = client
        self.enums_module = enums_module
        self.logger = logger
        self.enum_value_map = {}  # enum_type → {int: label}
        self._column_to_field_map = {}  # human-readable column name → GAQL field

    def _extract_field_names(self, query: str) -> list[str]:
        match = re.search(r"SELECT\s+(.*?)\s+FROM", query, re.IGNORECASE | re.DOTALL)
        if not match:
            return []
        fields = match.group(1)
        return list(set(re.findall(r"\b([a-z_]+(?:\.[a-z_]+)+)\b", fields)))

    def get_enum_types_for_fields(self, field_names: list[str]) -> dict[str, str]:
        self.logger.warning(f"Getting enum types for fields: {field_names}")
        field_service = self.client.get_service("GoogleAdsFieldService")
        result = {}

        chunks = [field_names[i : i + 50] for i in range(0, len(field_names), 50)]
        for chunk in chunks:
            quoted_names = ",".join(f'"{name}"' for name in chunk)
            query = f"SELECT name, data_type, type_url WHERE name IN ({quoted_names})"
            request = self.client.get_type("SearchGoogleAdsFieldsRequest")
            request.query = query

            try:
                response = field_service.search_google_ads_fields(request=request)
                for field in response:
                    self.logger.warning(f"Field '{field.name}' has data_type: {field.data_type}")
                    if field.data_type == 5:  # ENUM
                        type_url = getattr(field, "type_url", None)
                        enum_type = None
                        if type_url and "enums" in type_url:
                            parts = type_url.split(".")
                            if len(parts) >= 2:
                                enum_type = f"{parts[-2]}.{parts[-1]}"

                        if enum_type:
                            self.logger.warning(f"Enum type for field '{field.name}': {enum_type}")
                            result[field.name] = enum_type
            except Exception as e:
                self.logger.warning(f"Failed to query enum types: {e}")

        return result

    def _resolve_enum_class(self, dotted_name: str):
        obj = self.client.enums
        self.logger.warning(f"[Resolver] Starting at: client.enums")

        for part in dotted_name.split("."):
            self.logger.warning(f"[Resolver] Traversing: '{part}' on {obj}")
            obj = getattr(obj, part)

        self.logger.warning(f"[Resolver] Final resolved enum class: {obj}")
        return obj

    def _load_enum_mapping(self, enum_type_name: str) -> dict[int, str]:
        if enum_type_name in self.enum_value_map:
            self.logger.warning(f"[Loader] Cache hit for '{enum_type_name}'")
            return self.enum_value_map[enum_type_name]

        try:
            self.logger.warning(f"[Loader] Resolving enum class for '{enum_type_name}'")
            enum_class = self._resolve_enum_class(enum_type_name)

            self.logger.warning(f"[Loader] Enum class keys: {list(enum_class.keys())}")

            mapping = {value: name.replace("_", " ").title() for name, value in enum_class.items()}

            self.logger.warning(f"[Loader] Final mapping: {mapping}")
            self.enum_value_map[enum_type_name] = mapping
            return mapping

        except Exception as e:
            self.logger.warning(f"[Loader] Failed to load enum mapping for '{enum_type_name}': {e}")
            return {}

    def build_column_to_field_map(self, df_columns, field_names):
        mapping = {}
        for field in field_names:
            readable = field.replace(".", " ").replace("_", " ").title()
            mapping[readable] = field
        self._column_to_field_map = mapping
        self.logger.warning(f"Column to GAQL field map: {self._column_to_field_map}")

    def process_dataframe(self, df, gaql_query: str) -> None:
        field_names = self._extract_field_names(gaql_query)
        self.logger.warning(f"Extracted field names from GAQL: {field_names}")
        enum_field_map = self.get_enum_types_for_fields(field_names)
        self.logger.warning(f"Enum field map: {enum_field_map}")

        self.build_column_to_field_map(df.columns, field_names)

        for col in df.columns:
            gaql_field = self._column_to_field_map.get(col)
            enum_type = enum_field_map.get(gaql_field)
            self.logger.warning(f"Checking column '{col}' → GAQL field: '{gaql_field}' → enum: {enum_type}")

            if enum_type:
                value_map = self._load_enum_mapping(enum_type)
                self.logger.warning(f"Enum value map for '{col}': {value_map}")
                if value_map:
                    try:
                        df[col] = df[col].map(value_map).fillna(df[col])
                        self.logger.warning(f"Replaced enum values in column: '{col}'")
                    except Exception as e:
                        self.logger.warning(f"Failed to map enum values for '{col}': {e}")
                else:
                    self.logger.warning(f"No enum values matched for column: '{col}' — values unchanged.")

    def rename_columns(self, df):
        rename_map = {}
        for col in df.columns:
            new_col = col
            if col.lower().endswith("micros") and df[col].dtype in ("int64", "float64"):
                df[col] = df[col] / 1_000_000
                new_col = new_col.replace("Micros", "").replace("micros", "").strip()
                self.logger.warning(f"Converted micros field '{col}' to '{new_col}'")

            if new_col.startswith("Metrics "):
                new_col = new_col.replace("Metrics ", "", 1).strip()
                self.logger.warning(f"Renamed column '{col}' to '{new_col}'")

            if new_col != col:
                rename_map[col] = new_col

        if rename_map:
            df.rename(columns=rename_map, inplace=True)
            self.logger.warning(f"Applied column renaming: {rename_map}")
