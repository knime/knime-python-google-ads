import knime.extension as knext
import logging
import re
import importlib


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
        self._enum_field_cache = {}  # field name → enum type

    def _extract_field_names(self, query: str) -> list[str]:
        match = re.search(r"SELECT\s+(.*?)\s+FROM", query, re.IGNORECASE | re.DOTALL)
        if not match:
            return []
        fields = match.group(1)
        return list(set(re.findall(r"\b([a-z_]+\.[a-z_]+)\b", fields)))

    def _enum_to_module_path(self, outer_enum: str) -> str:
        base = outer_enum.removesuffix("Enum")
        return re.sub(r"(?<!^)(?=[A-Z])", "_", base).lower()

    def _load_enum_mapping(self, enum_type_name: str) -> dict[int, str]:
        if enum_type_name in self.enum_value_map:
            return self.enum_value_map[enum_type_name]

        try:
            outer_enum, inner_enum = enum_type_name.split(".")
            module_snake = self._enum_to_module_path(outer_enum)
            module_path = f"google.ads.googleads.v18.enums.types.{module_snake}"
            self.logger.warning(f"Trying to import enum module: '{module_path}' for '{enum_type_name}'")

            enum_module = importlib.import_module(module_path)
            outer_class = getattr(enum_module, outer_enum, None)

            if not outer_class:
                self.logger.warning(f"Enum outer class '{outer_enum}' not found in module.")
                return {}

            enum_class = getattr(outer_class, inner_enum, None)

            if not enum_class or not hasattr(enum_class, "__members__"):
                self.logger.warning(
                    f"Enum class '{enum_type_name}' resolved to {enum_class} — it has no '__members__'."
                )
                return {}

            mapping = {
                member.value: member.name.replace("_", " ").title() for member in enum_class.__members__.values()
            }

            self.enum_value_map[enum_type_name] = mapping
            return mapping

        except Exception as e:
            self.logger.warning(f"Failed to load enum '{enum_type_name}': {e}")
            return {}

    def _guess_enum_type_from_field(self, field_name: str) -> str | None:
        parts = field_name.split(".")
        if len(parts) != 2:
            return None

        resource, attr = parts

        def to_pascal(s):
            return "".join(part.capitalize() for part in s.split("_"))

        guesses = [
            f"{to_pascal(resource)}Enum.{to_pascal(resource)}{to_pascal(attr)}",
            f"{to_pascal(resource)}{to_pascal(attr)}Enum.{to_pascal(resource)}{to_pascal(attr)}",
            f"{to_pascal(resource)}{to_pascal(attr)}Enum.{to_pascal(attr)}",
            f"{to_pascal(attr)}Enum.{to_pascal(attr)}",
        ]

        for guess in guesses:
            if self._load_enum_mapping(guess):
                self.logger.warning(f"Guessed enum type for '{field_name}': {guess}")
                return guess

        self.logger.warning(f"Could not infer enum type for field '{field_name}'.")
        return None

    def get_enum_types_for_fields(self, field_names: list[str]) -> dict[str, str]:
        field_service = self.client.get_service("GoogleAdsFieldService")
        result = {}

        uncached_fields = [f for f in field_names if f not in self._enum_field_cache]

        if uncached_fields:
            chunks = [uncached_fields[i : i + 50] for i in range(0, len(uncached_fields), 50)]
            for chunk in chunks:
                quoted_names = ",".join(f'"{name}"' for name in chunk)
                query = f"SELECT name, data_type WHERE name IN ({quoted_names})"
                request = self.client.get_type("SearchGoogleAdsFieldsRequest")
                request.query = query

                try:
                    response = field_service.search_google_ads_fields(request=request)
                    for field in response:
                        if field.data_type == 5:  # ENUM
                            enum_type = getattr(field, "enum_type", None) or self._guess_enum_type_from_field(
                                field.name
                            )
                            self._enum_field_cache[field.name] = enum_type
                        else:
                            self._enum_field_cache[field.name] = None
                except Exception as e:
                    self.logger.warning(f"Failed to query enum types: {e}")

        for field in field_names:
            enum_type = self._enum_field_cache.get(field)
            if enum_type:
                result[field] = enum_type

        return result

    def process_dataframe(self, df, gaql_query: str) -> None:
        field_names = self._extract_field_names(gaql_query)
        enum_field_map = self.get_enum_types_for_fields(field_names)

        for col in df.columns:
            normalized = col.lower().replace(" ", "_").replace("_", ".", 1)
            enum_type = enum_field_map.get(normalized)

            if enum_type:
                value_map = self._load_enum_mapping(enum_type)
                if value_map:
                    try:
                        df[col] = df[col].map(value_map).fillna(df[col])
                        self.logger.warning(f"Replaced enum values in column: '{col}'")
                    except Exception as e:
                        self.logger.warning(f"Failed to map enum values for '{col}': {e}")
                else:
                    self.logger.warning(f"No enum values matched for column: '{col}' — values unchanged.")

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
