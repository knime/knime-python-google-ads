import knime.extension as knext
import re


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
    def __init__(self, client, enums_module):
        self.client = client
        self.enums_module = enums_module
        self.enum_value_map = {}  # enum_type → {int: label}
        self._column_to_field_map = {}  # human-readable column name → GAQL field

    def _extract_field_names(self, query: str) -> list[str]:
        # Extracts all field names from the SELECT part of the GAQL query.
        match = re.search(r"SELECT\s+(.*?)\s+FROM", query, re.IGNORECASE | re.DOTALL)
        if not match:
            return []
        fields = match.group(1)

        # Returns a list of unique field names in the format resource.field
        return list(set(re.findall(r"\b([a-z_]+(?:\.[a-z_]+)+)\b", fields)))

    def get_enum_types_for_fields(self, field_names: list[str]) -> dict[str, str]:
        """
        For each field in field_names, queries the GoogleAdsFieldService to determine if it is an enum.
        Returns a mapping of field name to enum type string (e.g., 'CampaignStatusEnum.CampaignStatus').
        """
        field_service = self.client.get_service("GoogleAdsFieldService")
        result = {}

        # Google Ads API allows querying up to 50 fields at a time.
        chunks = [field_names[i : i + 50] for i in range(0, len(field_names), 50)]
        for chunk in chunks:
            quoted_names = ",".join(f'"{name}"' for name in chunk)
            query = f"SELECT name, data_type, type_url WHERE name IN ({quoted_names})"
            request = self.client.get_type("SearchGoogleAdsFieldsRequest")
            request.query = query

            try:
                response = field_service.search_google_ads_fields(request=request)
                for field in response:
                    # data_type == 5 indicates ENUM type
                    if field.data_type == 5:  # ENUM
                        type_url = getattr(field, "type_url", None)
                        enum_type = None
                        # Parse the type_url to get the enum type string
                        if type_url and "enums" in type_url:
                            parts = type_url.split(".")
                            if len(parts) >= 2:
                                enum_type = f"{parts[-2]}.{parts[-1]}"

                        if enum_type:
                            result[field.name] = enum_type
            except Exception:
                # If the API call fails, skip and continue
                pass

        return result

    def _resolve_enum_class(self, dotted_name: str):
        """
        Resolves a dotted enum type name (e.g., 'CampaignStatusEnum.CampaignStatus')
        to the actual enum class using the client enums module.
        """

        obj = self.client.enums
        for part in dotted_name.split("."):
            obj = getattr(obj, part)

        return obj

    def _load_enum_mapping(self, enum_type_name: str) -> dict[int, str]:
        """
        Loads and caches a mapping from enum integer values to human-readable strings
        for a given enum type.
        """
        if enum_type_name in self.enum_value_map:
            return self.enum_value_map[enum_type_name]

        try:
            enum_class = self._resolve_enum_class(enum_type_name)
            # Convert enum keys to title case strings for user-friendly display
            mapping = {value: name.replace("_", " ").title() for name, value in enum_class.items()}
            self.enum_value_map[enum_type_name] = mapping
            return mapping

        except Exception:
            # If enum resolution fails, return empty mapping
            return {}

    def build_column_to_field_map(self, df_columns, field_names):
        """
        Builds a mapping from DataFrame column names (human-readable) to GAQL field names.
        This is used to match DataFrame columns to their original GAQL fields.
        """
        mapping = {}
        for field in field_names:
            # Convert GAQL field name to a human-readable format (title case, spaces)
            readable = field.replace(".", " ").replace("_", " ").title()
            mapping[readable] = field
        self._column_to_field_map = mapping

    def process_dataframe(self, df, gaql_query: str) -> None:
        """
        Main entry point for processing the DataFrame:
        - Extracts field names from the GAQL query.
        - Determines which fields are enums and loads their mappings.
        - Replaces enum values in the DataFrame with human-readable strings.
        - Renames columns and converts micros fields to standard units.
        """
        field_names = self._extract_field_names(gaql_query)
        enum_field_map = self.get_enum_types_for_fields(field_names)
        self.build_column_to_field_map(df.columns, field_names)

        for col in df.columns:
            gaql_field = self._column_to_field_map.get(col)
            enum_type = enum_field_map.get(gaql_field)
            # If the column corresponds to an enum field, replace its values

            if enum_type:
                value_map = self._load_enum_mapping(enum_type)
                if value_map:
                    try:
                        df[col] = df[col].map(value_map).fillna(df[col])
                    except Exception as e:
                        # If mapping fails, leave values unchanged
                        pass

        # After processing enums, handle column renaming and micros conversion
        self.rename_columns(df)

    def rename_columns(self, df):
        """
        Renames columns for user-friendliness and converts known micros fields to standard units.
        - Strips 'Metrics ' prefix from column names.
        - Converts known micros fields (from hardcoded list) by dividing by 1,000,000.
        - Renames columns to remove 'micros' suffix and extra spaces.
        """
        rename_map = {}

        # NOTE: There is no programmatic way to detect "micros" fields from the Google Ads API metadata.
        # The API does not provide a flag or attribute indicating if a field is returned in micros.
        # Therefore, we use a hardcoded list of known micros fields based on the official documentation:
        # https://developers.google.com/google-ads/api/fields/v20/metrics
        micros_fields = {
            "average_cpc",
            "average_cpm",
            "average_cpv",
            "average_cpa",
            "cost_per_all_conversions",
            "cost_per_conversion",
            "cost_per_current_model_attributed_conversion",
            "cost_micros",
            "amount_micros",
            "budget_micros",
            "target_cpa_micros",
            "target_cpm_micros",
            "target_roas_micros",
            "value_per_all_conversions",
            "value_per_conversion",
            # Add more as needed from the API docs
        }

        for col in df.columns:
            new_col = col
            # Remove 'Metrics ' prefix before normalization
            col_no_prefix = re.sub(r"^\s*metrics\s+", "", col, flags=re.IGNORECASE)
            col_norm = col_no_prefix.lower().replace(" ", "_")

            # Convert micros fields (by suffix or known field name)
            if (col_norm.endswith("micros") or col_norm in micros_fields) and df[col].dtype in (
                "int64",
                "float64",
                "double",
            ):
                df[col] = df[col] / 1_000_000
                new_col = re.sub(r"(?i)micros$", "", new_col).strip()

            # Remove 'Metrics ' prefix (case-insensitive, ignore leading spaces)
            new_col = re.sub(r"^\s*metrics\s+", "", new_col, flags=re.IGNORECASE).strip()
            if new_col != col:
                rename_map[col] = new_col

        if rename_map:
            df.rename(columns=rename_map, inplace=True)
