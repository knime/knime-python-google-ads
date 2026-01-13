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

"""
Google Ads Search Terms Actions (Labs)

This node executes actions on search terms: add as negative keywords (cut wasted spend)
or promote to keywords (scale high-intent traffic).

Pure executor — all rules/filtering done upstream with KNIME nodes.
"""

import logging
from datetime import datetime

import knime.extension as knext
import pandas as pd
import google_ads_ext

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)
from util.utils import check_column, create_type_filer
# FieldInspector is used to convert Google Ads API enum integer values to their string names
# (e.g., KeywordMatchTypeEnum returns integers like 2, 3, 4 instead of "EXACT", "PHRASE", "BROAD")
from util.pre_built_ad_queries import FieldInspector

LOGGER = logging.getLogger(__name__)


# =============================================================================
# ENUMS
# =============================================================================


class ActionType(knext.EnumParameterOptions):
    """Action type options."""

    ADD_NEGATIVE = (
        "Add as Negative Keyword",
        "Block search term from triggering ads.",
    )
    PROMOTE_TO_KEYWORD = (
        "Promote to Keyword",
        "Add search term as a keyword to the ad group.",
    )


class NegativeScope(knext.EnumParameterOptions):
    """Negative keyword scope options."""

    CAMPAIGN = (
        "Campaign level",
        "Add negative keyword at campaign level (blocks across all ad groups).",
    )
    AD_GROUP = (
        "Ad Group level",
        "Add negative keyword at ad group level (blocks only in that ad group).",
    )


class MatchType(knext.EnumParameterOptions):
    """Keyword match type options."""

    EXACT = (
        "Exact",
        "Matches the exact search term only.",
    )
    PHRASE = (
        "Phrase",
        "Matches searches containing the phrase.",
    )
    BROAD = (
        "Broad",
        "Matches broad variations of the term.",
    )


class ExecutionMode(knext.EnumParameterOptions):
    """Execution mode options."""

    PREVIEW = (
        "Preview",
        "Generate proposed changes without applying them.",
    )
    APPLY = (
        "Apply",
        "Apply the changes to the Google Ads account.",
    )


# =============================================================================
# NODE DEFINITION
# =============================================================================


@knext.node(
    name="Google Ads Search Terms Actions (Labs)",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/gads-icon.png",
    category=google_ads_ext.main_category,
    keywords=[
        "Google",
        "Google Ads",
        "Search Terms",
        "Negative Keywords",
        "Keywords",
        "Optimization",
    ],
)
@knext.input_port(
    "Google Ads Connection",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API.",
    google_ad_port_type,
)
@knext.input_table(
    name="Search Terms",
    description="Table containing search terms with campaign and ad group resource names.",
)
@knext.output_table(
    name="Results",
    description="Results with status for each search term action.",
)
class GoogleAdsSearchTermsActions:
    """
    Executes actions on search terms: add as negative keywords or promote to keywords.

    This node is a **pure executor** — all decision logic (rules, filtering) should be done
    upstream using KNIME nodes like Rule Engine, Row Filter, or Joiner.

    **Actions**

    - **Add as Negative Keyword**: Block search terms from triggering ads. Can be added at
      campaign level (blocks across all ad groups) or ad group level.
    - **Promote to Keyword**: Add high-performing search terms as keywords to gain more control
      over bidding and ad relevance.

    **Workflow Example**

    1. Use **Google Ads Query** node to fetch search term performance data
    2. Join with CRM data (MQLs, opportunities) using **Joiner** node
    3. Apply rules using **Rule Engine** to decide which terms to act on
    4. Filter using **Row Filter** to keep only actionable terms
    5. Connect to this node to execute the actions

    **Preview Mode**

    Use Preview mode to review proposed changes before applying them. The output table
    will show what would happen without making any API calls.

    **Duplicate Handling**

    The node queries existing keywords/negatives before processing to detect duplicates.
    This prevents API errors and provides clear feedback in the output table. Keywords
    that already exist are marked with status "ALREADY_EXISTS_AS_KEYWORD" or
    "ALREADY_EXISTS_AS_NEGATIVE_KEYWORD" and processing continues with the remaining terms.

    **Performance for Large Accounts**

    The node uses batched API queries to efficiently check for duplicates, even in accounts
    with thousands of ad groups and keywords. The batch size can be adjusted in Advanced
    Settings if needed (default: 100 ad groups per query).

    **Tip: Bulk Negative Uploads**

    This node works with any text input—not just search terms. You can bulk-add negative
    keywords from a seed list, CSV, or Excel file. Just provide a text column with the
    campaign/ad group resource names and the node will create the negatives for you.
    Common use cases: competitor brand terms, industry blacklists, or proven negatives
    from previous campaigns.
    """

    # ==========================================================================
    # ACTION SELECTION
    # ==========================================================================

    action_type = knext.EnumParameter(
        label="Action",
        description="Choose whether to add search terms as negative keywords or promote them to keywords.",
        default_value=ActionType.ADD_NEGATIVE.name,
        enum=ActionType,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    )

    # ==========================================================================
    # NEGATIVE KEYWORD SETTINGS (shown when action = ADD_NEGATIVE)
    # ==========================================================================

    negative_scope = knext.EnumParameter(
        label="Scope",
        description="Choose whether to add negatives at campaign level or ad group level.",
        default_value=NegativeScope.CAMPAIGN.name,
        enum=NegativeScope,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    ).rule(
        knext.OneOf(action_type, [ActionType.ADD_NEGATIVE.name]),
        knext.Effect.SHOW,
    )

    negative_match_type = knext.EnumParameter(
        label="Negative Match Type",
        description="Match type for negative keywords.",
        default_value=MatchType.PHRASE.name,
        enum=MatchType,
    ).rule(
        knext.OneOf(action_type, [ActionType.ADD_NEGATIVE.name]),
        knext.Effect.SHOW,
    )

    # ==========================================================================
    # KEYWORD PROMOTION SETTINGS (shown when action = PROMOTE_TO_KEYWORD)
    # ==========================================================================

    promotion_match_type = knext.EnumParameter(
        label="Keyword Match Type",
        description="Match type for promoted keywords.",
        default_value=MatchType.EXACT.name,
        enum=MatchType,
    ).rule(
        knext.OneOf(action_type, [ActionType.PROMOTE_TO_KEYWORD.name]),
        knext.Effect.SHOW,
    )

    # ==========================================================================
    # COLUMN MAPPING
    # ==========================================================================

    search_term_column = knext.ColumnParameter(
        label="Search Term Text",
        description="Column containing the search term text.",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        column_filter=create_type_filer(knext.string()),
    )

    campaign_resource_column = knext.ColumnParameter(
        label="Campaign Resource Name",
        description="Column containing Google Ads campaign resource names (e.g., 'customers/123/campaigns/456').",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        column_filter=create_type_filer(knext.string()),
    )

    ad_group_resource_column = knext.ColumnParameter(
        label="Ad Group Resource Name",
        description="Column containing Google Ads ad group resource names (e.g., 'customers/123/adGroups/789'). Required for ad group negatives and keyword promotion.",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        column_filter=create_type_filer(knext.string()),
    ).rule(
        knext.Or(
            knext.OneOf(action_type, [ActionType.PROMOTE_TO_KEYWORD.name]),
            knext.OneOf(negative_scope, [NegativeScope.AD_GROUP.name]),
        ),
        knext.Effect.SHOW,
    )

    # ==========================================================================
    # EXECUTION
    # ==========================================================================

    execution_mode = knext.EnumParameter(
        label="Mode",
        description="Preview generates a report without changes. Apply executes the changes.",
        default_value=ExecutionMode.PREVIEW.name,
        enum=ExecutionMode,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    )

    # ==========================================================================
    # ADVANCED SETTINGS
    # ==========================================================================

    batch_size = knext.IntParameter(
        label="Batch Size for Duplicate Check",
        description="Number of ad groups/campaigns to query per API call when checking for existing keywords. "
                    "Higher values reduce API calls but increase query size. For accounts with thousands of "
                    "ad groups, a smaller batch size may be more reliable. Default of 100 works well for most accounts.",
        default_value=100,
        min_value=10,
        max_value=500,
        is_advanced=True,
    )

    # ==========================================================================
    # CONFIGURE
    # ==========================================================================

    def configure(
        self,
        configure_context: knext.ConfigurationContext,
        spec: GoogleAdObjectSpec,
        input_table_schema: knext.Schema,
    ) -> knext.Schema:
        """Validate configuration and define output schema."""

        # Validate required columns
        search_term_col = self.search_term_column
        campaign_col = self.campaign_resource_column
        ad_group_col = self.ad_group_resource_column

        if not search_term_col:
            raise knext.InvalidParametersError(
                "Select a column for 'Search Term Text'."
            )
        if not campaign_col:
            raise knext.InvalidParametersError(
                "Select a column for 'Campaign Resource Name'."
            )

        check_column(input_table_schema, search_term_col, knext.string(), "search term")
        check_column(input_table_schema, campaign_col, knext.string(), "campaign resource name")

        # Ad group column required for ad group negatives or keyword promotion
        requires_ad_group = (
            self.action_type == ActionType.PROMOTE_TO_KEYWORD.name
            or (
                self.action_type == ActionType.ADD_NEGATIVE.name
                and self.negative_scope == NegativeScope.AD_GROUP.name
            )
        )

        if requires_ad_group:
            if not ad_group_col or ad_group_col == "<none>":
                raise knext.InvalidParametersError(
                    "Select a column for 'Ad Group Resource Name'. "
                    "Required for ad group negatives and keyword promotion."
                )
            check_column(input_table_schema, ad_group_col, knext.string(), "ad group resource name")

        # Define output schema: input columns + result columns
        # Start with input columns
        output_columns = list(input_table_schema)
        
        # Add result columns - ad_group_name only when needed (not for campaign-level negatives)
        result_column_names = ["Campaign Name", "Match Type", "Status", "Message", "Timestamp"]
        if requires_ad_group:
            result_column_names.insert(1, "Ad Group Name")  # Add after Campaign Name
        
        existing_col_names = {col.name for col in output_columns}
        
        for col_name in result_column_names:
            if col_name not in existing_col_names:
                output_columns.append(knext.Column(knext.string(), col_name))

        return knext.Schema.from_columns(output_columns)

    # ==========================================================================
    # EXECUTE
    # ==========================================================================

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
        input_table: knext.Table,
    ) -> knext.Table:
        """Execute the search terms actions."""

        client: GoogleAdsClient = port_object.client
        customer_id = port_object.spec.account_id

        # Create FieldInspector for enum handling
        field_inspector = FieldInspector(client, client.enums)

        # Convert input to DataFrame
        df = input_table.to_pandas()

        # Get column names
        search_term_col = self.search_term_column
        campaign_col = self.campaign_resource_column
        ad_group_col = self.ad_group_resource_column

        # Determine action details
        is_negative = self.action_type == ActionType.ADD_NEGATIVE.name
        is_campaign_level = self.negative_scope == NegativeScope.CAMPAIGN.name

        if is_negative:
            if is_campaign_level:
                action_name = "ADD_NEGATIVE_CAMPAIGN"
            else:
                action_name = "ADD_NEGATIVE_ADGROUP"
            match_type = self.negative_match_type
        else:
            action_name = "PROMOTE_TO_KEYWORD"
            match_type = self.promotion_match_type

        # Fetch human-readable names for campaigns (always) and ad groups (only when needed)
        exec_context.set_progress(0.05, "Fetching campaign and ad group names...")
        campaign_names, ad_group_names = self._fetch_resource_names(
            client, customer_id, df, campaign_col, ad_group_col, is_negative, is_campaign_level
        )

        # Get existing criteria for duplicate detection (always fetch to detect duplicates)
        exec_context.set_progress(0.1, "Checking for existing keywords/negatives...")
        existing_criteria: dict = self._fetch_existing_criteria(
            client, customer_id, df, campaign_col, ad_group_col, is_negative, is_campaign_level, field_inspector
        )
        LOGGER.debug(f"Fetched {len(existing_criteria)} existing criteria for duplicate detection")

        # Process each row
        exec_context.set_progress(0.3, "Processing search terms...")
        results = []
        timestamp = datetime.now().isoformat()

        is_preview = self.execution_mode == ExecutionMode.PREVIEW.name
        total_rows = len(df)

        for row_num, (idx, row) in enumerate(df.iterrows()):
            progress = 0.3 + (0.6 * (row_num + 1) / total_rows)
            exec_context.set_progress(progress, f"Processing {row_num + 1}/{total_rows}...")

            search_term = str(row[search_term_col]) if pd.notna(row[search_term_col]) else ""
            campaign_resource = str(row[campaign_col]) if pd.notna(row[campaign_col]) else ""
            ad_group_resource = ""
            if ad_group_col and ad_group_col != "<none>" and ad_group_col in df.columns:
                ad_group_resource = str(row[ad_group_col]) if pd.notna(row[ad_group_col]) else ""

            # Start with all input columns, then add result columns
            result = row.to_dict()
            # Add human-readable names from API
            result["Campaign Name"] = campaign_names.get(campaign_resource, "")
            if not (is_negative and is_campaign_level):
                result["Ad Group Name"] = ad_group_names.get(ad_group_resource, "")
            result["Match Type"] = match_type
            result["Status"] = ""
            result["Message"] = ""
            result["Timestamp"] = timestamp

            # Validate inputs
            if not search_term:
                result["Status"] = "FAILED"
                result["Message"] = "Search term text is empty"
                results.append(result)
                continue

            if not campaign_resource:
                result["Status"] = "FAILED"
                result["Message"] = "Campaign resource name is empty"
                results.append(result)
                continue

            if not is_campaign_level or not is_negative:
                if not ad_group_resource:
                    result["Status"] = "FAILED"
                    result["Message"] = "Ad group resource name required for this action"
                    results.append(result)
                    continue

            # Check for duplicates
            duplicate_key = self._build_duplicate_key(
                search_term, match_type, campaign_resource, ad_group_resource, is_negative, is_campaign_level
            )
            if duplicate_key in existing_criteria:
                existing_type = existing_criteria[duplicate_key]  # 'POSITIVE' or 'NEGATIVE'
                # Use clearer status names
                if existing_type == "POSITIVE":
                    result["Status"] = "ALREADY_EXISTS_AS_KEYWORD"
                    keyword_type = "keyword"
                else:
                    result["Status"] = "ALREADY_EXISTS_AS_NEGATIVE_KEYWORD"
                    keyword_type = "negative keyword"
                # Build location string using fetched names
                if is_negative and is_campaign_level:
                    location = f"in campaign '{result.get('Campaign Name', '')}'"
                else:
                    location = f"in ad group '{result.get('Ad Group Name', '')}'"
                result["Message"] = f"'{search_term}' already exists as {match_type} {keyword_type} {location}"
                results.append(result)
                continue

            # Preview mode - no API call
            if is_preview:
                result["Status"] = "PREVIEW"
                result["Message"] = self._build_preview_message(
                    search_term, action_name, match_type, is_campaign_level,
                    result.get("Campaign Name", ""), result.get("Ad Group Name", "")
                )
                results.append(result)
                continue

            # Apply mode - execute API call
            try:
                criterion_resource = self._create_criterion(
                    client, customer_id, search_term, match_type,
                    campaign_resource, ad_group_resource, is_negative, is_campaign_level
                )
                result["Status"] = "SUCCESS"
                result["Message"] = self._build_success_message(
                    search_term, action_name, match_type, is_campaign_level,
                    result.get("Campaign Name", ""), result.get("Ad Group Name", "")
                )
                # Add to existing dict to prevent duplicates within same run
                existing_criteria[duplicate_key] = "NEGATIVE" if is_negative else "POSITIVE"

            except GoogleAdsException as ex:
                error_msg = ex.failure.errors[0].message if ex.failure.errors else str(ex)
                result["Status"] = "FAILED"
                result["Message"] = f"API error: {error_msg}"

            except Exception as ex:
                result["Status"] = "FAILED"
                result["Message"] = f"Unexpected error: {str(ex)}"

            results.append(result)

        # Build output DataFrame
        exec_context.set_progress(0.95, "Building results...")
        output_df = pd.DataFrame(results)

        exec_context.set_progress(1.0, "Complete")
        return knext.Table.from_pandas(output_df)

    # ==========================================================================
    # HELPER METHODS
    # ==========================================================================

    def _fetch_resource_names(
        self, client: GoogleAdsClient, customer_id: str, df: pd.DataFrame,
        campaign_col: str, ad_group_col: str, is_negative: bool, is_campaign_level: bool
    ) -> tuple:
        """
        Fetch human-readable names for campaigns and ad groups.
        
        Returns:
            tuple: (campaign_names dict, ad_group_names dict)
                   Maps resource_name -> display_name
        """
        ga_service = client.get_service("GoogleAdsService")
        campaign_names = {}
        ad_group_names = {}
        
        # Get unique campaign resource names
        campaign_resources = df[campaign_col].dropna().unique().tolist()
        
        if campaign_resources:
            # Build query for campaign names
            campaign_ids = []
            for res in campaign_resources:
                # Extract campaign ID from resource name like 'customers/123/campaigns/456'
                parts = res.split('/')
                if len(parts) >= 4 and parts[2] == 'campaigns':
                    campaign_ids.append(parts[3])
            
            if campaign_ids:
                query = f"""
                    SELECT campaign.resource_name, campaign.name
                    FROM campaign
                    WHERE campaign.id IN ({','.join(campaign_ids)})
                """
                response = ga_service.search(customer_id=customer_id, query=query)
                for row in response:
                    campaign_names[row.campaign.resource_name] = row.campaign.name
        
        # Only fetch ad group names if needed (not campaign-level negatives)
        if not (is_negative and is_campaign_level):
            if ad_group_col and ad_group_col != "<none>" and ad_group_col in df.columns:
                ad_group_resources = df[ad_group_col].dropna().unique().tolist()
                
                if ad_group_resources:
                    # Build query for ad group names
                    ad_group_ids = []
                    for res in ad_group_resources:
                        # Extract ad group ID from resource name like 'customers/123/adGroups/789'
                        parts = res.split('/')
                        if len(parts) >= 4 and parts[2] == 'adGroups':
                            ad_group_ids.append(parts[3])
                    
                    if ad_group_ids:
                        query = f"""
                            SELECT ad_group.resource_name, ad_group.name
                            FROM ad_group
                            WHERE ad_group.id IN ({','.join(ad_group_ids)})
                        """
                        response = ga_service.search(customer_id=customer_id, query=query)
                        for row in response:
                            ad_group_names[row.ad_group.resource_name] = row.ad_group.name
        
        return campaign_names, ad_group_names

    def _get_match_type_enum(self, client: GoogleAdsClient, match_type: str):
        """Convert match type string to Google Ads enum."""
        match_type_map = {
            MatchType.EXACT.name: client.enums.KeywordMatchTypeEnum.EXACT,
            MatchType.PHRASE.name: client.enums.KeywordMatchTypeEnum.PHRASE,
            MatchType.BROAD.name: client.enums.KeywordMatchTypeEnum.BROAD,
        }
        return match_type_map.get(match_type, client.enums.KeywordMatchTypeEnum.EXACT)

    def _build_duplicate_key(
        self, search_term: str, match_type: str, campaign_resource: str,
        ad_group_resource: str, is_negative: bool, is_campaign_level: bool
    ) -> str:
        """Build a unique key for duplicate detection."""
        term_lower = search_term.lower().strip()
        if is_negative and is_campaign_level:
            return f"neg_campaign:{campaign_resource}:{term_lower}:{match_type}"
        elif is_negative:
            return f"neg_adgroup:{ad_group_resource}:{term_lower}:{match_type}"
        else:
            return f"keyword:{ad_group_resource}:{term_lower}:{match_type}"

    def _fetch_existing_criteria(
        self, client: GoogleAdsClient, customer_id: str, df: pd.DataFrame,
        campaign_col: str, ad_group_col: str, is_negative: bool, is_campaign_level: bool,
        field_inspector: FieldInspector
    ) -> dict:
        """Fetch existing keywords/negatives for duplicate detection.
        
        Uses batched queries with IN clauses for better performance with large accounts.
        Batches IDs in chunks of 100 to avoid query size limits while minimizing API calls.
        
        Returns:
            dict: Maps duplicate_key -> 'POSITIVE' or 'NEGATIVE'
        """
        existing = {}  # key -> 'POSITIVE' or 'NEGATIVE'
        ga_service = client.get_service("GoogleAdsService")
        
        # Batch size from advanced settings - balances query size limits vs API call overhead
        batch_size = self.batch_size
        
        # Get enum mappings for match type
        match_type_mapping = field_inspector._load_enum_mapping("KeywordMatchTypeEnum.KeywordMatchType")

        try:
            if is_negative and is_campaign_level:
                # Query campaign-level negatives using batched queries
                campaigns = [c for c in df[campaign_col].unique().tolist() if c]
                if campaigns:
                    # Extract campaign IDs
                    campaign_ids = []
                    for res in campaigns:
                        parts = res.split('/')
                        if len(parts) >= 4 and parts[2] == 'campaigns':
                            campaign_ids.append(parts[3])
                    
                    # Process in batches to avoid query size limits
                    for i in range(0, len(campaign_ids), batch_size):
                        batch_ids = campaign_ids[i:i + batch_size]
                        query = f"""
                            SELECT
                                campaign_criterion.keyword.text,
                                campaign_criterion.keyword.match_type,
                                campaign_criterion.campaign
                            FROM campaign_criterion
                            WHERE campaign.id IN ({','.join(batch_ids)})
                                AND campaign_criterion.negative = TRUE
                                AND campaign_criterion.type = 'KEYWORD'
                        """
                        try:
                            response = ga_service.search(customer_id=customer_id, query=query)
                            for row in response:
                                term = row.campaign_criterion.keyword.text.lower().strip()
                                match_raw = match_type_mapping.get(row.campaign_criterion.keyword.match_type, "")
                                match = match_raw.upper().replace(" ", "_") if match_raw else str(row.campaign_criterion.keyword.match_type)
                                campaign_resource = row.campaign_criterion.campaign
                                key = f"neg_campaign:{campaign_resource}:{term}:{match}"
                                existing[key] = "NEGATIVE"
                        except GoogleAdsException as gex:
                            LOGGER.warning(f"Error querying campaign negatives: {gex}")

            elif is_negative:
                # Query ad group-level negatives AND positive keywords (to detect conflicts)
                if ad_group_col and ad_group_col in df.columns:
                    ad_groups = [ag for ag in df[ad_group_col].unique().tolist() if ag and not pd.isna(ag)]
                    if ad_groups:
                        # Extract ad group IDs
                        ad_group_ids = []
                        for res in ad_groups:
                            parts = str(res).split('/')
                            if len(parts) >= 4 and parts[2] == 'adGroups':
                                ad_group_ids.append(parts[3])
                        
                        # Process in batches to avoid query size limits
                        for i in range(0, len(ad_group_ids), batch_size):
                            batch_ids = ad_group_ids[i:i + batch_size]
                            query = f"""
                                SELECT
                                    ad_group_criterion.keyword.text,
                                    ad_group_criterion.keyword.match_type,
                                    ad_group_criterion.ad_group,
                                    ad_group_criterion.negative
                                FROM ad_group_criterion
                                WHERE ad_group.id IN ({','.join(batch_ids)})
                                    AND ad_group_criterion.type = 'KEYWORD'
                                    AND ad_group_criterion.status != 'REMOVED'
                            """
                            try:
                                response = ga_service.search(customer_id=customer_id, query=query)
                                for row in response:
                                    term = row.ad_group_criterion.keyword.text.lower().strip()
                                    match_raw = match_type_mapping.get(row.ad_group_criterion.keyword.match_type, "")
                                    match = match_raw.upper().replace(" ", "_") if match_raw else str(row.ad_group_criterion.keyword.match_type)
                                    is_neg = row.ad_group_criterion.negative
                                    ad_group_resource = row.ad_group_criterion.ad_group
                                    key = f"neg_adgroup:{ad_group_resource}:{term}:{match}"
                                    existing[key] = "NEGATIVE" if is_neg else "POSITIVE"
                            except GoogleAdsException as gex:
                                LOGGER.warning(f"Error querying ad group keywords: {gex}")

            else:
                # Query keywords (positive) using batched queries
                if ad_group_col and ad_group_col != "<none>" and ad_group_col in df.columns:
                    ad_groups = [ag for ag in df[ad_group_col].unique().tolist() if ag and not pd.isna(ag)]
                    if ad_groups:
                        # Extract ad group IDs
                        ad_group_ids = []
                        for res in ad_groups:
                            parts = str(res).split('/')
                            if len(parts) >= 4 and parts[2] == 'adGroups':
                                ad_group_ids.append(parts[3])
                        
                        # Process in batches to avoid query size limits
                        for i in range(0, len(ad_group_ids), batch_size):
                            batch_ids = ad_group_ids[i:i + batch_size]
                            query = f"""
                                SELECT
                                    ad_group_criterion.keyword.text,
                                    ad_group_criterion.keyword.match_type,
                                    ad_group_criterion.ad_group,
                                    ad_group_criterion.negative
                                FROM ad_group_criterion
                                WHERE ad_group.id IN ({','.join(batch_ids)})
                                    AND ad_group_criterion.type = 'KEYWORD'
                                    AND ad_group_criterion.status != 'REMOVED'
                            """
                            try:
                                response = ga_service.search(customer_id=customer_id, query=query)
                                for row in response:
                                    match_raw = match_type_mapping.get(row.ad_group_criterion.keyword.match_type, "")
                                    match = match_raw.upper().replace(" ", "_") if match_raw else str(row.ad_group_criterion.keyword.match_type)
                                    is_neg = row.ad_group_criterion.negative
                                    term = row.ad_group_criterion.keyword.text.lower().strip()
                                    ad_group_resource = row.ad_group_criterion.ad_group
                                    key = f"keyword:{ad_group_resource}:{term}:{match}"
                                    existing[key] = "NEGATIVE" if is_neg else "POSITIVE"
                            except GoogleAdsException as gex:
                                LOGGER.warning(f"Error querying ad group keywords: {gex}")

        except Exception as ex:
            LOGGER.warning(f"Error fetching existing criteria: {ex}")

        return existing

    def _create_criterion(
        self, client: GoogleAdsClient, customer_id: str, search_term: str,
        match_type: str, campaign_resource: str, ad_group_resource: str,
        is_negative: bool, is_campaign_level: bool
    ) -> str:
        """Create a keyword or negative keyword criterion."""

        match_type_enum = self._get_match_type_enum(client, match_type)

        if is_negative and is_campaign_level:
            # Campaign-level negative
            service = client.get_service("CampaignCriterionService")
            operation = client.get_type("CampaignCriterionOperation")
            criterion = operation.create

            criterion.campaign = campaign_resource
            criterion.negative = True
            criterion.keyword.text = search_term
            criterion.keyword.match_type = match_type_enum

            response = service.mutate_campaign_criteria(
                customer_id=customer_id, operations=[operation]
            )
            return response.results[0].resource_name

        elif is_negative:
            # Ad group-level negative
            service = client.get_service("AdGroupCriterionService")
            operation = client.get_type("AdGroupCriterionOperation")
            criterion = operation.create

            criterion.ad_group = ad_group_resource
            criterion.negative = True
            criterion.keyword.text = search_term
            criterion.keyword.match_type = match_type_enum

            response = service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=[operation]
            )
            return response.results[0].resource_name

        else:
            # Positive keyword
            service = client.get_service("AdGroupCriterionService")
            operation = client.get_type("AdGroupCriterionOperation")
            criterion = operation.create

            criterion.ad_group = ad_group_resource
            criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
            criterion.keyword.text = search_term
            criterion.keyword.match_type = match_type_enum

            response = service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=[operation]
            )
            return response.results[0].resource_name

    def _build_preview_message(
        self, search_term: str, action_name: str, match_type: str, is_campaign_level: bool,
        campaign_name: str = "", ad_group_name: str = ""
    ) -> str:
        """Build a preview message for the audit log."""
        if action_name == "ADD_NEGATIVE_CAMPAIGN":
            location = f"to campaign '{campaign_name}'" if campaign_name else "as campaign negative"
            return f"Will add '{search_term}' as {match_type} negative {location}"
        elif action_name == "ADD_NEGATIVE_ADGROUP":
            location = f"to ad group '{ad_group_name}'" if ad_group_name else "as ad group negative"
            return f"Will add '{search_term}' as {match_type} negative {location}"
        else:
            location = f"to ad group '{ad_group_name}'" if ad_group_name else "as keyword"
            return f"Will promote '{search_term}' as {match_type} keyword {location}"

    def _build_success_message(
        self, search_term: str, action_name: str, match_type: str, is_campaign_level: bool,
        campaign_name: str = "", ad_group_name: str = ""
    ) -> str:
        """Build a success message for the audit log."""
        if action_name == "ADD_NEGATIVE_CAMPAIGN":
            location = f"to campaign '{campaign_name}'" if campaign_name else "as campaign negative"
            return f"Added '{search_term}' as {match_type} negative {location}"
        elif action_name == "ADD_NEGATIVE_ADGROUP":
            location = f"to ad group '{ad_group_name}'" if ad_group_name else "as ad group negative"
            return f"Added '{search_term}' as {match_type} negative {location}"
        else:
            location = f"to ad group '{ad_group_name}'" if ad_group_name else "as keyword"
            return f"Promoted '{search_term}' as {match_type} keyword {location}"
