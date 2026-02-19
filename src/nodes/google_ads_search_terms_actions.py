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
from util.search_terms_utils import (
    fetch_existing_criteria,
    check_shared_list_conflicts,
    check_campaign_conflicts,
    check_adgroup_conflicts,
    create_criterion,
    build_preview_message,
    build_success_message,
)

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
        "Add negative keyword at campaign level (blocks across all ad groups in the campaign).",
    )
    AD_GROUP = (
        "Ad Group level",
        "Add negative keyword at ad group level (blocks only in that ad group).",
    )
    SHARED_LIST = (
        "Account Shared Negative List",
        "Add negative keyword to an account-level shared negative keyword list. "
        "Use this for lists created within the client account. "
        "Requires a column with the shared set resource name.",
    )
    MCC_SHARED_LIST = (
        "MCC Shared Negative List",
        "Add negative keyword to an MCC-level (Manager Account) shared negative keyword list. "
        "Use this for lists created at the Manager Account level that are shared across client accounts. "
        "Requires a column with the shared set resource name.",
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
    # node_type is MANIPULATOR (not SINK) because it returns a results table
    # even though it writes to the Google Ads API
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
    "Connection containing credentials, customer ID, and optionally a Manager Account (MCC) ID to access the Google Ads API.",
    google_ad_port_type,
)
@knext.input_table(
    name="Input Data",
    description="Table containing terms and the required resource names (campaign, ad group, or shared set) depending on the selected action and scope.",
)
@knext.output_table(
    name="Results",
    description="Results with status for each term action.",
)
class GoogleAdsSearchTermsActions:
    """
    Executes actions on terms: add as negative keywords or promote to keywords.

    This node is a **pure executor** — all decision logic (rules, filtering) should be done
    upstream using KNIME nodes like Rule Engine, Row Filter, or Joiner.

    **Actions**

    - **Add as Negative Keyword**: Block terms from triggering ads. Can be added at
      campaign level, ad group level, or to shared negative keyword lists (Account or MCC level).
    - **Promote to Keyword**: Add high-performing terms as keywords to gain more control
      over bidding and ad relevance.

    **Workflow Example**

    1. Use **Google Ads Query** node to fetch search term performance data
    2. Join with CRM data (MQLs, opportunities) using **Joiner** node
    3. Apply rules using **Rule Engine** to decide which terms to act on
    4. Filter using **Row Filter** to keep only actionable terms
    5. Connect to this node to execute the actions

    **Preview Mode**

    Use Preview mode to review proposed changes before applying them. The output table
    will show what would happen without making any changes to your Google Ads account.

    **Duplicate & Conflict Detection**

    By default, the node queries existing keywords/negatives before processing to:
    
    - **Detect duplicates**: Keywords that already exist are marked with status 
      "ALREADY_EXISTS" and skipped.
    - **Detect conflicts**: Warns when adding a negative keyword that conflicts with an 
      existing positive keyword (e.g., adding "shoes" as negative when it's already a 
      positive keyword in an ad group). The action is skipped and no changes are made 
      to your Google Ads account.

    **⚠️ Shared Negative Lists (Account/MCC Level)**
    
    When adding negatives to **Account Shared Negative Lists** or **MCC Shared Negative Lists**, 
    only duplicate detection within the target list is performed. Conflict detection with 
    positive keywords at campaign/ad group level is **not** performed. This is by design: 
    adding keywords to a shared list is a deliberate account-wide decision, and users should 
    verify upstream that the terms do not conflict with active positive keywords in linked campaigns.

    **⚠️ Important: Skip Duplicate Check Option**

    The "Skip Duplicate/Conflict Check" option in Advanced Settings disables all pre-execution 
    queries for better performance. When enabled:
    
    - **No duplicate detection**: Duplicates will only be caught when the API returns an error.
    - **No conflict detection**: Negative keywords that conflict with positive keywords will be 
      added without warning. Google Ads allows this, but it can cause your positive keywords 
      to stop triggering ads!
    
    Use this option only when you're confident the keywords don't already exist, or for 
    testing purposes.

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
        description="Choose whether to add terms as negative keywords or promote them to keywords.",
        default_value=ActionType.ADD_NEGATIVE.name,
        enum=ActionType,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    )

    # ==========================================================================
    # NEGATIVE KEYWORD SETTINGS (shown when action = ADD_NEGATIVE)
    # ==========================================================================

    negative_scope = knext.EnumParameter(
        label="Scope",
        description="Choose where to add the negative keyword: campaign level, ad group level, or a shared negative keyword list.",
        default_value=NegativeScope.CAMPAIGN.name,
        enum=NegativeScope,
        style=knext.EnumParameter.Style.DROPDOWN,
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
        label="Term Text",
        description="Column containing the term text to add as keyword or negative keyword.",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        column_filter=create_type_filer(knext.string()),
    )

    campaign_resource_column = knext.ColumnParameter(
        label="Campaign Resource Name",
        description="Column containing Google Ads campaign resource names (e.g., 'customers/123/campaigns/456'). Not required for shared list scope.",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        column_filter=create_type_filer(knext.string()),
    ).rule(
        knext.Or(
            knext.OneOf(action_type, [ActionType.PROMOTE_TO_KEYWORD.name]),
            knext.OneOf(negative_scope, [NegativeScope.CAMPAIGN.name, NegativeScope.AD_GROUP.name]),
        ),
        knext.Effect.SHOW,
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

    shared_set_resource_column = knext.ColumnParameter(
        label="Shared Set Resource Name",
        description="Column containing the shared negative keyword list resource name (e.g., 'customers/123/sharedSets/456'). "
                    "Use the 'Negative Keyword Lists' pre-built query in the Google Ads Query node to retrieve available lists.",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        column_filter=create_type_filer(knext.string()),
    ).rule(
        knext.OneOf(negative_scope, [NegativeScope.SHARED_LIST.name, NegativeScope.MCC_SHARED_LIST.name]),
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

    skip_duplicate_check = knext.BoolParameter(
        label="Skip Duplicate/Conflict Check",
        description="When enabled, skips the pre-execution check for existing keywords and conflicts. "
                    "This significantly improves performance but has important implications: "
                    "(1) Duplicates will only be detected when the API returns an error. "
                    "(2) Conflicts between negative and positive keywords will NOT be detected — "
                    "negative keywords will be added even if they conflict with existing positive keywords, "
                    "which can cause your ads to stop showing for those terms. "
                    "Use only when you're confident the keywords don't already exist or for testing.",
        default_value=False,
        is_advanced=True,
    )

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
        shared_set_col = self.shared_set_resource_column

        if not search_term_col:
            raise knext.InvalidParametersError(
                "Select a column for 'Search Term Text'."
            )

        check_column(input_table_schema, search_term_col, knext.string(), "search term")

        # Determine what columns are required based on action/scope
        is_negative = self.action_type == ActionType.ADD_NEGATIVE.name
        is_shared_list = is_negative and self.negative_scope in [NegativeScope.SHARED_LIST.name, NegativeScope.MCC_SHARED_LIST.name]
        is_ad_group_level = is_negative and self.negative_scope == NegativeScope.AD_GROUP.name

        # Shared list scope requires shared set column
        if is_shared_list:
            if not shared_set_col:
                raise knext.InvalidParametersError(
                    "Select a column for 'Shared Set Resource Name'."
                )
            check_column(input_table_schema, shared_set_col, knext.string(), "shared set resource name")
        else:
            # Non-shared list scopes require campaign column
            if not campaign_col:
                raise knext.InvalidParametersError(
                    "Select a column for 'Campaign Resource Name'."
                )
            check_column(input_table_schema, campaign_col, knext.string(), "campaign resource name")

        # Ad group column required for ad group negatives or keyword promotion
        requires_ad_group = (
            self.action_type == ActionType.PROMOTE_TO_KEYWORD.name
            or is_ad_group_level
        )

        if requires_ad_group:
            if not ad_group_col:
                raise knext.InvalidParametersError(
                    "Select a column for 'Ad Group Resource Name'. "
                    "Required for ad group negatives and keyword promotion."
                )
            check_column(input_table_schema, ad_group_col, knext.string(), "ad group resource name")

        # Define output schema: input columns + result columns
        # Start with input columns
        output_columns = list(input_table_schema)
        
        # Add result columns based on scope
        if is_shared_list:
            result_column_names = ["Shared Set Name", "Match Type", "Status", "Message", "Timestamp"]
        elif requires_ad_group:
            result_column_names = ["Campaign Name", "Ad Group Name", "Match Type", "Status", "Message", "Timestamp"]
        else:
            result_column_names = ["Campaign Name", "Match Type", "Status", "Message", "Timestamp"]
        
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
        manager_account_id = port_object.spec.manager_account_id
        
        # Create FieldInspector for enum handling
        field_inspector = FieldInspector(client, client.enums)

        # Convert input to DataFrame
        df = input_table.to_pandas()

        # Get column names
        search_term_col = self.search_term_column
        campaign_col = self.campaign_resource_column
        ad_group_col = self.ad_group_resource_column
        shared_set_col = self.shared_set_resource_column

        # Determine action details
        is_negative = self.action_type == ActionType.ADD_NEGATIVE.name
        is_campaign_level = is_negative and self.negative_scope == NegativeScope.CAMPAIGN.name
        is_ad_group_level = is_negative and self.negative_scope == NegativeScope.AD_GROUP.name
        is_shared_list = is_negative and self.negative_scope in [NegativeScope.SHARED_LIST.name, NegativeScope.MCC_SHARED_LIST.name]
        is_mcc_shared_list = is_negative and self.negative_scope == NegativeScope.MCC_SHARED_LIST.name

        if is_negative:
            if is_shared_list:
                action_name = "ADD_NEGATIVE_SHARED_LIST"
            elif is_campaign_level:
                action_name = "ADD_NEGATIVE_CAMPAIGN"
            else:
                action_name = "ADD_NEGATIVE_ADGROUP"
            match_type = self.negative_match_type
        else:
            action_name = "PROMOTE_TO_KEYWORD"
            match_type = self.promotion_match_type

        # Get existing criteria for conflict detection (also fetches names)
        # Skip if user opted out for performance
        existing_criteria = {}
        shared_set_names = {}
        campaign_names = {}
        ad_group_names = {}
        
        if self.skip_duplicate_check:
            LOGGER.debug("Skipping duplicate/conflict check (skip_duplicate_check=True)")
        else:
            exec_context.set_progress(0.1, "Checking for existing keywords/negatives...")
            if is_shared_list:
                existing_criteria = fetch_existing_criteria(
                    client, customer_id, df, field_inspector, self.batch_size,
                    shared_set_col=shared_set_col, scope="shared_list"
                )
            else:
                scope = "campaign" if is_campaign_level else "ad_group"
                existing_criteria = fetch_existing_criteria(
                    client, customer_id, df, field_inspector, self.batch_size,
                    campaign_col=campaign_col, ad_group_col=ad_group_col, scope=scope
                )
            # Extract name mappings from existing_criteria
            shared_set_names = existing_criteria.pop('__shared_set_names__', {})
            campaign_names = existing_criteria.pop('__campaign_names__', {})
            ad_group_names = existing_criteria.pop('__ad_group_names__', {})
            
            # Count actual criteria (exclude metadata keys that start with __)
            criteria_count = sum(1 for k in existing_criteria if not k.startswith('__'))
            LOGGER.debug(f"Fetched {criteria_count} existing criteria for conflict detection")

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
            
            # Get resource names based on scope
            if is_shared_list:
                shared_set_resource = str(row[shared_set_col]) if shared_set_col and shared_set_col in df.columns and pd.notna(row[shared_set_col]) else ""
                campaign_resource = ""
                ad_group_resource = ""
            else:
                shared_set_resource = ""
                campaign_resource = str(row[campaign_col]) if campaign_col and campaign_col in df.columns and pd.notna(row[campaign_col]) else ""
                ad_group_resource = ""
                if ad_group_col and ad_group_col in df.columns:
                    ad_group_resource = str(row[ad_group_col]) if pd.notna(row[ad_group_col]) else ""

            # Start with all input columns, then add result columns
            result = row.to_dict()
            
            # Add human-readable names from API based on scope
            if is_shared_list:
                result["Shared Set Name"] = shared_set_names.get(shared_set_resource, "")
            else:
                result["Campaign Name"] = campaign_names.get(campaign_resource, "")
                if not is_campaign_level:
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

            if is_shared_list:
                if not shared_set_resource:
                    result["Status"] = "FAILED"
                    result["Message"] = "Shared set resource name is empty"
                    results.append(result)
                    continue
            else:
                if not campaign_resource:
                    result["Status"] = "FAILED"
                    result["Message"] = "Campaign resource name is empty"
                    results.append(result)
                    continue

                if is_ad_group_level or not is_negative:
                    if not ad_group_resource:
                        result["Status"] = "FAILED"
                        result["Message"] = "Ad group resource name required for this action"
                        results.append(result)
                        continue

            # Check for duplicates and conflicts across all levels
            conflict = None
            if is_shared_list:
                # For shared lists, check multiple levels
                conflict = check_shared_list_conflicts(
                    search_term, match_type, shared_set_resource, existing_criteria,
                    result.get('Shared Set Name', '')
                )
            elif is_negative and is_campaign_level:
                # For campaign-level negatives, check campaign + shared lists + ad groups
                conflict = check_campaign_conflicts(
                    search_term, match_type, campaign_resource, existing_criteria,
                    result.get('Campaign Name', '')
                )
            else:
                # For ad group negatives or keyword promotion
                conflict = check_adgroup_conflicts(
                    search_term, match_type, ad_group_resource, campaign_resource,
                    existing_criteria, result.get('Ad Group Name', ''),
                    result.get('Campaign Name', ''), is_negative
                )
            
            if conflict:
                result["Status"] = conflict['status']
                result["Message"] = conflict['message']
                results.append(result)
                continue

            # Preview mode - no API call
            if is_preview:
                result["Status"] = "PREVIEW"
                result["Message"] = build_preview_message(
                    search_term, action_name, match_type, is_campaign_level,
                    result.get("Campaign Name", ""), result.get("Ad Group Name", ""),
                    result.get("Shared Set Name", ""), is_shared_list
                )
                results.append(result)
                continue

            # Apply mode - execute API call
            try:
                # Only pass manager_account_id if the shared list is MCC-owned
                mcc_id_for_shared_list = manager_account_id if is_mcc_shared_list else ""
                criterion_resource = create_criterion(
                    client, customer_id, search_term, match_type,
                    campaign_resource, ad_group_resource, is_negative, is_campaign_level,
                    shared_set_resource, is_shared_list, mcc_id_for_shared_list
                )
                result["Status"] = "SUCCESS"
                result["Message"] = build_success_message(
                    search_term, action_name, match_type, is_campaign_level,
                    result.get("Campaign Name", ""), result.get("Ad Group Name", ""),
                    result.get("Shared Set Name", ""), is_shared_list
                )
                # Add to existing dict to prevent duplicates within same run
                term_lower = search_term.lower().strip()
                if is_shared_list:
                    sharedlist_key = f"sharedlist:{shared_set_resource}:{term_lower}:{match_type}"
                    existing_criteria[sharedlist_key] = {
                        'type': 'NEGATIVE',
                        'level': 'SHARED_LIST',
                        'location_name': result.get("Shared Set Name", "")
                    }
                elif is_campaign_level:
                    # Extract campaign ID for key
                    camp_id = existing_criteria.get('__campaign_resource_to_id__', {}).get(campaign_resource, '')
                    if not camp_id:
                        parts = campaign_resource.split('/')
                        if len(parts) >= 4 and parts[2] == 'campaigns':
                            camp_id = parts[3]
                    if camp_id:
                        campaign_key = f"campaign:{camp_id}:{term_lower}:{match_type}"
                        existing_criteria[campaign_key] = {
                            'type': 'NEGATIVE',
                            'level': 'CAMPAIGN',
                            'location_name': result.get("Campaign Name", "")
                        }
                else:
                    # Ad group level
                    ag_id = existing_criteria.get('__ad_group_resource_to_id__', {}).get(ad_group_resource, '')
                    if not ag_id:
                        parts = str(ad_group_resource).split('/')
                        if len(parts) >= 4 and parts[2] == 'adGroups':
                            ag_id = parts[3]
                    if ag_id:
                        adgroup_key = f"adgroup:{ag_id}:{term_lower}:{match_type}"
                        existing_criteria[adgroup_key] = {
                            'type': 'NEGATIVE' if is_negative else 'POSITIVE',
                            'level': 'AD_GROUP',
                            'location_name': result.get("Ad Group Name", "")
                        }

            except GoogleAdsException as ex:
                error_msg = ex.failure.errors[0].message if ex.failure.errors else str(ex)
                result["Status"] = "FAILED"
                result["Message"] = f"API error: {error_msg}"
                LOGGER.debug(f"GoogleAdsException for search_term='{search_term}': {error_msg}")
                for error in ex.failure.errors:
                    LOGGER.debug(f"  Error code: {error.error_code}")
                    LOGGER.debug(f"  Error message: {error.message}")
                    if hasattr(error, 'trigger') and error.trigger:
                        LOGGER.debug(f"  Trigger: {error.trigger.string_value if hasattr(error.trigger, 'string_value') else error.trigger}")

            except Exception as ex:
                result["Status"] = "FAILED"
                result["Message"] = f"Unexpected error: {str(ex)}"
                LOGGER.debug(f"Unexpected exception for search_term='{search_term}': {ex}", exc_info=True)

            results.append(result)

        # Build output DataFrame
        exec_context.set_progress(0.95, "Building results...")
        output_df = pd.DataFrame(results)

        exec_context.set_progress(1.0, "Complete")
        return knext.Table.from_pandas(output_df)
