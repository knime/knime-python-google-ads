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
Google Ads Budget Updater (Labs)

This node applies budget changes to campaigns via the Google Ads API. It is intended for scheduled and agent-driven
execution in KNIME Hub with strict safety limits.
"""

import logging
from datetime import datetime
from typing import List, Optional

import knime.extension as knext
import pandas as pd
import google_ads_ext

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf import field_mask_pb2

from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)
from util.utils import check_column, create_type_filer

LOGGER = logging.getLogger(__name__)

# Constants
MICROS_PER_UNIT = 1_000_000  # 1 currency unit = 1,000,000 micros


class BudgetDirection(knext.EnumParameterOptions):
    """Budget adjustment direction options."""

    INCREASE = (
        "Increase",
        "Increase campaign budgets.",
    )
    DECREASE = (
        "Decrease",
        "Decrease campaign budgets.",
    )


class BudgetStepMode(knext.EnumParameterOptions):
    """Budget adjustment step mode options."""

    ABSOLUTE = (
        "Absolute",
        "Adjust budget by a fixed amount (e.g., $50).",
    )
    PERCENTAGE = (
        "Percentage",
        "Adjust budget by a percentage of current budget (e.g., 10%).",
    )


class BudgetCapMode(knext.EnumParameterOptions):
    """Budget cap mode options."""

    PER_CAMPAIGN = (
        "Per Campaign",
        "Update each campaign's budget individually.",
    )
    TOTAL_BUDGET = (
        "Across All Campaigns",
        "Distribute a total budget across all campaigns.",
    )


class DistributionStrategy(knext.EnumParameterOptions):
    """Distribution strategy options for total budget mode."""

    EQUAL = (
        "Equal Split",
        "Distribute the total budget equally among all campaigns.",
    )
    PROPORTIONAL_SPEND = (
        "Proportional to Current Budget",
        "Campaigns with higher current budgets receive more of the extra budget.",
    )
    PROPORTIONAL_KPI = (
        "Proportional to KPI",
        "Campaigns with higher KPI values (e.g., conversions) receive more budget.",
    )


class ExecutionMode(knext.EnumParameterOptions):
    """Execution mode options."""

    PREVIEW = (
        "Preview",
        "Generate proposed changes without applying them. Use this to review changes before execution.",
    )
    APPLY = (
        "Apply",
        "Apply the budget changes to the Google Ads account. Changes take effect immediately.",
    )


# ==========================================================================
# PARAMETER GROUP DEFINITIONS (must be outside node class)
# ==========================================================================


@knext.parameter_group(label="Campaign Budgets to Update")
class InputColumnMapping:
    """
    Map your input table columns to the required Google Ads resource identifiers.
    These columns are used to identify which campaigns and budgets to update.
    """

    campaign_resource_column = knext.ColumnParameter(
        label="Campaign Resource Name",
        description="Column containing Google Ads campaign resource names (e.g., 'customers/123/campaigns/456').",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        column_filter=create_type_filer(knext.string()),
    )

    budget_resource_column = knext.ColumnParameter(
        label="Budget Resource Name",
        description="Column containing Google Ads campaign budget resource names (e.g., 'customers/123/campaignBudgets/789').",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        column_filter=create_type_filer(knext.string()),
    )


@knext.parameter_group(label="Budget Update Configuration")
class BudgetAdjustment:
    """
    Configure how budget changes are calculated. Choose between per-campaign limits
    or distributing a total budget across all campaigns.
    """

    # 1. Direction
    budget_direction = knext.EnumParameter(
        label="Increase or Decrease",
        description="Choose whether to increase or decrease campaign budgets.",
        default_value=BudgetDirection.INCREASE.name,
        enum=BudgetDirection,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    )

    # 2. Apply to
    budget_cap_mode = knext.EnumParameter(
        label="Apply to",
        description="Update each campaign individually, or distribute a total budget across all campaigns.",
        default_value=BudgetCapMode.PER_CAMPAIGN.name,
        enum=BudgetCapMode,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    )

    # === Per-campaign mode parameters ===

    # 3. Change by (Absolute vs Percentage)
    budget_step_mode = knext.EnumParameter(
        label="Change by",
        description="Calculate change as a fixed amount or percentage of current budget.",
        default_value=BudgetStepMode.PERCENTAGE.name,
        enum=BudgetStepMode,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    ).rule(
        knext.OneOf(budget_cap_mode, [BudgetCapMode.PER_CAMPAIGN.name]),
        knext.Effect.SHOW,
    )

    # 4. Value (shared between Per Campaign and Total Budget modes)
    budget_step_value = knext.DoubleParameter(
        label="Value",
        description=(
            "Numeric value used for budget changes. "
            "In 'Per Campaign' mode, this is either an absolute currency change (if 'Change by' is Absolute) "
            "or a percentage multiplier (if 'Change by' is Percentage, e.g., 150 means +150%). "
            "In 'Across All Campaigns' mode, this is the total amount to distribute across campaigns (currency units)."
        ),
        default_value=10.0,
        min_value=0.01,
    )

    # 5. Max per campaign (guardrail - at the end)
    max_budget_change = knext.DoubleParameter(
        label="Max per campaign",
        description="Maximum budget change per campaign (safety guardrail). Value in currency units.",
        default_value=100.0,
        min_value=0.01,
    ).rule(
        knext.OneOf(budget_cap_mode, [BudgetCapMode.PER_CAMPAIGN.name]),
        knext.Effect.SHOW,
    )

    # === Total budget mode parameters ===

    # 4. Distribution strategy
    distribution_strategy = knext.EnumParameter(
        label="Distribution strategy",
        description="How to distribute the total budget among campaigns.",
        default_value=DistributionStrategy.EQUAL.name,
        enum=DistributionStrategy,
    ).rule(
        knext.OneOf(budget_cap_mode, [BudgetCapMode.TOTAL_BUDGET.name]),
        knext.Effect.SHOW,
    )

    # 5. KPI Column (only when Proportional to KPI is selected)
    kpi_column = knext.ColumnParameter(
        label="KPI Column",
        description="Column containing KPI values (e.g., conversions, MQLs) used to weight budget distribution.",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
    ).rule(
        knext.And(
            knext.OneOf(budget_cap_mode, [BudgetCapMode.TOTAL_BUDGET.name]),
            knext.OneOf(distribution_strategy, [DistributionStrategy.PROPORTIONAL_KPI.name]),
        ),
        knext.Effect.SHOW,
    )


@knext.node(
    name="Google Ads Budget Updater (Labs)",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/gads-icon.png",
    category=google_ads_ext.main_category,
    keywords=[
        "Google",
        "Google Ads",
        "Budget",
        "Updater",
        "Campaign Budget",
        "tCPA",
        "tROAS",
        "Performance",
        "Automation",
    ],
)
@knext.input_port(
    "Google Ads Connection",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API.",
    google_ad_port_type,
)
@knext.input_table(
    name="Campaign Metrics",
    description="Table containing campaign performance metrics. You will select which columns contain the campaign resource names, budget resource names, cost, and KPI values.",
)
@knext.output_table(
    name="Audit / Change Log",
    description="Detailed log of all budget changes including campaign details, old values, new values, and execution status.",
)
class GoogleAdsBudgetUpdater:
    """
    Updates campaign budgets (increase or decrease) via the Google Ads API.

    This node is designed for **scheduled and agent-driven execution** in KNIME Hub with strict safety limits
    (guardrails) to prevent runaway budget changes.

    **Features**

    - **Campaign Budget Updates**: Increase or decrease daily budgets by a fixed amount or percentage.
    - **Two Budget Modes**: 
      - *Per Campaign*: Each campaign changes up to a max amount.
      - *Total Budget*: Distribute a total budget change across all campaigns.
    - **Distribution Strategies** (for Total Budget mode):
      - *Equal Split*: Distribute evenly among campaigns.
      - *Proportional to Current Budget*: Higher-budget campaigns get more.
      - *Proportional to KPI*: Campaigns with more conversions/MQLs get more.
    - **Shared Budget Handling**: Automatically detects when multiple campaigns share the same budget 
      and updates it only once, preventing duplicate changes.
    - **Preview Mode**: Review proposed changes before applying them.
    - **Agent-Friendly Output**: Rich messages in the audit log help AI agents understand each change.

    **Configuration**

    1. **Column Selection**: Map your input table columns to the required fields (campaign resource, budget resource, optional KPI).
    2. **Direction**: Choose to Increase or Decrease budgets.
    3. **Budget Mode**: Choose Per Campaign or Total Budget distribution.
    4. **Execution Mode**: Choose Preview to review changes or Apply to execute them.

    **Input Table Requirements**

    The input table should contain columns for:
    - **Campaign Resource Name**: Google Ads campaign resource name (e.g., 'customers/123/campaigns/456').
    - **Budget Resource Name**: Google Ads campaign budget resource name (e.g., 'customers/123/campaignBudgets/789').
    - **KPI Column** (optional): Numeric column for KPI-based distribution (e.g., conversions, MQLs).

    **Shared Budgets**

    If multiple campaigns in your input share the same budget resource, the node will:
    - Update the shared budget **only once** (via the first campaign encountered)
    - Mark subsequent campaigns as `SHARED_REF` in the audit log
    - Include shared budget warnings in the message column for transparency

    **Tip**: Use upstream KNIME nodes (Row Filter, Rule-based Row Filter) to select which campaigns 
    should receive budget changes based on performance metrics like cost, conversions, or ROAS.

    **Mandatory Upstream Node**

    - Connect to the **Google Ads Connector** node to authenticate with the Google Ads API.

    **Output**

    - **Audit / Change Log**: Detailed log of all proposed or applied changes with agent-friendly messages.
    """

    # Instantiate parameter groups (defined outside the class)
    input_columns = InputColumnMapping()
    budget_settings = BudgetAdjustment()

    # Execution mode (single parameter, not a group)
    execution_mode = knext.EnumParameter(
        label="Preview or Apply",
        description="Preview generates a report without changes. Apply updates budgets in Google Ads.",
        default_value=ExecutionMode.PREVIEW.name,
        enum=ExecutionMode,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    )

    # ==========================================================================
    # CONFIGURE METHOD
    # ==========================================================================

    def configure(
        self,
        configure_context: knext.ConfigurationContext,
        spec: GoogleAdObjectSpec,
        input_table_schema: knext.Schema,
    ) -> knext.Schema:
        """
        Validate configuration and define output schema.
        Returns the output schema for the audit log table.
        """

        # Validate required column selections (fail early during configuration)
        campaign_col = self.input_columns.campaign_resource_column
        budget_col = self.input_columns.budget_resource_column

        if not campaign_col:
            raise knext.InvalidParametersError(
                "Select a column for 'Campaign Resource Name' in the node configuration."
            )
        if not budget_col:
            raise knext.InvalidParametersError(
                "Select a column for 'Budget Resource Name' in the node configuration."
            )

        check_column(input_table_schema, campaign_col, knext.string(), "campaign resource name")
        check_column(input_table_schema, budget_col, knext.string(), "budget resource name")

        # KPI column is required only for Total Budget + Proportional to KPI
        if (
            self.budget_settings.budget_cap_mode == BudgetCapMode.TOTAL_BUDGET.name
            and self.budget_settings.distribution_strategy
            == DistributionStrategy.PROPORTIONAL_KPI.name
        ):
            kpi_col = self.budget_settings.kpi_column
            if not kpi_col:
                raise knext.InvalidParametersError(
                    "Select a 'KPI Column' when using 'Proportional to KPI' distribution."
                )
            # Allow any numeric-like types; KNIME uses double/int64 commonly
            if kpi_col not in input_table_schema.column_names:
                raise knext.InvalidParametersError(
                    f"The KPI column '{kpi_col}' is missing in the input table."
                )

        # Define output schema for audit log
        audit_schema = knext.Schema.from_columns(
            [
                knext.Column(knext.string(), "campaign_resource_name"),
                knext.Column(knext.string(), "campaign_budget_resource_name"),
                knext.Column(knext.string(), "campaign_name"),
                knext.Column(knext.double(), "current_budget"),
                knext.Column(knext.double(), "proposed_budget"),
                knext.Column(knext.double(), "budget_change"),
                knext.Column(knext.double(), "budget_change_pct"),
                knext.Column(knext.string(), "action"),
                knext.Column(knext.string(), "status"),
                knext.Column(knext.string(), "message"),
                knext.Column(knext.string(), "timestamp"),
                knext.Column(knext.bool_(), "is_shared_budget"),
                knext.Column(knext.string(), "shared_with_campaigns"),
            ]
        )

        return audit_schema

    # ==========================================================================
    # EXECUTE METHOD
    # ==========================================================================

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
        input_table: knext.Table,
    ) -> knext.Table:
        """
        Execute the budget optimization logic.
        """
        client: GoogleAdsClient = port_object.client
        customer_id = port_object.spec.account_id

        # Convert input table to pandas DataFrame
        df = input_table.to_pandas()

        # Get column names from parameters
        campaign_col = self.input_columns.campaign_resource_column
        budget_col = self.input_columns.budget_resource_column


        # Defensive validation: ColumnParameter should enforce this, but fail fast with a clear message
        if not campaign_col:
            raise knext.InvalidParametersError(
                "Select a column for 'Campaign Resource Name' in the node configuration."
            )
        if not budget_col:
            raise knext.InvalidParametersError(
                "Select a column for 'Budget Resource Name' in the node configuration."
            )
        if campaign_col not in df.columns:
            raise knext.InvalidParametersError(
                f"Selected 'Campaign Resource Name' column '{campaign_col}' was not found in the input table."
            )
        if budget_col not in df.columns:
            raise knext.InvalidParametersError(
                f"Selected 'Budget Resource Name' column '{budget_col}' was not found in the input table."
            )

        # Fetch current budgets from Google Ads
        exec_context.set_progress(0.1, "Fetching current campaign budgets...")
        budget_data = self._fetch_current_budgets(client, customer_id, df, budget_col)

        # Calculate proposed budget changes
        exec_context.set_progress(0.3, "Calculating budget adjustments...")
        changes = self._calculate_budget_changes(
            df, budget_data, campaign_col, budget_col
        )

        # Apply changes or generate preview
        if self.execution_mode == ExecutionMode.APPLY.name:
            exec_context.set_progress(0.5, "Applying budget changes...")
            audit_records = self._apply_budget_changes(
                client, customer_id, changes, exec_context
            )
        else:
            exec_context.set_progress(0.5, "Generating preview...")
            audit_records = self._generate_preview(changes)

        # Build audit log DataFrame
        exec_context.set_progress(0.9, "Building audit log...")
        audit_df = pd.DataFrame(audit_records)

        # Ensure all columns exist even if empty
        expected_columns = [
            "campaign_resource_name",
            "campaign_budget_resource_name",
            "campaign_name",
            "current_budget",
            "proposed_budget",
            "budget_change",
            "budget_change_pct",
            "action",
            "status",
            "message",
            "timestamp",
            "is_shared_budget",
            "shared_with_campaigns",
        ]
        for col in expected_columns:
            if col not in audit_df.columns:
                audit_df[col] = None

        # Convert shared_with_campaigns list to comma-separated string for KNIME compatibility
        if "shared_with_campaigns" in audit_df.columns:
            audit_df["shared_with_campaigns"] = audit_df["shared_with_campaigns"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) else ""
            )

        audit_df = audit_df[expected_columns]

        exec_context.set_progress(1.0, "Complete")

        return knext.Table.from_pandas(audit_df)

    # ==========================================================================
    # HELPER METHODS
    # ==========================================================================

    @staticmethod
    def _mask_customer_id(customer_id: str) -> str:
        """Mask customer id for safer logs."""
        if not customer_id:
            return "<empty>"
        s = str(customer_id)
        if len(s) <= 4:
            return "****"
        return f"****{s[-4:]}"

    def _fetch_current_budgets(
        self, client: GoogleAdsClient, customer_id: str, df: pd.DataFrame, budget_col: str
    ) -> dict:
        """
        Fetch current budget amounts for all campaigns in the input table.
        Returns a dict mapping budget_resource_name to budget details.
        """
        budget_data = {}

        # Get unique budget resource names from the selected column
        budget_resources = df[budget_col].unique().tolist()

        if not budget_resources:
            return budget_data

        # Build GAQL query to fetch budget details
        ga_service = client.get_service("GoogleAdsService")

        fail_count = 0

        for budget_resource in budget_resources:
            try:
                query = f"""
                    SELECT
                        campaign_budget.resource_name,
                        campaign_budget.amount_micros,
                        campaign_budget.name,
                        campaign.resource_name,
                        campaign.name
                    FROM campaign_budget
                    WHERE campaign_budget.resource_name = '{budget_resource}'
                """

                response = ga_service.search(customer_id=customer_id, query=query)

                for row in response:
                    budget_data[budget_resource] = {
                        "amount_micros": row.campaign_budget.amount_micros,
                        "amount": row.campaign_budget.amount_micros / MICROS_PER_UNIT,
                        "name": row.campaign_budget.name,
                        "campaign_name": row.campaign.name,
                        "campaign_resource": row.campaign.resource_name,
                    }

            except GoogleAdsException as ex:
                error_msg = ex.failure.errors[0].message
                # Fail immediately if resource name is malformed - likely wrong column selected
                if "malformed" in error_msg.lower():
                    raise knext.InvalidParametersError(
                        f"Wrong column selected for Budget Resource Name.\n\n"
                        f"The value '{budget_resource}' is not a valid budget resource name.\n\n"
                        f"Expected format: 'customers/{{customer_id}}/campaignBudgets/{{budget_id}}'\n"
                        f"Example: 'customers/1234567890/campaignBudgets/5555555555'"
                    )
                fail_count += 1
                continue

        return budget_data

    def _calculate_budget_changes(
        self, df: pd.DataFrame, budget_data: dict, 
        campaign_col: str, budget_col: str
    ) -> List[dict]:
        """
        Calculate proposed budget changes for all campaigns in the input.
        Returns a list of change records.
        
        IMPORTANT: Handles shared budgets correctly - if multiple campaigns share 
        the same budget resource, the budget is only updated ONCE and all campaigns
        are reported with their shared status.
        """
        timestamp = datetime.now().isoformat()
        
        # First pass: validate and collect campaign data
        campaign_rows = []
        for _, row in df.iterrows():
            campaign_resource = row.get(campaign_col, "")
            budget_resource = row.get(budget_col, "")

            # Validate campaign resource name format (fail fast on first bad value)
            if campaign_resource and (
                not campaign_resource.startswith("customers/") or "/campaigns/" not in campaign_resource
            ):
                raise knext.InvalidParametersError(
                    f"Wrong column selected for Campaign Resource Name.\n\n"
                    f"The value '{campaign_resource}' is not a valid campaign resource name.\n\n"
                    f"Expected format: 'customers/{{customer_id}}/campaigns/{{campaign_id}}'\n"
                    f"Example: 'customers/1234567890/campaigns/9876543210'"
                )

            # Get current budget info
            budget_info = budget_data.get(budget_resource, {})
            current_budget = budget_info.get("amount", 0)
            campaign_name = budget_info.get("campaign_name", "Unknown")
            
            # Get KPI value if column is selected
            kpi_value = 0.0
            kpi_col = self.budget_settings.kpi_column
            if kpi_col and kpi_col in row:
                kpi_value = float(row[kpi_col]) if pd.notna(row[kpi_col]) else 0.0

            campaign_rows.append({
                "campaign_resource": campaign_resource,
                "budget_resource": budget_resource,
                "campaign_name": campaign_name,
                "current_budget": current_budget,
                "kpi_value": kpi_value,
            })

        # Detect shared budgets: group campaigns by budget_resource
        budget_to_campaigns = {}
        for i, row in enumerate(campaign_rows):
            budget_res = row["budget_resource"]
            if budget_res not in budget_to_campaigns:
                budget_to_campaigns[budget_res] = []
            budget_to_campaigns[budget_res].append(i)

        # For shared budgets, we only want to count the budget ONCE in calculations
        # Create a deduplicated list for allocation calculation
        unique_budget_rows = []
        budget_to_dedup_idx = {}
        for budget_res, campaign_indices in budget_to_campaigns.items():
            # Use the first campaign's data as representative for allocation
            first_idx = campaign_indices[0]
            budget_to_dedup_idx[budget_res] = len(unique_budget_rows)
            unique_budget_rows.append(campaign_rows[first_idx])

        # Calculate budget distribution based on mode (using deduplicated rows)
        if self.budget_settings.budget_cap_mode == BudgetCapMode.TOTAL_BUDGET.name:
            unique_allocations = self._calculate_total_budget_distribution(unique_budget_rows)
        else:
            unique_allocations = self._calculate_per_campaign_changes(unique_budget_rows)


        # Map allocations back to all campaigns (shared budgets get the same allocation)
        budget_allocations = []
        for campaign_row in campaign_rows:
            budget_res = campaign_row["budget_resource"]
            dedup_idx = budget_to_dedup_idx[budget_res]
            budget_allocations.append(unique_allocations[dedup_idx])

        # Track which budgets have already been marked for update (to avoid duplicate API calls)
        budgets_to_update = set()

        # Build change records
        changes = []
        for i, campaign_row in enumerate(campaign_rows):
            current_budget = campaign_row["current_budget"]
            budget_change = budget_allocations[i]
            budget_res = campaign_row["budget_resource"]
            
            # Check if this is a shared budget
            shared_campaign_indices = budget_to_campaigns[budget_res]
            is_shared = len(shared_campaign_indices) > 1
            shared_count = len(shared_campaign_indices)
            shared_campaign_names = [campaign_rows[idx]["campaign_name"] for idx in shared_campaign_indices]
            
            change_record = {
                "campaign_resource_name": campaign_row["campaign_resource"],
                "campaign_budget_resource_name": budget_res,
                "campaign_name": campaign_row["campaign_name"],
                "current_budget": current_budget,
                "proposed_budget": current_budget,
                "budget_change": 0.0,
                "budget_change_pct": 0.0,
                "action": "NO_CHANGE",
                "status": "PENDING",
                "message": "",
                "timestamp": timestamp,
                "is_shared_budget": is_shared,
                "shared_with_campaigns": shared_campaign_names if is_shared else [],
            }

            # Check if budget was found
            if current_budget == 0:
                change_record["action"] = "SKIPPED"
                change_record["message"] = self._build_rich_message(
                    action="SKIPPED",
                    reason="Could not fetch current budget from Google Ads",
                    campaign_name=campaign_row["campaign_name"],
                    current_budget=0,
                    proposed_budget=0,
                    budget_change=0,
                    budget_change_pct=0,
                    is_shared=is_shared,
                    shared_count=shared_count,
                    shared_campaigns=shared_campaign_names,
                )
                changes.append(change_record)
                continue

            # For shared budgets, only the FIRST campaign triggers the actual update
            if is_shared and budget_res in budgets_to_update:
                # This budget was already processed - mark as shared reference
                change_record["action"] = "SHARED_REF"
                change_record["status"] = "SHARED"
                change_record["message"] = self._build_rich_message(
                    action="SHARED_REF",
                    reason=f"Budget shared with {shared_count} campaigns - update applied via first campaign",
                    campaign_name=campaign_row["campaign_name"],
                    current_budget=current_budget,
                    proposed_budget=current_budget + budget_change if self.budget_settings.budget_direction != BudgetDirection.DECREASE.name else max(0, current_budget - budget_change),
                    budget_change=budget_change,
                    budget_change_pct=(budget_change / current_budget) * 100 if current_budget > 0 else 0,
                    is_shared=is_shared,
                    shared_count=shared_count,
                    shared_campaigns=shared_campaign_names,
                )
                changes.append(change_record)
                continue

            if budget_change > 0:
                # Mark this budget as processed (for shared budget deduplication)
                budgets_to_update.add(budget_res)
                
                # Determine direction
                is_decrease = self.budget_settings.budget_direction == BudgetDirection.DECREASE.name
                
                if is_decrease:
                    new_budget = max(0, current_budget - budget_change)  # Don't go below 0
                    actual_change = current_budget - new_budget
                    budget_change_pct = (actual_change / current_budget) * 100 if current_budget > 0 else 0
                    action = "DECREASE"
                else:
                    new_budget = current_budget + budget_change
                    budget_change_pct = (budget_change / current_budget) * 100 if current_budget > 0 else 0
                    actual_change = budget_change
                    action = "INCREASE"

                # Round to cents to avoid Google Ads "not a multiple of minimum unit" error
                new_budget = round(new_budget, 2)
                actual_change = round(actual_change, 2)

                change_record["proposed_budget"] = new_budget
                change_record["budget_change"] = actual_change if not is_decrease else -actual_change
                change_record["budget_change_pct"] = budget_change_pct if not is_decrease else -budget_change_pct
                change_record["action"] = action
                change_record["message"] = self._build_rich_message(
                    action=action,
                    reason=None,
                    campaign_name=campaign_row["campaign_name"],
                    current_budget=current_budget,
                    proposed_budget=new_budget,
                    budget_change=actual_change if not is_decrease else -actual_change,
                    budget_change_pct=budget_change_pct if not is_decrease else -budget_change_pct,
                    is_shared=is_shared,
                    shared_count=shared_count,
                    shared_campaigns=shared_campaign_names,
                )

            changes.append(change_record)

        return changes

    def _build_rich_message(
        self,
        action: str,
        reason: Optional[str],
        campaign_name: str,
        current_budget: float,
        proposed_budget: float,
        budget_change: float,
        budget_change_pct: float,
        is_shared: bool,
        shared_count: int,
        shared_campaigns: List[str],
    ) -> str:
        """
        Build a rich, agent-friendly message with all relevant context.
        This allows AI agents to understand the full picture of each change.
        """
        parts = []
        
        # Action summary
        if action == "INCREASE":
            parts.append(f"INCREASE: {campaign_name}")
            parts.append(f"Budget: ${current_budget:.2f} → ${proposed_budget:.2f} (+${abs(budget_change):.2f}, +{abs(budget_change_pct):.1f}%)")
        elif action == "DECREASE":
            parts.append(f"DECREASE: {campaign_name}")
            parts.append(f"Budget: ${current_budget:.2f} → ${proposed_budget:.2f} (-${abs(budget_change):.2f}, -{abs(budget_change_pct):.1f}%)")
        elif action == "SKIPPED":
            parts.append(f"SKIPPED: {campaign_name}")
            if reason:
                parts.append(f"Reason: {reason}")
        elif action == "SHARED_REF":
            parts.append(f"SHARED BUDGET (no duplicate update): {campaign_name}")
            parts.append(f"Budget: ${current_budget:.2f} → ${proposed_budget:.2f}")
        else:
            parts.append(f"NO CHANGE: {campaign_name}")
            parts.append(f"Budget remains at ${current_budget:.2f}")
        
        # Shared budget info
        if is_shared:
            other_campaigns = [c for c in shared_campaigns if c != campaign_name]
            if other_campaigns:
                parts.append(f"⚠️ Shared budget with: {', '.join(other_campaigns)}")
        
        return " | ".join(parts)

    def _calculate_per_campaign_changes(self, campaign_rows: List[dict]) -> List[float]:
        """
        Calculate budget changes using per-campaign mode.
        Each campaign gets a change capped by max_budget_change.
        """
        allocations = []
        
        for campaign in campaign_rows:
            current_budget = campaign["current_budget"]
            
            if current_budget == 0:
                allocations.append(0.0)
                continue

            # Calculate budget change based on mode
            if self.budget_settings.budget_step_mode == BudgetStepMode.ABSOLUTE.name:
                budget_change = self.budget_settings.budget_step_value
            else:
                budget_change = current_budget * (self.budget_settings.budget_step_value / 100.0)

            # Apply guardrail: cap at max budget change per campaign
            budget_change = min(budget_change, self.budget_settings.max_budget_change)
            allocations.append(budget_change)

        return allocations

    def _calculate_total_budget_distribution(self, campaign_rows: List[dict]) -> List[float]:
        """
        Calculate budget distribution using total budget mode.
        Distributes a total amount across all campaigns based on strategy.
        """
        total_budget = self.budget_settings.budget_step_value
        strategy = self.budget_settings.distribution_strategy

        
        # Filter to valid campaigns (those with current budget > 0)
        valid_indices = [i for i, c in enumerate(campaign_rows) if c["current_budget"] > 0]
        n_valid = len(valid_indices)
        
        if n_valid == 0:
            return [0.0] * len(campaign_rows)

        allocations = [0.0] * len(campaign_rows)

        if strategy == DistributionStrategy.EQUAL.name:
            # Equal split among all valid campaigns
            per_campaign = total_budget / n_valid
            for i in valid_indices:
                allocations[i] = per_campaign

        elif strategy == DistributionStrategy.PROPORTIONAL_SPEND.name:
            # Proportional to current budget
            total_current = sum(campaign_rows[i]["current_budget"] for i in valid_indices)
            if total_current > 0:
                for i in valid_indices:
                    weight = campaign_rows[i]["current_budget"] / total_current
                    allocations[i] = total_budget * weight

        elif strategy == DistributionStrategy.PROPORTIONAL_KPI.name:
            # Proportional to KPI value
            total_kpi = sum(campaign_rows[i]["kpi_value"] for i in valid_indices)
            if total_kpi > 0:
                for i in valid_indices:
                    weight = campaign_rows[i]["kpi_value"] / total_kpi
                    allocations[i] = total_budget * weight
            else:
                # Fallback to equal if no KPI data
                per_campaign = total_budget / n_valid
                for i in valid_indices:
                    allocations[i] = per_campaign

        return allocations

    def _generate_preview(self, changes: List[dict]) -> List[dict]:
        """
        Generate preview records without applying changes.
        """
        for change in changes:
            if change["action"] in ("INCREASE", "DECREASE"):
                change["status"] = "PREVIEW"
                change["message"] = f"[PREVIEW] {change['message']}"
            elif change["action"] == "SHARED_REF":
                change["status"] = "PREVIEW_SHARED"
                change["message"] = f"[PREVIEW] {change['message']}"
            elif change["action"] == "SKIPPED":
                change["status"] = "SKIPPED"
            else:
                change["status"] = "NO_ACTION"

        return changes

    def _apply_budget_changes(
        self,
        client: GoogleAdsClient,
        customer_id: str,
        changes: List[dict],
        exec_context: knext.ExecutionContext,
    ) -> List[dict]:
        """
        Apply budget changes to Google Ads via the API.
        
        IMPORTANT: Only applies changes for INCREASE/DECREASE actions.
        SHARED_REF actions are skipped (the budget was already updated via the primary campaign).
        """
        campaign_budget_service = client.get_service("CampaignBudgetService")

        # Count actionable changes (both INCREASE and DECREASE, but not SHARED_REF)
        actionable_changes = [c for c in changes if c["action"] in ("INCREASE", "DECREASE")]
        total_changes = len(actionable_changes)
        applied_count = 0
        failed_count = 0

        for change in changes:
            # Skip non-actionable records
            if change["action"] not in ("INCREASE", "DECREASE"):
                if change["action"] == "SKIPPED":
                    change["status"] = "SKIPPED"
                elif change["action"] == "SHARED_REF":
                    change["status"] = "SHARED_APPLIED"
                    # Keep the existing message (already set in _calculate_budget_changes)
                else:
                    change["status"] = "NO_ACTION"
                continue

            try:
                # Build the budget operation
                operation = client.get_type("CampaignBudgetOperation")
                budget = operation.update

                # Set the resource name
                budget.resource_name = change["campaign_budget_resource_name"]

                # Set the new budget amount in micros
                # Round to cents (2 decimal places) to avoid "not a multiple of minimum unit" error
                rounded_budget = round(change["proposed_budget"], 2)
                new_amount_micros = int(rounded_budget * MICROS_PER_UNIT)
                budget.amount_micros = new_amount_micros

                # Create field mask for the update
                field_mask = field_mask_pb2.FieldMask(paths=["amount_micros"])
                operation.update_mask.CopyFrom(field_mask)

                # Execute the mutation
                campaign_budget_service.mutate_campaign_budgets(
                    customer_id=customer_id, operations=[operation]
                )

                # Mark as successful
                change["status"] = "SUCCESS"
                # Append success info to existing rich message
                change["message"] = f"{change['message']} | ✅ Applied successfully"

                applied_count += 1
                progress = 0.5 + (0.4 * applied_count / max(total_changes, 1))
                exec_context.set_progress(
                    progress, f"Applied {applied_count}/{total_changes} changes"
                )

            except GoogleAdsException as ex:
                error_message = ex.failure.errors[0].message if ex.failure.errors else str(ex)
                # Fail immediately if resource name is malformed - likely wrong column selected
                if "malformed" in error_message.lower():
                    raise knext.InvalidParametersError(
                        f"Wrong column selected for Budget Resource Name.\n\n"
                        f"The value '{change['campaign_budget_resource_name']}' is not a valid budget resource name.\n\n"
                        f"Expected format: 'customers/{{customer_id}}/campaignBudgets/{{budget_id}}'\n"
                        f"Example: 'customers/1234567890/campaignBudgets/5555555555'"
                    )
                change["status"] = "FAILED"
                change["message"] = f"{change['message']} | ❌ API Error: {error_message}"
                failed_count += 1

            except Exception as ex:
                change["status"] = "FAILED"
                change["message"] = f"{change['message']} | ❌ Error: {str(ex)}"
                failed_count += 1
        return changes
