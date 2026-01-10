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
from typing import List, Tuple

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
from util.utils import create_type_filer

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

    # 4. Amount value
    budget_step_absolute = knext.DoubleParameter(
        label="Amount",
        description="Fixed amount to change budget by (in currency units).",
        default_value=50.0,
        min_value=0.01,
    ).rule(
        knext.And(
            knext.OneOf(budget_cap_mode, [BudgetCapMode.PER_CAMPAIGN.name]),
            knext.OneOf(budget_step_mode, [BudgetStepMode.ABSOLUTE.name]),
        ),
        knext.Effect.SHOW,
    )

    # 4. Percentage value
    budget_step_percentage = knext.DoubleParameter(
        label="Percentage",
        description="Percentage to change budget by (e.g., 10 for 10%).",
        default_value=10.0,
        min_value=0.01,
        max_value=100.0,
    ).rule(
        knext.And(
            knext.OneOf(budget_cap_mode, [BudgetCapMode.PER_CAMPAIGN.name]),
            knext.OneOf(budget_step_mode, [BudgetStepMode.PERCENTAGE.name]),
        ),
        knext.Effect.SHOW,
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

    # 3. Total amount
    total_extra_budget = knext.DoubleParameter(
        label="Total amount",
        description="Total budget to distribute across ALL campaigns. Value in currency units.",
        default_value=1000.0,
        min_value=0.01,
    ).rule(
        knext.OneOf(budget_cap_mode, [BudgetCapMode.TOTAL_BUDGET.name]),
        knext.Effect.SHOW,
    )

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
    Increases campaign budgets by a fixed amount or percentage via the Google Ads API.

    This node is designed for **scheduled and agent-driven execution** in KNIME Hub with strict safety limits
    (guardrails) to prevent runaway budget increases.

    **Features**

    - **Campaign Budget Updates**: Increase daily budgets by a fixed amount or percentage.
    - **Two Budget Modes**: 
      - *Per Campaign*: Each campaign can increase up to a max amount.
      - *Total Budget*: Distribute a total extra budget across all campaigns.
    - **Distribution Strategies** (for Total Budget mode):
      - *Equal Split*: Distribute evenly among campaigns.
      - *Proportional to Current Budget*: Higher-budget campaigns get more.
      - *Proportional to KPI*: Campaigns with more conversions/MQLs get more.
    - **Preview Mode**: Review proposed changes before applying them.
    - **Deterministic Audit Output**: Full change log for compliance and debugging.

    **Configuration**

    1. **Column Selection**: Map your input table columns to the required fields (campaign resource, budget resource, optional KPI).
    2. **Budget Mode**: Choose Per Campaign or Total Budget distribution.
    3. **Execution Mode**: Choose Preview to review changes or Apply to execute them.

    **Input Table Requirements**

    The input table should contain columns for:
    - **Campaign Resource Name**: Google Ads campaign resource name (e.g., 'customers/123/campaigns/456').
    - **Budget Resource Name**: Google Ads campaign budget resource name (e.g., 'customers/123/campaignBudgets/789').
    - **KPI Column** (optional): Numeric column for KPI-based distribution (e.g., conversions, MQLs).

    **Tip**: Use upstream KNIME nodes (Row Filter, Rule-based Row Filter) to select which campaigns 
    should receive budget increases based on performance metrics like cost, conversions, or ROAS.

    **Mandatory Upstream Node**

    - Connect to the **Google Ads Connector** node to authenticate with the Google Ads API.

    **Output**

    1. **Audit / Change Log**: Detailed log of all proposed or applied changes.
    2. **Google Ads Connection**: Pass-through for downstream nodes.
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
    ) -> Tuple[knext.Schema, GoogleAdObjectSpec]:
        """
        Validate configuration and input table schema.
        Returns the output schema for the audit log table and the pass-through connection spec.
        """
        # Validate connection
        if not hasattr(spec, "account_id"):
            raise knext.InvalidParametersError(
                "Connect to the Google Ads Connector node."
            )

        # Column validation is handled by ColumnParameter - no need for manual checks

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
    ) -> Tuple[knext.Table, GoogleAdConnectionObject]:
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
        ]
        for col in expected_columns:
            if col not in audit_df.columns:
                audit_df[col] = None

        audit_df = audit_df[expected_columns]

        exec_context.set_progress(1.0, "Complete")

        return knext.Table.from_pandas(audit_df)

    # ==========================================================================
    # HELPER METHODS
    # ==========================================================================

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
                        f"Example: 'customers/1234567890/campaignBudgets/5555555555'\n\n"
                        f"Tip: Select the column containing 'campaign_budget.resource_name' values."
                    )
                LOGGER.warning(f"Failed to fetch budget for {budget_resource}: {error_msg}")
                continue

        return budget_data

    def _calculate_budget_changes(
        self, df: pd.DataFrame, budget_data: dict, 
        campaign_col: str, budget_col: str
    ) -> List[dict]:
        """
        Calculate proposed budget changes for all campaigns in the input.
        Returns a list of change records.
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
                    f"Example: 'customers/1234567890/campaigns/9876543210'\n\n"
                    f"Tip: Select the column containing 'campaign.resource_name' values."
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

        # Calculate budget distribution based on mode
        if self.budget_settings.budget_cap_mode == BudgetCapMode.TOTAL_BUDGET.name:
            budget_allocations = self._calculate_total_budget_distribution(campaign_rows)
        else:
            budget_allocations = self._calculate_per_campaign_changes(campaign_rows)

        # Build change records
        changes = []
        for i, campaign_row in enumerate(campaign_rows):
            current_budget = campaign_row["current_budget"]
            budget_change = budget_allocations[i]
            
            change_record = {
                "campaign_resource_name": campaign_row["campaign_resource"],
                "campaign_budget_resource_name": campaign_row["budget_resource"],
                "campaign_name": campaign_row["campaign_name"],
                "current_budget": current_budget,
                "proposed_budget": current_budget,
                "budget_change": 0.0,
                "budget_change_pct": 0.0,
                "action": "NO_CHANGE",
                "status": "PENDING",
                "message": "",
                "timestamp": timestamp,
            }

            # Check if budget was found
            if current_budget == 0:
                change_record["action"] = "SKIPPED"
                change_record["message"] = "Could not fetch current budget from Google Ads"
                changes.append(change_record)
                continue

            if budget_change > 0:
                # Determine direction
                is_decrease = self.budget_settings.budget_direction == BudgetDirection.DECREASE.name
                
                if is_decrease:
                    new_budget = max(0, current_budget - budget_change)  # Don't go below 0
                    actual_change = current_budget - new_budget
                    budget_change_pct = (actual_change / current_budget) * 100 if current_budget > 0 else 0
                    action = "DECREASE"
                    message = f"Decreasing budget by {actual_change:.2f} ({budget_change_pct:.1f}%)"
                else:
                    new_budget = current_budget + budget_change
                    budget_change_pct = (budget_change / current_budget) * 100 if current_budget > 0 else 0
                    actual_change = budget_change
                    action = "INCREASE"
                    message = f"Increasing budget by {budget_change:.2f} ({budget_change_pct:.1f}%)"

                change_record["proposed_budget"] = new_budget
                change_record["budget_change"] = actual_change if not is_decrease else -actual_change
                change_record["budget_change_pct"] = budget_change_pct if not is_decrease else -budget_change_pct
                change_record["action"] = action
                change_record["message"] = message

            changes.append(change_record)

        return changes

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
                budget_change = self.budget_settings.budget_step_absolute
            else:
                budget_change = current_budget * (self.budget_settings.budget_step_percentage / 100.0)

            # Apply guardrail: cap at max budget change per campaign
            budget_change = min(budget_change, self.budget_settings.max_budget_change)
            allocations.append(budget_change)

        return allocations

    def _calculate_total_budget_distribution(self, campaign_rows: List[dict]) -> List[float]:
        """
        Calculate budget distribution using total budget mode.
        Distributes total_extra_budget across all campaigns based on strategy.
        """
        total_budget = self.budget_settings.total_extra_budget
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
                LOGGER.warning("No KPI values found, falling back to equal distribution")

        return allocations

    def _generate_preview(self, changes: List[dict]) -> List[dict]:
        """
        Generate preview records without applying changes.
        """
        for change in changes:
            if change["action"] == "INCREASE":
                change["status"] = "PREVIEW"
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
        """
        campaign_budget_service = client.get_service("CampaignBudgetService")

        total_changes = len([c for c in changes if c["action"] == "INCREASE"])
        applied_count = 0

        for change in changes:
            if change["action"] != "INCREASE":
                if change["action"] == "SKIPPED":
                    change["status"] = "SKIPPED"
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
                new_amount_micros = int(change["proposed_budget"] * MICROS_PER_UNIT)
                budget.amount_micros = new_amount_micros

                # Create field mask for the update
                field_mask = field_mask_pb2.FieldMask(paths=["amount_micros"])
                operation.update_mask.CopyFrom(field_mask)

                # Execute the mutation
                response = campaign_budget_service.mutate_campaign_budgets(
                    customer_id=customer_id, operations=[operation]
                )

                # Mark as successful
                change["status"] = "SUCCESS"
                change["message"] = (
                    f"Budget updated successfully. "
                    f"Resource: {response.results[0].resource_name}"
                )

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
                        f"Example: 'customers/1234567890/campaignBudgets/5555555555'\n\n"
                        f"Tip: Select the column containing 'campaign_budget.resource_name' values."
                    )
                change["status"] = "FAILED"
                change["message"] = f"API Error: {error_message}"
                LOGGER.error(f"Failed to update budget: {error_message}")

            except Exception as ex:
                change["status"] = "FAILED"
                change["message"] = f"Error: {str(ex)}"
                LOGGER.error(f"Unexpected error updating budget: {str(ex)}")

        return changes
