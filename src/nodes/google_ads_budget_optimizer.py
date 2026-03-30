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

This node applies budget changes to campaigns via the Google Ads API. It is a pure executor — all
decision logic should be done upstream using standard KNIME nodes.
"""

import logging

import knime.extension as knext
import pandas as pd
import google_ads_ext

from google.ads.googleads.client import GoogleAdsClient

from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)
from util.utils import check_column, create_type_filer
from util.budget_optimizer_utils import (
    fetch_current_budgets,
    calculate_budget_changes,
    generate_preview,
    apply_budget_changes,
)

LOGGER = logging.getLogger(__name__)


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
        column_filter=lambda c: c.ktype in (knext.double(), knext.int32(), knext.int64()),
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
    icon_path="icons/Update-budget.png",
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
    Applies budget changes to campaigns via the Google Ads API.

    This node is a **pure executor** — all decision logic (rules, filtering, KPI thresholds)
    should be done upstream using KNIME nodes like Rule Engine, Row Filter, or Joiner.

    **Actions**

    - **Increase Budget**: Raise campaign daily budgets by a fixed amount or percentage.
    - **Decrease Budget**: Lower campaign daily budgets by a fixed amount or percentage.

    **Budget Modes**

    - **Per Campaign**: Each campaign's budget changes individually up to a configurable max.
    - **Across All Campaigns**: Distribute a total budget change across all campaigns using
      equal split, proportional to current budget, or proportional to a KPI column.

    **Workflow Example**

    1. Use **Google Ads Query** node to fetch campaign performance data (cost, conversions, ROAS)
    2. Join with CRM data (MQLs, opportunities, revenue) using **Joiner** node
    3. Apply rules using **Rule Engine** to decide which campaigns need budget changes
    4. Filter using **Row Filter** to keep only campaigns meeting your criteria
    5. Connect to this node to execute the budget updates

    **Preview Mode**

    Use Preview mode to review proposed changes before applying them. The output table
    will show what would happen without making any changes to your Google Ads account.

    **Shared Budget Handling**

    Google Ads allows multiple campaigns to share the same budget. The node automatically:

    - **Detects shared budgets**: Identifies when multiple campaigns in your input share
      the same budget resource.
    - **Updates once**: Applies the change only once (via the first campaign encountered).
    - **Tracks references**: Marks subsequent campaigns as `SHARED_REF` in the audit log.

    **Input Table Requirements**

    The input table should contain columns for:
    - **Campaign Resource Name**: e.g., 'customers/123/campaigns/456'
    - **Budget Resource Name**: e.g., 'customers/123/campaignBudgets/789'
    - **KPI Column** (optional): Numeric values for KPI-based distribution (conversions, MQLs, etc.)

    **Safety Guardrails**

    - **Max per campaign**: Configurable limit on how much any single campaign's budget can change.
    - **Preview mode**: Always review changes before applying to avoid unintended modifications.

    **Tip: Bulk Budget Updates**

    This node works with any input containing valid campaign and budget resource names. You can
    use it for bulk updates by preparing a table with your desired campaigns and connecting it
    directly — no need for a query node if you already have the resource names.
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
        budget_data = fetch_current_budgets(client, customer_id, df, budget_col)

        # Calculate proposed budget changes
        exec_context.set_progress(0.3, "Calculating budget adjustments...")
        changes = calculate_budget_changes(
            df=df,
            budget_data=budget_data,
            campaign_col=campaign_col,
            budget_col=budget_col,
            kpi_column=self.budget_settings.kpi_column,
            budget_cap_mode=self.budget_settings.budget_cap_mode,
            budget_direction=self.budget_settings.budget_direction,
            budget_step_mode=self.budget_settings.budget_step_mode,
            budget_step_value=self.budget_settings.budget_step_value,
            max_budget_change=self.budget_settings.max_budget_change,
            distribution_strategy=self.budget_settings.distribution_strategy,
        )

        # Apply changes or generate preview
        if self.execution_mode == ExecutionMode.APPLY.name:
            exec_context.set_progress(0.5, "Applying budget changes...")
            audit_records = apply_budget_changes(
                client, customer_id, changes, exec_context
            )
        else:
            exec_context.set_progress(0.5, "Generating preview...")
            audit_records = generate_preview(changes)

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
