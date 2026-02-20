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
Utility functions for the Google Ads Budget Updater node.

Contains helper methods for fetching budgets, calculating changes,
and applying updates via the Google Ads API.
"""

from datetime import datetime
from typing import List, Optional

import knime.extension as knext
import pandas as pd

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf import field_mask_pb2


# Constants
MICROS_PER_UNIT = 1_000_000  # 1 currency unit = 1,000,000 micros


def mask_customer_id(customer_id: str) -> str:
    """Mask customer id for safer logs."""
    if not customer_id:
        return "<empty>"
    s = str(customer_id)
    if len(s) <= 4:
        return "****"
    return f"****{s[-4:]}"


def fetch_current_budgets(
    client: GoogleAdsClient, customer_id: str, df: pd.DataFrame, budget_col: str
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
                    f"Example: 'customers/1234567890/campaignBudgets/5555555555'"
                )
            continue

    return budget_data


def build_rich_message(
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


def calculate_per_campaign_changes(
    campaign_rows: List[dict],
    budget_step_mode: str,
    budget_step_value: float,
    max_budget_change: float,
) -> List[float]:
    """
    Calculate budget changes using per-campaign mode.
    Each campaign gets a change capped by max_budget_change.
    
    Args:
        campaign_rows: List of campaign data dictionaries with 'current_budget' key
        budget_step_mode: "ABSOLUTE" or "PERCENTAGE"
        budget_step_value: The step value (amount or percentage)
        max_budget_change: Maximum budget change per campaign (safety guardrail)
    """
    allocations = []
    
    for campaign in campaign_rows:
        current_budget = campaign["current_budget"]
        
        if current_budget == 0:
            allocations.append(0.0)
            continue

        # Calculate budget change based on mode
        if budget_step_mode == "ABSOLUTE":
            budget_change = budget_step_value
        else:
            budget_change = current_budget * (budget_step_value / 100.0)

        # Apply guardrail: cap at max budget change per campaign
        budget_change = min(budget_change, max_budget_change)
        allocations.append(budget_change)

    return allocations


def calculate_total_budget_distribution(
    campaign_rows: List[dict],
    total_budget: float,
    distribution_strategy: str,
) -> List[float]:
    """
    Calculate budget distribution using total budget mode.
    Distributes a total amount across all campaigns based on strategy.
    
    Args:
        campaign_rows: List of campaign data dictionaries with 'current_budget' and 'kpi_value' keys
        total_budget: The total budget to distribute
        distribution_strategy: "EQUAL", "PROPORTIONAL_SPEND", or "PROPORTIONAL_KPI"
    """
    # Filter to valid campaigns (those with current budget > 0)
    valid_indices = [i for i, c in enumerate(campaign_rows) if c["current_budget"] > 0]
    n_valid = len(valid_indices)
    
    if n_valid == 0:
        return [0.0] * len(campaign_rows)

    allocations = [0.0] * len(campaign_rows)

    if distribution_strategy == "EQUAL":
        # Equal split among all valid campaigns
        per_campaign = total_budget / n_valid
        for i in valid_indices:
            allocations[i] = per_campaign

    elif distribution_strategy == "PROPORTIONAL_SPEND":
        # Proportional to current budget
        total_current = sum(campaign_rows[i]["current_budget"] for i in valid_indices)
        if total_current > 0:
            for i in valid_indices:
                weight = campaign_rows[i]["current_budget"] / total_current
                allocations[i] = total_budget * weight

    elif distribution_strategy == "PROPORTIONAL_KPI":
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


def calculate_budget_changes(
    df: pd.DataFrame,
    budget_data: dict,
    campaign_col: str,
    budget_col: str,
    kpi_column: Optional[str],
    budget_cap_mode: str,
    budget_direction: str,
    budget_step_mode: str,
    budget_step_value: float,
    max_budget_change: float,
    distribution_strategy: str,
) -> List[dict]:
    """
    Calculate proposed budget changes for all campaigns in the input.
    Returns a list of change records.
    
    IMPORTANT: Handles shared budgets correctly - if multiple campaigns share 
    the same budget resource, the budget is only updated ONCE and all campaigns
    are reported with their shared status.
    
    Args:
        df: Input DataFrame with campaign data
        budget_data: Dict mapping budget_resource_name to budget details
        campaign_col: Column name for campaign resource names
        budget_col: Column name for budget resource names
        kpi_column: Optional column name for KPI values
        budget_cap_mode: "PER_CAMPAIGN" or "TOTAL_BUDGET"
        budget_direction: "INCREASE" or "DECREASE"
        budget_step_mode: "ABSOLUTE" or "PERCENTAGE"
        budget_step_value: The step value (amount, percentage, or total budget)
        max_budget_change: Maximum budget change per campaign (safety guardrail)
        distribution_strategy: "EQUAL", "PROPORTIONAL_SPEND", or "PROPORTIONAL_KPI"
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
        if kpi_column and kpi_column in row:
            kpi_value = float(row[kpi_column]) if pd.notna(row[kpi_column]) else 0.0

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
    if budget_cap_mode == "TOTAL_BUDGET":
        unique_allocations = calculate_total_budget_distribution(
            unique_budget_rows, budget_step_value, distribution_strategy
        )
    else:
        unique_allocations = calculate_per_campaign_changes(
            unique_budget_rows, budget_step_mode, budget_step_value, max_budget_change
        )

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
            change_record["message"] = build_rich_message(
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
            is_decrease = budget_direction == "DECREASE"
            proposed = max(0, current_budget - budget_change) if is_decrease else current_budget + budget_change
            change_record["action"] = "SHARED_REF"
            change_record["status"] = "SHARED"
            change_record["message"] = build_rich_message(
                action="SHARED_REF",
                reason=f"Budget shared with {shared_count} campaigns - update applied via first campaign",
                campaign_name=campaign_row["campaign_name"],
                current_budget=current_budget,
                proposed_budget=proposed,
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
            is_decrease = budget_direction == "DECREASE"
            
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
            change_record["message"] = build_rich_message(
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


def generate_preview(changes: List[dict]) -> List[dict]:
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


def apply_budget_changes(
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

    for change in changes:
        # Skip non-actionable records
        if change["action"] not in ("INCREASE", "DECREASE"):
            if change["action"] == "SKIPPED":
                change["status"] = "SKIPPED"
            elif change["action"] == "SHARED_REF":
                change["status"] = "SHARED_APPLIED"
                # Keep the existing message (already set in calculate_budget_changes)
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

        except Exception as ex:
            change["status"] = "FAILED"
            change["message"] = f"{change['message']} | ❌ Error: {str(ex)}"

    return changes
