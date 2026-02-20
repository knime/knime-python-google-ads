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
# ------------------------------------------------------------------------

"""
Utility functions for Google Ads Search Terms Actions node.

Contains helper functions for:
- Fetching existing criteria for conflict detection
- Conflict checking at different levels (shared list, campaign, ad group)
- Creating criteria via Google Ads API
- Building messages for preview and success states
"""

import logging
import pandas as pd

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from util.pre_built_ad_queries import FieldInspector

LOGGER = logging.getLogger(__name__)


# =============================================================================
# FETCH EXISTING CRITERIA
# =============================================================================


def fetch_existing_criteria(
    client: GoogleAdsClient, customer_id: str, df: pd.DataFrame,
    field_inspector: FieldInspector, batch_size: int = 100,
    shared_set_col: str = None, campaign_col: str = None, ad_group_col: str = None,
    scope: str = "campaign"
) -> dict:
    """
    Fetch existing keywords for comprehensive conflict detection.
    
    Unified function that handles all scopes:
    - shared_list: Start from shared sets, find linked campaigns/ad groups
    - campaign: Start from campaigns, find linked shared sets + ad groups
    - ad_group: Start from ad groups/campaigns, find linked shared sets
    
    Args:
        client: Google Ads API client
        customer_id: The customer ID to query
        df: DataFrame containing resource names
        field_inspector: FieldInspector for enum mapping
        batch_size: Number of IDs to query at once
        shared_set_col: Column name containing shared set resource names (for shared_list scope)
        campaign_col: Column name containing campaign resource names
        ad_group_col: Column name containing ad group resource names
        scope: One of 'shared_list', 'campaign', 'ad_group', 'keyword'
        
    Returns:
        dict: Maps conflict_key -> dict with 'type', 'level', 'location_name'
              Also includes metadata keys prefixed with '__'
    """
    
    existing = {}
    ga_service = client.get_service("GoogleAdsService")
    match_type_mapping = field_inspector._load_enum_mapping("KeywordMatchTypeEnum.KeywordMatchType")
    
    # Initialize metadata
    shared_set_names = {}
    campaign_names = {}
    ad_group_names = {}
    campaign_resource_to_id = {}
    ad_group_resource_to_id = {}
    ad_group_to_campaign = {}
    
    try:
        if scope == "shared_list":
            # ===============================================================
            # SHARED LIST SCOPE: Start from shared sets
            # ===============================================================
            if not shared_set_col or shared_set_col not in df.columns:
                return _finalize_result(existing, shared_set_names, campaign_names, ad_group_names,
                                       campaign_resource_to_id, ad_group_resource_to_id, ad_group_to_campaign)
            
            shared_sets = [s for s in df[shared_set_col].unique().tolist() if s and not pd.isna(s)]
            if not shared_sets:
                return _finalize_result(existing, shared_set_names, campaign_names, ad_group_names,
                                       campaign_resource_to_id, ad_group_resource_to_id, ad_group_to_campaign)
            
            # Extract shared set IDs
            shared_set_ids = []
            for res in shared_sets:
                parts = str(res).split('/')
                if len(parts) >= 4 and parts[2] == 'sharedSets':
                    shared_set_ids.append(parts[3])
                else:
                    LOGGER.debug(f"Could not parse shared set resource name: '{res}' (parts={parts})")
            
            if not shared_set_ids:
                return _finalize_result(existing, shared_set_names, campaign_names, ad_group_names,
                                       campaign_resource_to_id, ad_group_resource_to_id, ad_group_to_campaign)
            
            # Get keywords in the shared lists
            _fetch_shared_list_keywords(
                ga_service, customer_id, shared_set_ids, batch_size,
                match_type_mapping, existing, shared_set_names,
                key_prefix="sharedlist"
            )
            
            # Find campaigns linked to these shared sets
            linked_campaigns = _fetch_linked_campaigns_from_shared_sets(
                ga_service, customer_id, shared_set_ids, batch_size
            )
            
            if linked_campaigns:
                campaign_ids = list(linked_campaigns.keys())
                
                # Get campaign keywords (keyed by shared set)
                _fetch_campaign_keywords_for_shared_list_scope(
                    ga_service, customer_id, campaign_ids, batch_size,
                    match_type_mapping, existing, linked_campaigns
                )
                
                # Get ad group keywords (keyed by shared set)
                _fetch_adgroup_keywords_for_shared_list_scope(
                    ga_service, customer_id, campaign_ids, batch_size,
                    match_type_mapping, existing, linked_campaigns
                )
        
        else:
            # ===============================================================
            # CAMPAIGN/AD_GROUP/KEYWORD SCOPE: Start from campaigns
            # ===============================================================
            campaigns = []
            if campaign_col and campaign_col in df.columns:
                campaigns = [c for c in df[campaign_col].unique().tolist() if c]
            
            ad_groups = []
            if ad_group_col and ad_group_col in df.columns:
                ad_groups = [ag for ag in df[ad_group_col].unique().tolist() if ag and not pd.isna(ag)]
            
            # Extract campaign IDs
            campaign_ids = []
            for res in campaigns:
                parts = res.split('/')
                if len(parts) >= 4 and parts[2] == 'campaigns':
                    campaign_ids.append(parts[3])
                    campaign_resource_to_id[res] = parts[3]
            
            # Extract ad group IDs
            ad_group_ids = []
            for res in ad_groups:
                parts = str(res).split('/')
                if len(parts) >= 4 and parts[2] == 'adGroups':
                    ad_group_ids.append(parts[3])
                    ad_group_resource_to_id[res] = parts[3]
            
            # Get shared lists linked to campaigns
            if campaign_ids:
                shared_sets_by_campaign = _fetch_shared_sets_by_campaign(
                    ga_service, customer_id, campaign_ids, batch_size
                )
                
                # Get keywords from linked shared lists
                if shared_sets_by_campaign:
                    _fetch_shared_list_keywords_for_campaign_scope(
                        ga_service, customer_id, shared_sets_by_campaign, batch_size,
                        match_type_mapping, existing
                    )
                
                # Get campaign keywords
                _fetch_campaign_keywords(
                    ga_service, customer_id, campaign_ids, batch_size,
                    match_type_mapping, existing
                )
                
                # Get campaign names
                _fetch_campaign_names(
                    ga_service, customer_id, campaign_ids, batch_size, campaign_names
                )
            
            # Get ad group keywords
            if ad_group_ids:
                _fetch_adgroup_keywords(
                    ga_service, customer_id, ad_group_ids, batch_size,
                    match_type_mapping, existing, ad_group_to_campaign
                )
                
                # Get ad group names
                _fetch_adgroup_names(
                    ga_service, customer_id, ad_group_ids, batch_size, ad_group_names
                )
    
    except Exception as ex:
        LOGGER.debug(f"Error fetching existing criteria: {ex}", exc_info=True)
    
    return _finalize_result(existing, shared_set_names, campaign_names, ad_group_names,
                           campaign_resource_to_id, ad_group_resource_to_id, ad_group_to_campaign)


def _finalize_result(existing, shared_set_names, campaign_names, ad_group_names,
                    campaign_resource_to_id, ad_group_resource_to_id, ad_group_to_campaign):
    """Add metadata to the result dict."""
    existing['__shared_set_names__'] = shared_set_names
    existing['__campaign_names__'] = campaign_names
    existing['__ad_group_names__'] = ad_group_names
    existing['__campaign_resource_to_id__'] = campaign_resource_to_id
    existing['__ad_group_resource_to_id__'] = ad_group_resource_to_id
    existing['__ad_group_to_campaign__'] = ad_group_to_campaign
    return existing


# =============================================================================
# QUERY HELPERS
# =============================================================================


def _fetch_shared_list_keywords(
    ga_service, customer_id: str, shared_set_ids: list, batch_size: int,
    match_type_mapping: dict, existing: dict, shared_set_names: dict,
    key_prefix: str = "sharedlist"
):
    """Fetch keywords from shared lists."""
    for i in range(0, len(shared_set_ids), batch_size):
        batch_ids = shared_set_ids[i:i + batch_size]
        query = f"""
            SELECT
                shared_criterion.keyword.text,
                shared_criterion.keyword.match_type,
                shared_criterion.shared_set,
                shared_set.name
            FROM shared_criterion
            WHERE shared_set.id IN ({','.join(batch_ids)})
                AND shared_criterion.type = 'KEYWORD'
        """
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                term = row.shared_criterion.keyword.text.lower().strip()
                match_raw = match_type_mapping.get(row.shared_criterion.keyword.match_type, "")
                match = match_raw.upper().replace(" ", "_") if match_raw else str(row.shared_criterion.keyword.match_type)
                shared_set_resource = row.shared_criterion.shared_set
                shared_set_names[shared_set_resource] = row.shared_set.name
                
                key = f"{key_prefix}:{shared_set_resource}:{term}:{match}"
                existing[key] = {
                    'type': 'NEGATIVE',
                    'level': 'SHARED_LIST',
                    'location_name': row.shared_set.name
                }
                LOGGER.debug(f"    Found shared list keyword: '{term}' ({match}) in '{row.shared_set.name}'")
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error querying shared criteria: {gex.failure.errors if hasattr(gex, 'failure') else gex}")
            LOGGER.debug(f"  Query was: {query}")


def _fetch_linked_campaigns_from_shared_sets(
    ga_service, customer_id: str, shared_set_ids: list, batch_size: int
) -> dict:
    """Find campaigns linked to shared sets."""
    linked_campaigns = {}  # campaign_id -> {'name': str, 'shared_sets': set}
    
    for i in range(0, len(shared_set_ids), batch_size):
        batch_ids = shared_set_ids[i:i + batch_size]
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign_shared_set.shared_set
            FROM campaign_shared_set
            WHERE shared_set.id IN ({','.join(batch_ids)})
                AND campaign_shared_set.status = 'ENABLED'
        """
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                camp_id = str(row.campaign.id)
                if camp_id not in linked_campaigns:
                    linked_campaigns[camp_id] = {'name': row.campaign.name, 'shared_sets': set()}
                linked_campaigns[camp_id]['shared_sets'].add(row.campaign_shared_set.shared_set)
                LOGGER.debug(f"    Campaign '{row.campaign.name}' (ID: {camp_id}) linked to shared set")
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error querying campaign_shared_set: {gex.failure.errors if hasattr(gex, 'failure') else gex}")
            LOGGER.debug(f"  Query was: {query}")
    
    return linked_campaigns


def _fetch_campaign_keywords_for_shared_list_scope(
    ga_service, customer_id: str, campaign_ids: list, batch_size: int,
    match_type_mapping: dict, existing: dict, linked_campaigns: dict
):
    """Fetch campaign keywords, keyed by shared set resource."""
    for i in range(0, len(campaign_ids), batch_size):
        batch_ids = campaign_ids[i:i + batch_size]
        query = f"""
            SELECT
                campaign_criterion.keyword.text,
                campaign_criterion.keyword.match_type,
                campaign_criterion.negative,
                campaign.id,
                campaign.name
            FROM campaign_criterion
            WHERE campaign.id IN ({','.join(batch_ids)})
                AND campaign_criterion.type = 'KEYWORD'
                AND campaign_criterion.status != 'REMOVED'
        """
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                term = row.campaign_criterion.keyword.text.lower().strip()
                match_raw = match_type_mapping.get(row.campaign_criterion.keyword.match_type, "")
                match = match_raw.upper().replace(" ", "_") if match_raw else str(row.campaign_criterion.keyword.match_type)
                is_neg = row.campaign_criterion.negative
                camp_id = str(row.campaign.id)
                
                # Key by shared set resource for shared list scope
                for shared_set_res in linked_campaigns[camp_id]['shared_sets']:
                    key = f"campaign:{shared_set_res}:{term}:{match}"
                    if key not in existing:
                        existing[key] = {
                            'type': 'NEGATIVE' if is_neg else 'POSITIVE',
                            'level': 'CAMPAIGN',
                            'location_name': row.campaign.name
                        }
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error querying campaign keywords: {gex.failure.errors if hasattr(gex, 'failure') else gex}")
            LOGGER.debug(f"  Query was: {query}")


def _fetch_adgroup_keywords_for_shared_list_scope(
    ga_service, customer_id: str, campaign_ids: list, batch_size: int,
    match_type_mapping: dict, existing: dict, linked_campaigns: dict
):
    """Fetch ad group keywords, keyed by shared set resource."""
    for i in range(0, len(campaign_ids), batch_size):
        batch_ids = campaign_ids[i:i + batch_size]
        query = f"""
            SELECT
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.negative,
                ad_group.name,
                campaign.id
            FROM ad_group_criterion
            WHERE campaign.id IN ({','.join(batch_ids)})
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
                camp_id = str(row.campaign.id)
                
                # Key by shared set resource for shared list scope
                for shared_set_res in linked_campaigns[camp_id]['shared_sets']:
                    key = f"adgroup:{shared_set_res}:{term}:{match}"
                    if key not in existing:
                        existing[key] = {
                            'type': 'NEGATIVE' if is_neg else 'POSITIVE',
                            'level': 'AD_GROUP',
                            'location_name': row.ad_group.name
                        }
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error querying ad group keywords: {gex.failure.errors if hasattr(gex, 'failure') else gex}")
            LOGGER.debug(f"  Query was: {query}")


def _fetch_shared_sets_by_campaign(
    ga_service, customer_id: str, campaign_ids: list, batch_size: int
) -> dict:
    """Get shared sets linked to campaigns."""
    shared_sets_by_campaign = {}  # campaign_id -> [(shared_set_resource, shared_set_name)]
    
    for i in range(0, len(campaign_ids), batch_size):
        batch_ids = campaign_ids[i:i + batch_size]
        query = f"""
            SELECT
                campaign.id,
                shared_set.resource_name,
                shared_set.name
            FROM campaign_shared_set
            WHERE campaign.id IN ({','.join(batch_ids)})
                AND shared_set.type = 'NEGATIVE_KEYWORDS'
                AND campaign_shared_set.status = 'ENABLED'
        """
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                camp_id = str(row.campaign.id)
                if camp_id not in shared_sets_by_campaign:
                    shared_sets_by_campaign[camp_id] = []
                shared_sets_by_campaign[camp_id].append((row.shared_set.resource_name, row.shared_set.name))
                LOGGER.debug(f"    Campaign {camp_id} has shared set '{row.shared_set.name}'")
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error querying campaign_shared_set: {gex.failure.errors if hasattr(gex, 'failure') else gex}")
            LOGGER.debug(f"  Query was: {query}")
    
    return shared_sets_by_campaign


def _fetch_shared_list_keywords_for_campaign_scope(
    ga_service, customer_id: str, shared_sets_by_campaign: dict, batch_size: int,
    match_type_mapping: dict, existing: dict
):
    """Fetch shared list keywords, keyed by campaign ID."""
    # Collect all unique shared set IDs
    all_shared_set_ids = set()
    for camp_id, shared_sets in shared_sets_by_campaign.items():
        for ss_res, ss_name in shared_sets:
            parts = ss_res.split('/')
            if len(parts) >= 4 and parts[2] == 'sharedSets':
                all_shared_set_ids.add(parts[3])
    
    if not all_shared_set_ids:
        LOGGER.debug("  No shared set IDs found, returning")
        return
    
    shared_set_ids_list = list(all_shared_set_ids)
    for i in range(0, len(shared_set_ids_list), batch_size):
        batch_ids = shared_set_ids_list[i:i + batch_size]
        query = f"""
            SELECT
                shared_criterion.keyword.text,
                shared_criterion.keyword.match_type,
                shared_criterion.shared_set,
                shared_set.name
            FROM shared_criterion
            WHERE shared_set.id IN ({','.join(batch_ids)})
                AND shared_criterion.type = 'KEYWORD'
        """
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                term = row.shared_criterion.keyword.text.lower().strip()
                match_raw = match_type_mapping.get(row.shared_criterion.keyword.match_type, "")
                match = match_raw.upper().replace(" ", "_") if match_raw else str(row.shared_criterion.keyword.match_type)
                ss_resource = row.shared_criterion.shared_set
                
                # Key by campaign ID for campaign/ad_group scope
                for camp_id, shared_sets in shared_sets_by_campaign.items():
                    for ss_res, ss_name in shared_sets:
                        if ss_res == ss_resource:
                            key = f"sharedlist:campaign:{camp_id}:{term}:{match}"
                            if key not in existing:
                                existing[key] = {
                                    'type': 'NEGATIVE',
                                    'level': 'SHARED_LIST',
                                    'location_name': row.shared_set.name
                                }
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error querying shared criteria: {gex.failure.errors if hasattr(gex, 'failure') else gex}")
            LOGGER.debug(f"  Query was: {query}")


def _fetch_campaign_keywords(
    ga_service, customer_id: str, campaign_ids: list, batch_size: int,
    match_type_mapping: dict, existing: dict
):
    """Fetch campaign keywords, keyed by campaign ID."""
    for i in range(0, len(campaign_ids), batch_size):
        batch_ids = campaign_ids[i:i + batch_size]
        query = f"""
            SELECT
                campaign_criterion.keyword.text,
                campaign_criterion.keyword.match_type,
                campaign_criterion.negative,
                campaign.id,
                campaign.name
            FROM campaign_criterion
            WHERE campaign.id IN ({','.join(batch_ids)})
                AND campaign_criterion.type = 'KEYWORD'
                AND campaign_criterion.status != 'REMOVED'
        """
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                term = row.campaign_criterion.keyword.text.lower().strip()
                match_raw = match_type_mapping.get(row.campaign_criterion.keyword.match_type, "")
                match = match_raw.upper().replace(" ", "_") if match_raw else str(row.campaign_criterion.keyword.match_type)
                is_neg = row.campaign_criterion.negative
                camp_id = str(row.campaign.id)
                
                key = f"campaign:{camp_id}:{term}:{match}"
                if key not in existing:
                    existing[key] = {
                        'type': 'NEGATIVE' if is_neg else 'POSITIVE',
                        'level': 'CAMPAIGN',
                        'location_name': row.campaign.name
                    }
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error querying campaign keywords: {gex.failure.errors if hasattr(gex, 'failure') else gex}")
            LOGGER.debug(f"  Query was: {query}")


def _fetch_adgroup_keywords(
    ga_service, customer_id: str, ad_group_ids: list, batch_size: int,
    match_type_mapping: dict, existing: dict, ad_group_to_campaign: dict
):
    """Fetch ad group keywords, keyed by ad group ID."""
    for i in range(0, len(ad_group_ids), batch_size):
        batch_ids = ad_group_ids[i:i + batch_size]
        query = f"""
            SELECT
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.negative,
                ad_group.id,
                ad_group.name,
                campaign.id
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
                ag_id = str(row.ad_group.id)
                camp_id = str(row.campaign.id)
                
                ad_group_to_campaign[ag_id] = camp_id
                
                key = f"adgroup:{ag_id}:{term}:{match}"
                if key not in existing:
                    existing[key] = {
                        'type': 'NEGATIVE' if is_neg else 'POSITIVE',
                        'level': 'AD_GROUP',
                        'location_name': row.ad_group.name,
                        'campaign_id': camp_id
                    }
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error querying ad group keywords: {gex.failure.errors if hasattr(gex, 'failure') else gex}")
            LOGGER.debug(f"  Query was: {query}")


def _fetch_campaign_names(
    ga_service, customer_id: str, campaign_ids: list, batch_size: int, campaign_names: dict
):
    """Fetch campaign resource_name -> name mapping."""
    for i in range(0, len(campaign_ids), batch_size):
        batch_ids = campaign_ids[i:i + batch_size]
        query = f"""SELECT campaign.resource_name, campaign.name FROM campaign WHERE campaign.id IN ({','.join(batch_ids)})"""
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                campaign_names[row.campaign.resource_name] = row.campaign.name
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error fetching campaign names: {gex.failure.errors if hasattr(gex, 'failure') else gex}")


def _fetch_adgroup_names(
    ga_service, customer_id: str, ad_group_ids: list, batch_size: int, ad_group_names: dict
):
    """Fetch ad_group resource_name -> name mapping."""
    for i in range(0, len(ad_group_ids), batch_size):
        batch_ids = ad_group_ids[i:i + batch_size]
        query = f"""SELECT ad_group.resource_name, ad_group.name FROM ad_group WHERE ad_group.id IN ({','.join(batch_ids)})"""
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                ad_group_names[row.ad_group.resource_name] = row.ad_group.name
        except GoogleAdsException as gex:
            LOGGER.debug(f"Error fetching ad group names: {gex.failure.errors if hasattr(gex, 'failure') else gex}")


# =============================================================================
# CONFLICT CHECKING
# =============================================================================


def check_shared_list_conflicts(
    search_term: str, match_type: str, shared_set_resource: str,
    existing_criteria: dict, shared_set_name: str
) -> dict | None:
    """
    Check for conflicts when adding a keyword to a shared list.
    
    Checks in priority order:
    1. Already exists in the target shared list (same level - blocking)
    2. Exists as positive keyword at campaign level (warning)
    3. Exists as negative keyword at campaign level (warning)
    4. Exists as positive keyword at ad group level (warning)
    5. Exists as negative keyword at ad group level (warning)
    
    Returns:
        dict with 'status' and 'message' if conflict found, None otherwise
    """
    term_lower = search_term.lower().strip()
    
    # Check 1: Already exists in the shared list (same level - blocks addition)
    key_sharedlist = f"sharedlist:{shared_set_resource}:{term_lower}:{match_type}"
    if key_sharedlist in existing_criteria:
        info = existing_criteria[key_sharedlist]
        return {
            'status': 'ALREADY_EXISTS',
            'message': f"'{search_term}' already exists as {match_type} negative in shared list '{info['location_name']}'"
        }
    
    # Check 2: Exists at campaign level (linked campaigns)
    key_campaign = f"campaign:{shared_set_resource}:{term_lower}:{match_type}"
    if key_campaign in existing_criteria:
        info = existing_criteria[key_campaign]
        kw_type = "negative" if info['type'] == 'NEGATIVE' else "positive keyword"
        return {
            'status': f"CONFLICT_CAMPAIGN_{info['type']}",
            'message': f"'{search_term}' exists as {match_type} {kw_type} at campaign level in '{info['location_name']}' (linked to this shared list)"
        }
    
    # Check 3: Exists at ad group level (in linked campaigns)
    key_adgroup = f"adgroup:{shared_set_resource}:{term_lower}:{match_type}"
    if key_adgroup in existing_criteria:
        info = existing_criteria[key_adgroup]
        kw_type = "negative" if info['type'] == 'NEGATIVE' else "positive keyword"
        return {
            'status': f"CONFLICT_ADGROUP_{info['type']}",
            'message': f"'{search_term}' exists as {match_type} {kw_type} at ad group level in '{info['location_name']}' (in campaign linked to this shared list)"
        }
    
    return None


def check_campaign_conflicts(
    search_term: str, match_type: str, campaign_resource: str,
    existing_criteria: dict, campaign_name: str
) -> dict | None:
    """
    Check for conflicts when adding a campaign-level negative.
    
    Checks:
    1. Already exists as campaign negative (same level - blocking)
    2. Exists in a shared list linked to this campaign (higher level)
    3. Exists as positive/negative at ad group level (lower level)
    
    Returns:
        dict with 'status' and 'message' if conflict found, None otherwise
    """
    term_lower = search_term.lower().strip()
    campaign_resource_to_id = existing_criteria.get('__campaign_resource_to_id__', {})
    camp_id = campaign_resource_to_id.get(campaign_resource, '')
    
    if not camp_id:
        # Try to extract from resource name
        parts = campaign_resource.split('/')
        if len(parts) >= 4 and parts[2] == 'campaigns':
            camp_id = parts[3]
    
    if not camp_id:
        return None
    
    # Check 1: Already exists as campaign negative/positive (same level)
    key_campaign = f"campaign:{camp_id}:{term_lower}:{match_type}"
    if key_campaign in existing_criteria:
        info = existing_criteria[key_campaign]
        if info['type'] == 'NEGATIVE':
            return {
                'status': 'ALREADY_EXISTS',
                'message': f"'{search_term}' already exists as {match_type} negative at campaign level in '{info['location_name']}'"
            }
        else:
            return {
                'status': 'CONFLICT_CAMPAIGN_POSITIVE',
                'message': f"'{search_term}' exists as {match_type} positive keyword at campaign level in '{info['location_name']}'"
            }
    
    # Check 2: Exists in a shared list linked to this campaign (higher level)
    key_sharedlist = f"sharedlist:campaign:{camp_id}:{term_lower}:{match_type}"
    if key_sharedlist in existing_criteria:
        info = existing_criteria[key_sharedlist]
        return {
            'status': 'CONFLICT_SHAREDLIST_NEGATIVE',
            'message': f"'{search_term}' exists as {match_type} negative in shared list '{info['location_name']}' (linked to this campaign)"
        }
    
    # Check 3: Exists at ad group level (lower level) - scan all ad groups for this campaign
    for key, info in existing_criteria.items():
        if key.startswith('adgroup:') and isinstance(info, dict) and info.get('campaign_id') == camp_id:
            # Parse the key: adgroup:{ag_id}:{term}:{match}
            parts = key.split(':')
            if len(parts) >= 4:
                key_term = parts[2]
                key_match = parts[3]
                if key_term == term_lower and key_match == match_type:
                    kw_type = "negative" if info['type'] == 'NEGATIVE' else "positive keyword"
                    return {
                        'status': f"CONFLICT_ADGROUP_{info['type']}",
                        'message': f"'{search_term}' exists as {match_type} {kw_type} at ad group level in '{info['location_name']}'"
                    }
    
    return None


def check_adgroup_conflicts(
    search_term: str, match_type: str, ad_group_resource: str,
    campaign_resource: str, existing_criteria: dict, ad_group_name: str,
    campaign_name: str, is_negative: bool
) -> dict | None:
    """
    Check for conflicts when adding an ad group-level keyword or negative.
    
    For negatives, checks:
    1. Already exists as ad group negative (same level - blocking)
    2. Exists as positive keyword in same ad group (conflict)
    3. Exists at campaign level (higher level)
    4. Exists in a shared list linked to the campaign (higher level)
    
    For keyword promotion, checks:
    1. Already exists as positive keyword (same level - blocking)
    2. Exists as negative in same ad group (conflict)
    3. Exists as campaign negative (higher level)
    4. Exists in a shared list linked to the campaign (higher level)
    
    Returns:
        dict with 'status' and 'message' if conflict found, None otherwise
    """
    term_lower = search_term.lower().strip()
    
    ad_group_resource_to_id = existing_criteria.get('__ad_group_resource_to_id__', {})
    campaign_resource_to_id = existing_criteria.get('__campaign_resource_to_id__', {})
    ad_group_to_campaign = existing_criteria.get('__ad_group_to_campaign__', {})
    
    ag_id = ad_group_resource_to_id.get(ad_group_resource, '')
    if not ag_id:
        parts = str(ad_group_resource).split('/')
        if len(parts) >= 4 and parts[2] == 'adGroups':
            ag_id = parts[3]
    
    camp_id = campaign_resource_to_id.get(campaign_resource, '')
    if not camp_id:
        parts = campaign_resource.split('/')
        if len(parts) >= 4 and parts[2] == 'campaigns':
            camp_id = parts[3]
    # Also try from ad_group_to_campaign mapping
    if not camp_id and ag_id:
        camp_id = ad_group_to_campaign.get(ag_id, '')
    
    if not ag_id:
        return None
    
    # Check ad group level
    key_adgroup = f"adgroup:{ag_id}:{term_lower}:{match_type}"
    if key_adgroup in existing_criteria:
        info = existing_criteria[key_adgroup]
        if is_negative:
            # Adding negative - check if already exists
            if info['type'] == 'NEGATIVE':
                return {
                    'status': 'ALREADY_EXISTS',
                    'message': f"'{search_term}' already exists as {match_type} negative at ad group level in '{info['location_name']}'"
                }
            else:
                return {
                    'status': 'CONFLICT_ADGROUP_POSITIVE',
                    'message': f"'{search_term}' exists as {match_type} positive keyword in ad group '{info['location_name']}'"
                }
        else:
            # Adding positive keyword - check if already exists
            if info['type'] == 'POSITIVE':
                return {
                    'status': 'ALREADY_EXISTS',
                    'message': f"'{search_term}' already exists as {match_type} keyword in ad group '{info['location_name']}'"
                }
            else:
                return {
                    'status': 'CONFLICT_ADGROUP_NEGATIVE',
                    'message': f"'{search_term}' exists as {match_type} negative in ad group '{info['location_name']}'"
                }
    
    if camp_id:
        # Check campaign level (higher level)
        key_campaign = f"campaign:{camp_id}:{term_lower}:{match_type}"
        if key_campaign in existing_criteria:
            info = existing_criteria[key_campaign]
            kw_type = "negative" if info['type'] == 'NEGATIVE' else "positive keyword"
            return {
                'status': f"CONFLICT_CAMPAIGN_{info['type']}",
                'message': f"'{search_term}' exists as {match_type} {kw_type} at campaign level in '{info['location_name']}'"
            }
        
        # Check shared lists linked to campaign (higher level)
        key_sharedlist = f"sharedlist:campaign:{camp_id}:{term_lower}:{match_type}"
        if key_sharedlist in existing_criteria:
            info = existing_criteria[key_sharedlist]
            return {
                'status': 'CONFLICT_SHAREDLIST_NEGATIVE',
                'message': f"'{search_term}' exists as {match_type} negative in shared list '{info['location_name']}' (linked to campaign)"
            }
    
    return None


# =============================================================================
# CRITERION CREATION
# =============================================================================


def create_criterion(
    client: GoogleAdsClient, customer_id: str, search_term: str,
    match_type: str, campaign_resource: str, ad_group_resource: str,
    is_negative: bool, is_campaign_level: bool,
    shared_set_resource: str = "", is_shared_list: bool = False,
    manager_account_id: str = ""
) -> str:
    """Create a keyword or negative keyword criterion.
    
    Args:
        client: Google Ads API client
        customer_id: The customer ID
        search_term: The keyword text
        match_type: Match type (EXACT, PHRASE, BROAD)
        campaign_resource: Campaign resource name
        ad_group_resource: Ad group resource name
        is_negative: True if adding negative keyword
        is_campaign_level: True if campaign-level (vs ad group-level)
        shared_set_resource: Shared set resource name (for shared list scope)
        is_shared_list: True if adding to shared list
        manager_account_id: The MCC account ID (for MCC-owned shared lists)
        
    Returns:
        str: The created criterion resource name
    """
    match_type_enum = getattr(
        client.enums.KeywordMatchTypeEnum, match_type,
        client.enums.KeywordMatchTypeEnum.EXACT
    )

    if is_shared_list:
        # Shared negative keyword list (MCC-level or account-level)
        # For MCC-owned shared lists, use manager_account_id
        # The resource name from queries shows client's ID, but mutations need owner's ID
        shared_set_owner_id = manager_account_id if manager_account_id else customer_id
        
        # Also update the shared_set_resource to use the correct customer ID
        # Safely update only the customer ID segment in the resource name
        if manager_account_id and shared_set_resource:
            parts = shared_set_resource.split('/')
            if len(parts) >= 4 and parts[0] == 'customers':
                parts[1] = manager_account_id
                shared_set_resource = '/'.join(parts)
        
        service = client.get_service("SharedCriterionService")
        operation = client.get_type("SharedCriterionOperation")
        criterion = operation.create

        criterion.shared_set = shared_set_resource
        criterion.keyword.text = search_term
        criterion.keyword.match_type = match_type_enum

        try:
            response = service.mutate_shared_criteria(
                customer_id=shared_set_owner_id, operations=[operation]
            )
            result = response.results[0].resource_name
            return result
        except GoogleAdsException as gex:
            LOGGER.debug(f"GoogleAdsException adding to shared list: customer_id='{shared_set_owner_id}', shared_set='{shared_set_resource}', keyword='{search_term}'")
            for error in gex.failure.errors:
                LOGGER.debug(f"  Error code: {error.error_code}")
                LOGGER.debug(f"  Error message: {error.message}")
                if error.location:
                    for field_path in error.location.field_path_elements:
                        LOGGER.debug(f"  Field: {field_path.field_name}")
            raise

    elif is_negative and is_campaign_level:
        # Campaign-level negative
        service = client.get_service("CampaignCriterionService")
        operation = client.get_type("CampaignCriterionOperation")
        criterion = operation.create

        criterion.campaign = campaign_resource
        criterion.negative = True
        criterion.keyword.text = search_term
        criterion.keyword.match_type = match_type_enum

        try:
            response = service.mutate_campaign_criteria(
                customer_id=customer_id, operations=[operation]
            )
            result = response.results[0].resource_name
            return result
        except GoogleAdsException as gex:
            LOGGER.debug(f"GoogleAdsException adding campaign negative: customer_id='{customer_id}', campaign='{campaign_resource}', keyword='{search_term}'")
            for error in gex.failure.errors:
                LOGGER.debug(f"  Error code: {error.error_code}")
                LOGGER.debug(f"  Error message: {error.message}")
            raise

    elif is_negative:
        # Ad group-level negative
        service = client.get_service("AdGroupCriterionService")
        operation = client.get_type("AdGroupCriterionOperation")
        criterion = operation.create

        criterion.ad_group = ad_group_resource
        criterion.negative = True
        criterion.keyword.text = search_term
        criterion.keyword.match_type = match_type_enum

        try:
            response = service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=[operation]
            )
            result = response.results[0].resource_name
            LOGGER.debug(f"  Success! Created ad group negative criterion: {result}")
            return result
        except GoogleAdsException as gex:
            LOGGER.debug(f"GoogleAdsException adding ad group negative: customer_id='{customer_id}', ad_group='{ad_group_resource}', keyword='{search_term}'")
            for error in gex.failure.errors:
                LOGGER.debug(f"  Error code: {error.error_code}")
                LOGGER.debug(f"  Error message: {error.message}")
            raise

    else:
        # Positive keyword
        service = client.get_service("AdGroupCriterionService")
        operation = client.get_type("AdGroupCriterionOperation")
        criterion = operation.create

        criterion.ad_group = ad_group_resource
        criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
        criterion.keyword.text = search_term
        criterion.keyword.match_type = match_type_enum

        try:
            response = service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=[operation]
            )
            result = response.results[0].resource_name
            LOGGER.debug(f"  Success! Created positive keyword criterion: {result}")
            return result
        except GoogleAdsException as gex:
            LOGGER.debug(f"GoogleAdsException adding positive keyword: customer_id='{customer_id}', ad_group='{ad_group_resource}', keyword='{search_term}'")
            for error in gex.failure.errors:
                LOGGER.debug(f"  Error code: {error.error_code}")
                LOGGER.debug(f"  Error message: {error.message}")
            raise


# =============================================================================
# MESSAGE BUILDING
# =============================================================================


def build_preview_message(
    search_term: str, action_name: str, match_type: str, is_campaign_level: bool,
    campaign_name: str = "", ad_group_name: str = "",
    shared_set_name: str = "", is_shared_list: bool = False
) -> str:
    """Build a preview message for the audit log."""
    if action_name == "ADD_NEGATIVE_SHARED_LIST":
        location = f"to shared list '{shared_set_name}'" if shared_set_name else "to shared negative list"
        return f"Will add '{search_term}' as {match_type} negative {location}"
    elif action_name == "ADD_NEGATIVE_CAMPAIGN":
        location = f"to campaign '{campaign_name}'" if campaign_name else "as campaign negative"
        return f"Will add '{search_term}' as {match_type} negative {location}"
    elif action_name == "ADD_NEGATIVE_ADGROUP":
        location = f"to ad group '{ad_group_name}'" if ad_group_name else "as ad group negative"
        return f"Will add '{search_term}' as {match_type} negative {location}"
    else:
        location = f"to ad group '{ad_group_name}'" if ad_group_name else "as keyword"
        return f"Will promote '{search_term}' as {match_type} keyword {location}"


def build_success_message(
    search_term: str, action_name: str, match_type: str, is_campaign_level: bool,
    campaign_name: str = "", ad_group_name: str = "",
    shared_set_name: str = "", is_shared_list: bool = False
) -> str:
    """Build a success message for the audit log."""
    if action_name == "ADD_NEGATIVE_SHARED_LIST":
        location = f"to shared list '{shared_set_name}'" if shared_set_name else "to shared negative list"
        return f"Added '{search_term}' as {match_type} negative {location}"
    elif action_name == "ADD_NEGATIVE_CAMPAIGN":
        location = f"to campaign '{campaign_name}'" if campaign_name else "as campaign negative"
        return f"Added '{search_term}' as {match_type} negative {location}"
    elif action_name == "ADD_NEGATIVE_ADGROUP":
        location = f"to ad group '{ad_group_name}'" if ad_group_name else "as ad group negative"
        return f"Added '{search_term}' as {match_type} negative {location}"
    else:
        location = f"to ad group '{ad_group_name}'" if ad_group_name else "as keyword"
        return f"Promoted '{search_term}' as {match_type} keyword {location}"
