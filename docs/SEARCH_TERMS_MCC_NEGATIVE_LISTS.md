# Shared Negative Keyword Lists - Implementation Notes

## Overview

The **Google Ads Search Terms Actions** node supports adding negative keywords to shared negative keyword lists (also known as MCC-level negative lists). This allows users to maintain centralized negative keyword lists that can be linked to multiple campaigns.

## Scope Options

The node supports three negative keyword scopes:

| Scope | Description | Target Resource |
|-------|-------------|-----------------|
| **Campaign level** | Blocks across all ad groups in campaign | `CampaignCriterion` |
| **Ad Group level** | Blocks only in that ad group | `AdGroupCriterion` |
| **Shared Negative List** | Adds to a reusable list | `SharedCriterion` |

## Shared Negative List Workflow

### Step 1: Query Available Shared Sets

Use the **Google Ads Query** node with a custom GAQL query to retrieve available negative keyword lists:

```sql
SELECT
    shared_set.id,
    shared_set.name,
    shared_set.resource_name,
    shared_set.member_count,
    shared_set.status
FROM shared_set
WHERE shared_set.type = 'NEGATIVE_KEYWORDS'
    AND shared_set.status = 'ENABLED'
```

The `shared_set.resource_name` column (format: `customers/{customer_id}/sharedSets/{shared_set_id}`) is required for the Search Terms Actions node.

### Step 2: Prepare Your Data

Join or combine your search terms data with the shared set information so each row has:
- **Search term text** - the keyword to add as negative
- **Shared set resource name** - the target list (`customers/123/sharedSets/456`)

### Step 3: Configure Search Terms Actions Node

1. Set **Action Type** to "Add Negative Keyword"
2. Set **Scope** to "Shared Negative List"
3. Select the column containing `shared_set.resource_name`
4. Choose the match type (Exact, Phrase, or Broad)

## API Details

### Resource: SharedCriterion

Adding a keyword to a shared list creates a `SharedCriterion` resource:

- **Service**: `SharedCriterionService.mutate_shared_criteria()`
- **Operation**: Create with `shared_set` reference and keyword info
- **Resource name format**: `customers/{customer_id}/sharedCriteria/{shared_set_id}~{criterion_id}`

### Duplicate Detection

The node checks if the exact term+match_type already exists at the target level before adding.

### Conflict Detection (All Scopes)

The node performs comprehensive conflict detection across all levels for each scope:

#### Campaign-Level Negatives
| Check Level | Status | Description |
|-------------|--------|-------------|
| **Campaign** | `ALREADY_EXISTS` | Negative already exists at campaign level |
| **Campaign** | `CONFLICT_CAMPAIGN_POSITIVE` | Positive keyword exists at campaign level |
| **Shared List** | `CONFLICT_SHAREDLIST_NEGATIVE` | Negative exists in a shared list linked to campaign |
| **Ad Group** | `CONFLICT_ADGROUP_POSITIVE` | Positive keyword exists at ad group level |
| **Ad Group** | `CONFLICT_ADGROUP_NEGATIVE` | Negative exists at ad group level |

#### Ad Group-Level Negatives
| Check Level | Status | Description |
|-------------|--------|-------------|
| **Ad Group** | `ALREADY_EXISTS` | Negative already exists at ad group level |
| **Ad Group** | `CONFLICT_ADGROUP_POSITIVE` | Positive keyword exists in same ad group |
| **Campaign** | `CONFLICT_CAMPAIGN_POSITIVE` | Positive keyword exists at campaign level |
| **Campaign** | `CONFLICT_CAMPAIGN_NEGATIVE` | Negative exists at campaign level |
| **Shared List** | `CONFLICT_SHAREDLIST_NEGATIVE` | Negative exists in shared list linked to campaign |

#### Keyword Promotion (Positive Keywords)
| Check Level | Status | Description |
|-------------|--------|-------------|
| **Ad Group** | `ALREADY_EXISTS` | Keyword already exists in ad group |
| **Ad Group** | `CONFLICT_ADGROUP_NEGATIVE` | Negative exists in same ad group |
| **Campaign** | `CONFLICT_CAMPAIGN_POSITIVE` | Positive keyword exists at campaign level |
| **Campaign** | `CONFLICT_CAMPAIGN_NEGATIVE` | Negative exists at campaign level |
| **Shared List** | `CONFLICT_SHAREDLIST_NEGATIVE` | Negative exists in shared list linked to campaign |

#### Shared Negative List (Account and MCC Level)

**Note:** For shared negative lists, only **duplicate detection** within the target list is performed. 
Conflict detection with positive keywords at campaign/ad group level is **not** performed. 
This is by design: adding keywords to a shared list is a deliberate account-wide decision. 
Ensure upstream that terms do not conflict with active positive keywords in linked campaigns.

| Check Level | Status | Description |
|-------------|--------|-------------|
| **Shared List** | `ALREADY_EXISTS` | Negative already exists in shared list |

This detection runs in both **Preview** and **Apply** modes.

## Example GAQL Queries

### List Keywords in a Shared Set

```sql
SELECT
    shared_criterion.keyword.text,
    shared_criterion.keyword.match_type,
    shared_criterion.resource_name,
    shared_set.name
FROM shared_criterion
WHERE shared_set.id = {shared_set_id}
    AND shared_criterion.type = 'KEYWORD'
```

### Which Campaigns Use a Shared Set

```sql
SELECT
    campaign.id,
    campaign.name,
    shared_set.name,
    campaign_shared_set.status
FROM campaign_shared_set
WHERE shared_set.id = {shared_set_id}
    AND campaign_shared_set.status = 'ENABLED'
```

## MCC Considerations

When connected to an MCC (Manager account):
- Shared sets created at MCC level can be linked to child account campaigns
- Query using MCC credentials to see all shared sets
- The `shared_set.resource_name` will contain the MCC's customer ID
