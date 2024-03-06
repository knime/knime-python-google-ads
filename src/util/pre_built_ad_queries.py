import knime.extension as knext

class HardCodedQueries(knext.EnumParameterOptions):
            
    CAMPAIGNS = (
        "Campaigns",
        "The default Campaigns overview screen in the UI."
        
    )
    ADGROUPS = ( 
        "Ad Groups",
        "The default Ad groups overview screen in the UI."
        
    )
    ADS = (
        "Ads",
        "The default Ads overview screen in the UI. Note that this particular query specifically fetches the individual components of an Expanded Text Ad, which are seen rendered together in the UI screen's **Ad** column."
        
    )
    SEARCHKEYWORDS = (
        "Search Keywords",
        "The default Search keywords overview screen in the UI."
    )
    SEARCHTERMS = (
        "Search Terms",
        "The default Search terms overview screen in the UI."
        
    )
    AUDIENCE = (
        "Audiences",
        "The default Audiences overview screen in the UI. Note that the reporting API returns audiences by their criterion IDs. To get their display names, look up the IDs in the reference tables provided in the [Codes and formats page](https://developers.google.com/google-ads/api/data/codes-formats). You can key off the **ad_group_criterion.type** field to determine which criteria type table to use."
        
    )
    AGE = (
        "Age (Demographics)",
        "The default Age demographics overview screen in the UI."
        
    )
    GENDER = (
        "Gender (Demographics)",
        "The default Gender demographics overview screen in the UI."
       
    )
    LOCATION = (
        "Locations",
        "The default Locations overview screen in the UI. Note that the reporting API returns locations by their criterion IDs. To get their display names, look up the **campaign_criterion.location.geo_target_constant** in the [geo target data](https://developers.google.com/google-ads/api/data/geotargets), or use the API to query the **geo_target_constant resource**."
        
    )   

def get_query(name,start_date,end_date)->str:
    query = mapping_queries[name].replace("$$start_date$$", str(start_date)).replace("$$end_date$$", str(end_date))
    return query

mapping_queries = {
    "CAMPAIGNS":"""SELECT campaign.name,
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
    "ADGROUPS":"""     SELECT ad_group.name,
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
    "ADS" : """ SELECT ad_group_ad.ad.expanded_text_ad.headline_part1,
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
    "SEARCHTERMS" : """SELECT search_term_view.search_term,
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
    "AUDIENCE" :"""SELECT ad_group_criterion.resource_name,
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
    
    "AGE" : """SELECT ad_group_criterion.age_range.type,
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
    "GENDER" : """SELECT ad_group_criterion.gender.type,
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
    "LOCATION" : """SELECT campaign_criterion.location.geo_target_constant,
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
        """
}