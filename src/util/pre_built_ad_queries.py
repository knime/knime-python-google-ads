prebuilt_query_campaigns = """
                    SELECT campaign.name,
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
                    """
prebuilt_query_adgroups ="""
                    SELECT ad_group.name,
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
                    """
prebuilt_query_ads = """
                SELECT ad_group_ad.ad.expanded_text_ad.headline_part1,
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
                    """
prebuilt_query_search_keywords = """
                SELECT ad_group_criterion.keyword.text,
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
                """
prebuilt_query_search_terms = """
                SELECT search_term_view.search_term,
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
                """
prebuilt_query_audience = """
                SELECT ad_group_criterion.resource_name,
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
                """
prebuilt_query_age ="""
                SELECT ad_group_criterion.age_range.type,
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
                """
prebuilt_query_gender = """
            SELECT ad_group_criterion.gender.type,
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
                """
prebuilt_query_location = """
                    SELECT campaign_criterion.location.geo_target_constant,
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