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
import knime.extension as knext
import google_ads_ext
import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)
from google.ads.googleads.v15.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
from google.ads.googleads.errors import GoogleAdsException

from google.ads.googleads.v15.services.types.google_ads_service import GoogleAdsRow
from google.protobuf.internal.containers import RepeatedScalarFieldContainer
from google.protobuf.pyext import _message


LOGGER = logging.getLogger(__name__)


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


class QueryBuilderMode(knext.EnumParameterOptions):
    PREBUILT = (
        "Pre-built",
        "These pre-built queries provide a set of queries in Google Ads Query Language that return the same data as the screens in the [Google Ads UI](https://developers.google.com/google-ads/api/docs/query/cookbook).",
    )
    MANUALLY = (
        "Custom",
        "Build your query using [Google Ads Query Builder](https://developers.google.com/google-ads/api/fields/v12/overview_query_builder), then validate it with [Google Ads Query Validator](https://developers.google.com/google-ads/api/fields/v12/query_validator) for desired results.",
    )



@knext.node(
    name="Google Ads Query",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/google_ads_query_logo.png",
    category=google_ads_ext.main_category,
)
@knext.input_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)
@knext.output_table(name="Output Data", description="KNIME table with query results")
class GoogleAdsQuery:
    """Fetch data from Google Adwords for a given query.

    Long description of the node.
    Can be multiple lines.
    """

    query_mode = knext.EnumParameter(
        "Query mode",
        "You can choose from **pre-built** queries or create a **custom** one from scratch.",
        
        QueryBuilderMode.PREBUILT.name,
        QueryBuilderMode,
        style=knext.EnumParameter.Style.VALUE_SWITCH,
    )
    
    query_custom = knext.MultilineStringParameter(
        label="Custom query:",
        description="Input your query below, replacing the default query.",
        default_value="""SELECT
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
        FROM campaign""",
        number_of_lines = 10,
    ).rule(knext.OneOf(query_mode, [QueryBuilderMode.MANUALLY.name]),knext.Effect.SHOW,)

    query_prebuilt_type = knext.EnumParameter(
        label="Pre-built queries:",
        description="Select an available pre-built query to be used.",
        default_value= HardCodedQueries.CAMPAIGNS.name,
        enum= HardCodedQueries,
        ).rule(knext.OneOf(query_mode, [QueryBuilderMode.PREBUILT.name]),knext.Effect.SHOW,)
    

    #TODO move the pre-built queries to a separte file in the util folder pre-built_ad_queries.py
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
                        WHERE segments.date DURING LAST_7_DAYS
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
                        WHERE segments.date DURING LAST_7_DAYS
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
                        WHERE segments.date DURING LAST_7_DAYS
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
                    WHERE segments.date DURING LAST_7_DAYS
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
                    WHERE segments.date DURING LAST_7_DAYS
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
                    WHERE segments.date DURING LAST_7_DAYS
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
                    WHERE segments.date DURING LAST_7_DAYS
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
                WHERE segments.date DURING LAST_7_DAYS                  
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
                WHERE segments.date DURING LAST_7_DAYS
                AND campaign_criterion.status != 'REMOVED'
                    """

    def configure(self, configuration_context, spec: GoogleAdObjectSpec):
        # TODO Check and throw config error maybe if spec.customer_id is not a string or does not have a specific format
        if hasattr(spec, "customer_id") == False:
            raise knext.InvalidParametersError("Connect to the Google Ads Connector node.")
        pass #Which configuration I need to pass?? explain better the configure method, not 100% clear. 

    def execute(self, exec_context: knext.ExecutionContext, port_object: GoogleAdConnectionObject):
        
        #counter to build the progress bar during execution
        i=0
        client: GoogleAdsClient
        client = port_object.client
        customer_id = port_object.spec.customer_id
        

        ####################
        # [START QUERY TEST]
        ####################
        # TODO Implement config window with a query builder
        execution_query = self.define_query()
        
        #TODO move the pre-built queries to a separte file in the util folder pre-built_ad_queries.py
        DEFAULT_QUERY = """
        SELECT
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
        FROM campaign"""
        
        if execution_query == "":
            exec_context.set_warning("Used default query because you didn't provide one.")
            execution_query = DEFAULT_QUERY
      
        ga_service: GoogleAdsServiceClient
        ga_service = client.get_service("GoogleAdsService")

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = customer_id
        search_request.query = execution_query
        

        df = pd.DataFrame()
        #TODO add configuration for timeout (find default timeout and use it.)
        try:
            response_stream = ga_service.search_stream(search_request)
            data = []
            header_array = []
          
            for batch in response_stream:
                
                header_array = [field for field in batch.field_mask.paths]
                number_of_results = len([result for result in batch.results])                           
               
                for row in batch.results:
                    data_row = []
                    row: GoogleAdsRow
                    for field in batch.field_mask.paths:
                        
                        # Split the attribute_name string into parts
                        attribute_parts = field.split(".")
                        
                        # Initialize the object to start the traversal
                        attribute_value = row
                       
                        # Traverse the attribute parts and access the attributes
                        for part in attribute_parts:
                            
                            #query-fix for ADGROUP and AD queries: we are iterating over the attribute_value (type = class) line
                            #and using the field name splitted to access the values with the getattr(method), 
                            #when trying to use 'type' there is not any attr called like this
                            #in the class attribute_value, so adding and underscore fix this.
                            #temp fix: we don't know how to check before the attr name of the class attribute value
                            if part=="type":
                                part= part+"_"
                           
                            attribute_value = getattr(attribute_value, part)
                            
                            #query-fix for AD query. Explanation for the below if: when fetching the field "final_urls" from the response_stream, it returned a [] type that was not in any Python readable type.
                            #indeed the type was this protobuf RepeatedScalarFieldContainer. The goal of the if clause is to convert the empty list to empty strings and extract the RepeatedScalarFieldContainer( similar to list type) element
                            #for reference https://googleapis.dev/python/protobuf/latest/google/protobuf/internal/containers.html
                            if type(attribute_value) is _message.RepeatedScalarContainer:
                                attribute_value : RepeatedScalarFieldContainer
                                if len(attribute_value) == 0:
                                    attribute_value = ""
                                else:
                                    attribute_value = attribute_value.pop(0)         
                        data_row.append(attribute_value)
                    data.append(data_row)
                    i += 1
                    exec_context.set_progress(i/number_of_results,"We are preparing your data \U0001F468\u200D\U0001F373")
            df = pd.DataFrame(data, columns=header_array)
            
        except GoogleAdsException as ex:
            LOGGER.warning(
                "Google Ads API request failed. Please check your query and credentials."
            )
            LOGGER.warning(ex.error)
        ##################
        # [END QUERY TEST]
        ##################

        return knext.Table.from_pandas(pd.DataFrame(df))

    def define_query(self):
        query = ""
        if self.query_mode == "MANUALLY":
            query = self.query_custom
        elif self.query_mode =="PREBUILT" and self.query_prebuilt_type =="CAMPAIGNS":
            query = self.prebuilt_query_campaigns
        elif self.query_mode =="PREBUILT" and self.query_prebuilt_type =="ADGROUPS":
            query = self.prebuilt_query_adgroups
        elif self.query_mode =="PREBUILT" and self.query_prebuilt_type =="ADS":
            query = self.prebuilt_query_ads
        elif self.query_mode =="PREBUILT" and self.query_prebuilt_type =="SEARCHKEYWORDS":
            query = self.prebuilt_query_search_keywords
        elif self.query_mode =="PREBUILT" and self.query_prebuilt_type =="SEARCHTERMS":
            query = self.prebuilt_query_search_terms
        elif self.query_mode =="PREBUILT" and self.query_prebuilt_type =="AUDIENCE":
            query = self.prebuilt_query_audience
        elif self.query_mode =="PREBUILT" and self.query_prebuilt_type =="AGE":
            query = self.prebuilt_query_age
        elif self.query_mode =="PREBUILT" and self.query_prebuilt_type =="GENDER":
            query = self.prebuilt_query_gender
        elif self.query_mode =="PREBUILT" and self.query_prebuilt_type =="LOCATION":
            query = self.prebuilt_query_location
        return query
    
 

   
