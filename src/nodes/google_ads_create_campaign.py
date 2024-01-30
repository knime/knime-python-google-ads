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

import uuid
import logging
import knime.extension as knext
import google_ads_ext
import argparse
import datetime
import sys
import uuid

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v14.services.types.campaign_budget_service import (
   CampaignBudgetOperation
)
from google.ads.googleads.v14.services.services.campaign_service import (
    CampaignServiceClient
)
from google.ads.googleads.v14.services.services.campaign_budget_service import (
    CampaignBudgetServiceClient
)
from google.ads.googleads.v14.services.types.campaign_service import (
    CampaignOperation
)
from google.ads.googleads.v14.enums.types.budget_delivery_method import (
    BudgetDeliveryMethodEnum
)
from google.ads.googleads.v14.enums.types.advertising_channel_type import (
    AdvertisingChannelTypeEnum
)
from google.ads.googleads.v14.enums.types.campaign_status import (
    CampaignStatusEnum
)
from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
    )
LOGGER = logging.getLogger(__name__)


_DATE_FORMAT = "%Y%m%d"

@knext.parameter_group(label="Campaign settings")
#TODO Add as a parameter: Campaign name (flow variable), Bidding strategy, Network type
class CampaignSettings:
    class CampaignType(knext.EnumParameterOptions):
        SEARCH = ("Search", "Get in front of high-intent customers at the right time on Google Search.")
        PERFORMANCE_MAX = ("Performance Max", "Reach audiences across all of Google with a single campaign.")
        DISPLAY = ("Display", "Reach customers across 3 million sites and apps with engaging creative.")
        SHOPPING = ("Shopping","Showcase your products to shoppers as they explore what to buy")
        VIDEO = ("Video","Reach viewers on YouTube and get conversions")
        DEMAND_GEN = ("Demand Gen","Run ads on YouTube, Gmail, Discover, and more")

    campaign_type= knext.EnumParameter(
        label="Select a campaign type",
        description="Your campaign type determines the places online where customers will find your ads.",
        default_value=CampaignType.SEARCH.name,
        enum=CampaignType,
    )


@knext.node(
    name="Google Ads Campaign Creator",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/gads-icon.png",
    category=google_ads_ext.main_category,
)
@knext.input_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)

#TODO set campaign Goals and Conversion Goals???



class GoogleAdsCampaignCreator(knext.PythonNode):

    campaign_settings = CampaignSettings()

    def configure(self, configure_context: knext.ConfigurationContext, spec: GoogleAdObjectSpec):
        # TODO Check and throw config error maybe if spec.customer_id is not a string or does not have a specific format NOSONAR
        pass

    def execute(self, exec_context: knext.ExecutionContext, port_object: GoogleAdConnectionObject):

        client: GoogleAdsClient
        client = port_object.client
        customer_id = port_object.spec.customer_id
        campaign_budget_service: CampaignBudgetServiceClient
        campaign_budget_service = client.get_service("CampaignBudgetService")
        campaign_service: CampaignServiceClient
        campaign_service = client.get_service("CampaignService")

        # [START add_campaigns]
        # Create a budget, which can be shared by multiple campaigns.
        campaign_budget_operation: CampaignBudgetOperation
        campaign_budget_operation = client.get_type("CampaignBudgetOperation")
        campaign_budget = campaign_budget_operation.create
        campaign_budget.name = f"Campaign test diego {uuid.uuid4()}"
        campaign_budget.delivery_method = BudgetDeliveryMethodEnum.BudgetDeliveryMethod.STANDARD
        # campaign_budget.delivery_method = ( NOSONAR
        #     client.enums.BudgetDeliveryMethodEnum.STANDARD
        # )
        campaign_budget.amount_micros = 500000

        # Add budget. TODO: add exception rule here
        campaign_budget_response = (
            campaign_budget_service.mutate_campaign_budgets(
                customer_id=customer_id, operations=[campaign_budget_operation]
                )
             )
        # try: NOSONAR
        #     campaign_budget_response = (
        #         campaign_budget_service.mutate_campaign_budgets(
        #             customer_id=customer_id, operations=[campaign_budget_operation]
        #         )
        #     )
        # except GoogleAdsException as ex:
        #     handle_googleads_exception(ex)
        #     # [END add_campaigns]

        # [START add_campaigns_1]
        # Create campaign.
        campaign_operation: CampaignOperation
        campaign_operation = client.get_type("CampaignOperation")
        campaign = campaign_operation.create
        campaign.name = f"Interplanetary Cruise {uuid.uuid4()}"
        campaign.advertising_channel_type: AdvertisingChannelTypeEnum
        campaign.advertising_channel_type = self.campaign_settings.campaign_type
        #TODO query to fetch which campaign type are allowed within the Google Ads Account ?? NOSONAR

        # Recommendation: Set the campaign to PAUSED when creating it to prevent
        # the ads from immediately serving. Set to ENABLED once you've added
        # targeting and the ads are ready to serve.
        campaign.status: CampaignStatusEnum
        campaign.status = CampaignStatusEnum.CampaignStatus.PAUSED

        # Set the bidding strategy and budget.
        campaign.manual_cpc.enhanced_cpc_enabled = True
        campaign.campaign_budget = campaign_budget_response.results[0].resource_name

        # Set the campaign network options.
        campaign.network_settings.target_google_search = True
        campaign.network_settings.target_search_network = True
        campaign.network_settings.target_partner_search_network = False
        # Enable Display Expansion on Search campaigns. For more details see:
        # https://support.google.com/google-ads/answer/7193800
        campaign.network_settings.target_content_network = True
        # [END add_campaigns_1]

        # Optional: Set the start date.
        start_time = datetime.date.today() + datetime.timedelta(days=1)
        campaign.start_date = datetime.date.strftime(start_time, _DATE_FORMAT)

        # Optional: Set the end date.
        end_time = start_time + datetime.timedelta(weeks=4)
        campaign.end_date = datetime.date.strftime(end_time, _DATE_FORMAT)

        # Add the campaign.

        campaign_response = campaign_service.mutate_campaigns(
               customer_id=customer_id, operations=[campaign_operation]
            )
        LOGGER.warning(msg=(f"Created campaign {campaign_response.results[0].resource_name}."))

        # try: NOSONAR
        #     campaign_response = campaign_service.mutate_campaigns(
        #         customer_id=customer_id, operations=[campaign_operation]
        #     )
        #     print(f"Created campaign {campaign_response.results[0].resource_name}.")
        # except GoogleAdsException as ex:
        #     handle_googleads_exception(ex)


    # def handle_googleads_exception(exception): NOSONAR
    #     print(
    #         f'Request with ID "{exception.request_id}" failed with status '
    #         f'"{exception.error.code().name}" and includes the following errors:'
    #     )
    #     for error in exception.failure.errors:
    #         print(f'\tError with message "{error.message}".')
    #         if error.location:
    #             for field_path_element in error.location.field_path_elements:
    #                 print(f"\t\tOn field: {field_path_element.field_name}")
    #     sys.exit(1)