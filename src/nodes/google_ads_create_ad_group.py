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
import pandas as pd

from google.ads.googleads.v14.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
from google.ads.googleads.v14.services.types.google_ads_service import GoogleAdsRow
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)

LOGGER = logging.getLogger(__name__)

# def get_campaigns_id():
#      campaigns_id = ["dafafda","dalfjdosaihfpos","aldkjfsañkj"]
#      return campaigns_id


def get_campaigns_id(ctx: knext.DialogCreationContext) -> list[str]:
    LOGGER.warning("¨get_campaign_ids")
    connection_port_object_spec: GoogleAdObjectSpec
    connection_port_object_spec = ctx.get_input_specs()[
        0
    ]  # TODO verify that it is the correct spec (it should be the first object spec as we have only one (which is then also the first) port object as input - in theory)
    ids = list(map(str, connection_port_object_spec.campaign_ids))
    LOGGER.warning(ids)
    LOGGER.warning(type(ids))
    LOGGER.warning(ids[0])
    LOGGER.warning(type(ids[0]))
    return ids


#    return connection_port_object_spec.campaign_ids


@knext.node(
    name="Google Ads AdGroup Creator (Labs)",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/gads-icon.png",
    category=google_ads_ext.main_category,
)
@knext.input_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)

# @knext.input_table(
#     name="Ad Group information",
#     description="Pass the necessary parameter to create a new Ad Group under the target Google Adwords campaign"
# )
class GoogleAdsAdGroupCreator(knext.PythonNode):
    LOGGER.warning("or before generating the list")

    def get_campaigns_ids_list(self: str) -> knext.StringParameter:
        return knext.StringParameter(
            label="Campaign Id",
            description="blablba",
            choices=lambda c: get_campaigns_id(c),
            default_value="unselected",
            is_advanced=False,
        )

    LOGGER.warning("is here the error??")

    campaign_ids = get_campaigns_ids_list("Campaigns")

    def configure(
        self, configure_context: knext.ConfigurationContext, spec: GoogleAdObjectSpec
    ):
        # TODO Check and throw config error maybe if spec.customer_id is not a string or does not have a specific format
        pass

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
    ):
        client: GoogleAdsClient
        client = port_object.client
        customer_id = port_object.spec.customer_id

        # ga_service: GoogleAdsServiceClient --> Not needed?
        # ga_service = client.get_service("GoogleAdsService") --> Not needed?

        ad_group_service = client.get_service("AdGroupService")
        campaign_service = client.get_service("CampaignService")
        campaign_id = 0  # Hardcoded # TODO changed to 0 for anonymization

        # Create ad group.
        ad_group_operation = client.get_type("AdGroupOperation")
        ad_group = ad_group_operation.create  # should it really not be 'create()'?
        ad_group.name = f"Test AdGroup Creator node {uuid.uuid4()}"
        ad_group.status = client.enums.AdGroupStatusEnum.ENABLED
        ad_group.campaign = campaign_service.campaign_path(customer_id, campaign_id)
        ad_group.type_ = client.enums.AdGroupTypeEnum.SEARCH_STANDARD
        ad_group.cpc_bid_micros = 10000000
        LOGGER.warning("ad_group details:")
        LOGGER.warning(ad_group)
        LOGGER.warning(ad_group.name)
        LOGGER.warning(ad_group.status)
        LOGGER.warning(ad_group.campaign)
        LOGGER.warning(ad_group.type_)
        LOGGER.warning(ad_group.cpc_bid_micros)
        # Add the ad group.
        ad_group_response = ad_group_service.mutate_ad_groups(
            customer_id=customer_id, operations=[ad_group_operation]
        )
        LOGGER.warning(
            f"Created ad group {ad_group_response.results[0].resource_name}."
        )
