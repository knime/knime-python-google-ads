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
from util.common import (
    GoogleAdObjectSpec,
    GoogleAdConnectionObject,
    google_ad_port_type,
)
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v16.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
from google.oauth2.credentials import Credentials

from google.ads.googleads.v16.services.services.keyword_plan_idea_service.client import (
    KeywordPlanIdeaServiceClient,
)
from google.ads.googleads.v16.services.services.geo_target_constant_service.client import (
    GeoTargetConstantServiceClient,
)
from google.ads.googleads.v16.services.types.keyword_plan_idea_service import (
    GenerateKeywordIdeasRequest,
)
from google.ads.googleads.v16.enums.types.keyword_plan_competition_level import (
    KeywordPlanCompetitionLevelEnum,
)
from google.ads.googleads.v16.enums.types.keyword_plan_network import (
    KeywordPlanNetworkEnum,
)

LOGGER = logging.getLogger(__name__)

knext.DialogCreationContext()


def retrieve_locations(self):

    client: GoogleAdsClient
    client = google_ad_port_type.object_class.client

    # Calls the Geo Target Constants service to build the request to get location IDs and names
    geo_target_constants_service: GoogleAdsServiceClient
    geo_target_constants_service = client.get_service("GeoTargetConstantService")

    geo_target_constants_request = client.get_type("SuggestGeoTargetConstantsRequest")

    results = geo_target_constants_request.suggest_geo_target_constants(
        geo_target_constants_request
    )

    geo_target_constants_id = []
    for suggestion in results.geo_target_constant_suggestions:
        geo_target_constant = suggestion.geo_target_constant
        geo_target_constants_id.append(geo_target_constant.resource_name)

    return geo_target_constants_id


def _create_location_ids_list() -> knext.StringParameter:
    return knext.StringParameter(
        label="Location Id",
        description="",
        choices=lambda c: retrieve_locations(c),
        default_value="Unselected",
        is_advanced=False,
    )


@knext.parameter_group(label="")
class LocationListInputSettings:
    location_id = _create_location_ids_list()


@knext.node(
    name="Google Ads Keyword Ideas",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/gads-icon.png",
    category=google_ads_ext.main_category,
)
@knext.input_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)
@knext.input_table(
    name="Keywords",
    description="KNIME table that contains a list of keywords or URLs to generate ideas from",
)
@knext.output_table(name="Output Data", description="KNIME table with keyword ideas")
class GoogleAdsKwdIdeas(knext.PythonNode):

    input_settings = LocationListInputSettings()

    def configure(
        self,
        configure_context: knext.DialogCreationContext,
        spec: GoogleAdObjectSpec,
        input_table_schema: knext.Schema,
    ):
        # TODO Check and throw config error maybe if spec.customer_id is not a string or does not have a specific format NOSONAR
        # We will add one column of type double to the table
        # return input_table_schema.append(knext.Column(knext.string(), "Keyword Ideas"))
        pass

    def execute(
        self,
        exec_context: knext.ExecutionContext,
        port_object: GoogleAdConnectionObject,
        input_table: knext.Table,
    ) -> knext.Table:

        pass
