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
from google.ads.googleads.v14.services.services.google_ads_service.client import (
    GoogleAdsServiceClient,
)
from google.ads.googleads.v14.services.services.keyword_plan_idea_service.client import (
    KeywordPlanIdeaServiceClient,
)
from google.ads.googleads.v14.services.services.geo_target_constant_service.client import (
    GeoTargetConstantServiceClient,
)
from google.ads.googleads.v14.services.types.keyword_plan_idea_service import (
    GenerateKeywordIdeasRequest,
)
from google.ads.googleads.v14.enums.types.keyword_plan_competition_level import (
    KeywordPlanCompetitionLevelEnum,
)
from google.ads.googleads.v14.enums.types.keyword_plan_network import (
    KeywordPlanNetworkEnum,
)

LOGGER = logging.getLogger(__name__)


@knext.parameter_group(label="Select target column, the language and the location")
class MySettings:
    location_id = knext.StringParameter(
        label="Location ID",
        description="Input your location ID",
        default_value="1023191",
        is_advanced=False,
    )

    language_id = knext.StringParameter(
        label="Language Id",
        description="Input the Language ID",
        default_value="1000",
        is_advanced=False,
    )

    selected_column = knext.ColumnParameter(
        "Keywords Column",
        "KNIME table column containing the kewyords from which fetch the ideas with the search volume",
        port_index=1,
        include_row_key=False,
        include_none_column=False,
        since_version=None,
    )


@knext.node(
    name="Google Ads Keyword Ideas",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/google_ads_keyword_ideas_logo.png",
    category=google_ads_ext.main_category,
)
@knext.input_port(
    "Google Client",
    "Contains necessary tokens and credentials as well as customer ID to access the Google Ads API",
    google_ad_port_type,
)
@knext.input_table(
    name="Keywords",
    description="KNIME table that contains a list of keywords to generate ideas from",
)
@knext.output_table(name="Output Data", description="KNIME table with keyword ideas")
class GoogleAdsKwdIdeas(knext.PythonNode):

    my_settings = MySettings()

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
        # TODO make optional the page url provider to generate ideas also from there! NOSONAR

        location_ids = [self.my_settings.location_id]
        selected_column = self.my_settings.selected_column
        if self.my_settings.selected_column != "<none>":
            keyword_texts_df = input_table.to_pandas()
        else:
            exec_context.set_warning("No column selected")

        keyword_texts = keyword_texts_df[selected_column].tolist()

        LOGGER.warning(msg="check the keyword_texts object")
        LOGGER.warning(type(keyword_texts))
        # TODO pass a KNIME table and transform here in a list, select also the column!! keyword_texts = keyword_processing.values.tolist() NOSONAR

        client: GoogleAdsClient
        client = port_object.client
        customer_id = port_object.spec.customer_id

        keyword_plan_idea_service: KeywordPlanIdeaServiceClient
        keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")

        # TODO check this part and also the keyword_plan_network NOSONAR
        # kwd_plan_competition: KeywordPlanCompetitionLevelEnum
        # kwd_plan_competition = client.get_type("KeywordPlanCompetitionLevelEnum")
        # keyword_competition_level_enum.status = (client.enums.kwd_plan_competition)

        keyword_plan_network = 3
        # (
        #     client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS
        # )
        # client: GoogleAdsClient NOSONAR
        # client = port_object.client
        location_rns = map_locations_ids_to_resource_names(self, client, location_ids)
        language_rn_get_service: GoogleAdsServiceClient
        language_rn_get_service = client.get_service("GoogleAdsService")
        language_rn = language_rn_get_service.language_constant_path(
            self.my_settings.language_id
        )

        # Either keywords or a page_url are required to generate keyword ideas
        # so this raises an error if neither are provided.
        if not keyword_texts:  # TODO or page_url): NOSONAR
            raise ValueError(
                "At least one of keywords"  # or page URL is required, " NOSONAR
                "but neither was specified."
            )

        # Only one of the fields "url_seed", "keyword_seed", or
        # "keyword_and_url_seed" can be set on the request, depending on whether
        # keywords, a page_url or both were passed to this function.
        request: GenerateKeywordIdeasRequest
        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = customer_id
        request.language = language_rn
        request.geo_target_constants = location_rns
        request.include_adult_keywords = False
        request.keyword_plan_network = keyword_plan_network

        # TODO admit a website URL as input NOSONAR
        # To generate keyword ideas with only a page_url and no keywords we need
        # to initialize a UrlSeed object with the page_url as the "url" field.
        # if not keyword_texts and page_url:
        #     request.url_seed.url = page_url

        # To generate keyword ideas with only a list of keywords and no page_url
        # we need to initialize a KeywordSeed object and set the "keywords" field
        # to be a list of StringValue objects.
        # if not keyword_texts.empty: #and not page_url:
        request.keyword_seed.keywords.extend(keyword_texts)

        # To generate keyword ideas using both a list of keywords and a page_url we
        # need to initialize a KeywordAndUrlSeed object, setting both the "url" and
        # "keywords" fields.
        # if keyword_texts and page_url:
        #     request.keyword_and_url_seed.url = page_url
        #     request.keyword_and_url_seed.keywords.extend(keyword_texts)

        keyword_ideas = keyword_plan_idea_service.generate_keyword_ideas(
            request=request
        )
        LOGGER.warning("let'see what it is")
        LOGGER.warning(type(keyword_ideas))
        # for idea in keyword_ideas: NOSONAR
        #     competition_value = idea.keyword_idea_metrics.competition.name
        #     print(
        #         f'Keyword idea text "{idea.text}" has '
        #         f'"{idea.keyword_idea_metrics.avg_monthly_searches}" '
        #         f'average monthly searches and "{competition_value}" '
        #         "competition.\n"
        #     )

        # Create empty lists to store data
        keywords = []
        avg_monthly_searches = []
        competition_values = []

        # Extract data and populate lists
        for idea in keyword_ideas:
            # keywords.append(idea['text']) NOSONAR
            # avg_monthly_searches.append(idea['keyword_idea_metrics']['avg_monthly_searches'])
            # competition_values.append(idea['keyword_idea_metrics']['competition']['name'])
            keywords.append(idea.text)
            avg_monthly_searches.append(idea.keyword_idea_metrics.avg_monthly_searches)
            competition_values.append(idea.keyword_idea_metrics.competition.name)
        # Create a DataFrame from the lists
        data = {
            "Keyword": keywords,
            "Avg_Monthly_Searches": avg_monthly_searches,
            "Competition": competition_values,
        }
        LOGGER.warning(msg="check the data df")
        df = pd.DataFrame(data)
        LOGGER.warning(type(df))
        # Display the DataFrame
        return knext.Table.from_pandas(df)

        # [END generate_keyword_ideas]


def map_locations_ids_to_resource_names(self, port_object, location_ids):

    client: GoogleAdsClient
    client = port_object

    build_resource_name_client: GeoTargetConstantServiceClient
    build_resource_name_client = client.get_service("GeoTargetConstantService")
    build_resource_name = build_resource_name_client.geo_target_constant_path
    return [build_resource_name(location_id) for location_id in location_ids]
