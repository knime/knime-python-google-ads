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


import knime.extension as knext
import logging
from knime.extension.nodes import ConnectionPortObject

LOGGER = logging.getLogger(__name__)


class GoogleAdObjectSpec(knext.PortObjectSpec):
    def __init__(self, customer_id: str) -> None:
        super().__init__()
        self._customer_id = customer_id

    @property
    def customer_id(self) -> str:
        return self._customer_id

    def serialize(self) -> dict:
        return {"customer_id": self._customer_id}

    @classmethod
    def deserialize(cls, data: dict) -> "GoogleAdObjectSpec":
        return cls(data["customer_id"])


#
class GoogleAdConnectionObject(ConnectionPortObject):
    def __init__(
        self,
        spec: GoogleAdObjectSpec,
        client: "client",
    ) -> None:
        super().__init__(spec)
        self._client = client

    @property
    def spec(self) -> GoogleAdObjectSpec:
        return super().spec

    @property
    def client(self):
        return self._client

    def to_connection_data(self):
        """
        Provide the data that makes up this ConnectionPortObject such that it can be used
        by downstream nodes in the ``from_connection_data`` method.
        """
        return {
            "client": self._client,
        }

    @classmethod
    def from_connection_data(
        cls, spec: knext.PortObjectSpec, data
    ) -> "ConnectionPortObject":
        """
        Construct a ConnectionPortObject from spec and data. The data is the data that has
        been returned by the ``to_connection_data`` method of the ConnectionPortObject
        by the upstream node.

        The data should not be tempered with, as it is a Python object that is handed to
        all nodes using this ConnectionPortObject.
        """

        return cls(spec, data["client"])


google_ad_port_type = knext.port_type(
    "Google Ad Port Type", GoogleAdConnectionObject, GoogleAdObjectSpec
)
