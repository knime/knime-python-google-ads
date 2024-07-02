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
from typing import Callable, List
from abc import ABC, abstractmethod
import re
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

LOGGER = logging.getLogger(__name__)


def check_canceled(exec_context: knext.ExecutionContext) -> None:
    """
    Checks if the user has canceled the execution and if so throws a RuntimeException
    """
    if exec_context.is_canceled():
        raise RuntimeError("Execution canceled")


def check_column(
    input_table: knext.Schema,
    column_name: str,
    expected_type: knext.KnimeType,
    column_purpose: str,
) -> None:
    """
    Raises an InvalidParametersError if a column named column_name is not contained in input_table or has the wrong KnimeType.
    """
    if column_name not in input_table.column_names:
        raise knext.InvalidParametersError(
            f"The {column_purpose} column '{column_name}' is missing in the input table."
        )
    ktype = input_table[column_name].ktype
    if ktype != expected_type:
        raise knext.InvalidParametersError(
            f"The {column_purpose} column '{column_name}' is of type {str(ktype)} but should be of type {str(expected_type)}."
        )


def pick_default_column(input_table: knext.Schema, ktype: knext.KnimeType) -> str:
    default_column = pick_default_columns(input_table, ktype, 1)[0]
    return default_column


def pick_default_columns(
    input_table: knext.Schema, ktype: knext.KnimeType, n_columns: int
) -> List[str]:
    columns = [c for c in input_table if c.ktype == ktype]

    if len(columns) < n_columns:
        raise knext.InvalidParametersError(
            f"The input table does not contain enough ({n_columns}) distinct columns of type '{str(ktype)}'. Found: {len(columns)}"
        )
    return [column_name.name for column_name in columns[:n_columns]]


def create_type_filer(ktype: knext.KnimeType) -> Callable[[knext.Column], bool]:
    return lambda c: c.ktype == ktype
