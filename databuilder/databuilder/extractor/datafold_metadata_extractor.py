# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

# Install graphql libraries with: pip install --pre gql[all]

import logging
from typing import Any
from gql import gql

from pyhocon import ConfigTree

from databuilder.extractor.base_datafold_extractor import BaseDatafoldExtractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata

LOGGER = logging.getLogger(__name__)


class DatafoldMetadataExtractor(BaseDatafoldExtractor):
    """
    An extractor for Datafold Lineage data
    """
    def init(self, conf: ConfigTree) -> None:
        BaseDatafoldExtractor.init(self, conf)
        self.metadata = self._process_metadata()
        self.iter = iter(self.metadata)

    def _process_metadata(self):
        client = self.get_graphql_client()

        # Provide a GraphQL query
        query = gql(
            """
            query {
              tables(dataSourceId:3){
                items {
                  prop {
                    path
                    dataSourceId
                  }
                  descriptions {
                    description
                  }
                  tags {
                    name
                  }
                  columns {
                    prop {
                      name
                      type
                      dbType
                      number
                    }
                    descriptions {
                      description
                    }
                    tags { 
                        name
                    }
                  }
                }
              }
            }
        """
        )

        lineage = []

        def get_amundsen_id(path):
            s = path.split('.')
            table_name = f"bigquery://{s[0]}.{s[1]}/{s[2]}"
            if len(s) > 3:
                return table_name + "/" + s[3]
            return table_name

        # Execute the query on the transport
        data = client.execute(query)

        tables = []
        for table in data['tables']['items']:
            path = self.unquote_path(table['prop']['path'])
            cols = self._parse_cols(table['columns'])
            desc = table['descriptions'][0]['description'] if len(table['descriptions']) > 0 else ''
            tables.append(
                TableMetadata(
                    database='bigquery',
                    cluster=path[0],
                    schema=path[1],
                    name=path[2],
                    description=desc,
                    columns=cols,
                    is_view=False)
            )

        return tables

    def _parse_cols(self, col_list):
        cols = []
        for col in col_list:
            prop = col['prop']
            desc = col['descriptions'][0]['description'] if len(col['descriptions']) > 0 else ''
            cols.append(
                ColumnMetadata(
                    name=prop['name'],
                    description=desc,
                    col_type=prop['dbType'],
                    sort_order=prop['number']
                )
            )
        return cols

    def extract(self) -> Any:
        try:
            return next(self.iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.datafold_metadata_extractor'
