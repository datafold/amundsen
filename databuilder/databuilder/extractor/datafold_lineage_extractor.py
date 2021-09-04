# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

# Install graphql libraries with: pip install --pre gql[all]

import logging
from typing import Any, Tuple
from gql import gql

from pyhocon import ConfigTree

from databuilder.extractor.base_datafold_extractor import BaseDatafoldExtractor
from databuilder.models.table_lineage import TableLineage, ColumnLineage

LOGGER = logging.getLogger(__name__)


class DatafoldLineageExtractor(BaseDatafoldExtractor):
    """
    An extractor for Datafold Lineage data
    """
    def init(self, conf: ConfigTree) -> None:
        BaseDatafoldExtractor.init(self, conf)
        self.lineage_data = self._process_lineage()
        self.iter = iter(self.lineage_data)

    def _process_lineage(self):
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
                  columns {
                    prop {
                      name
                    }
                    downstream {
                      prop {
                        name
                      }
                      table {
                        prop {
                          path
                        }
                      }
                    }
                  }
                }
              }
            }
        """
        )

        lineage = []

        def get_amundsen_id(path: Tuple[str]):
            return f"bigquery://{path[0]}.{path[1]}/{path[2]}"

        # Execute the query on the transport
        data = client.execute(query)
        print(data)

        for table in data['tables']['items']:
            am_table_key = get_amundsen_id(self.unquote_path(table['prop']['path']))
            downstream_tables = set()

            for col in table['columns']:
                if len(col['downstream']) == 0:
                    continue

                downstream_columns = []
                column_key = f"{am_table_key}/{col['prop']['name']}"
                for ds_col in col['downstream']:
                    ds_table_key = get_amundsen_id(
                        self.unquote_path(ds_col['table']['prop']['path']))
                    downstream_tables.add(ds_table_key)

                    downstream_key = f"{ds_table_key}/{ds_col['prop']['name']}"
                    downstream_columns.append(downstream_key)

                lineage.append(
                    ColumnLineage(
                        column_key=column_key,
                        downstream_deps=downstream_columns)
                )

            lineage.append(
                TableLineage(table_key=am_table_key,
                             downstream_deps=list(downstream_tables))
            )

        return lineage

    def extract(self) -> Any:
        try:
            return next(self.iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.datafold_lineage_extractor'
