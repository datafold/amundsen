# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

# Install graphql libraries with: pip install --pre gql[all]

import logging
from typing import Any, Optional, Tuple
from gql import Client
from gql.transport.requests import RequestsHTTPTransport

from pyhocon import ConfigTree

from databuilder.extractor.base_extractor import Extractor

LOGGER = logging.getLogger(__name__)


class _Buffer:
    def __init__(self, s: str):
        self.s = s
        self.i = 0

    def peek(self) -> Optional[str]:
        if self.i < len(self.s):
            return self.s[self.i]
        return None

    def pop(self) -> Optional[str]:
        c = self.peek()
        self.i += 1
        return c

    def __len__(self):
        return max(0, len(self.s) - self.i)


class BaseDatafoldExtractor(Extractor):
    """
    An extractor for Datafold Lineage data
    """
    HOST_KEY = 'host'
    API_KEY_KEY = 'api_key'

    def init(self, conf: ConfigTree) -> None:
        Extractor.init(self, conf)
        self.host = conf.get_string(BaseDatafoldExtractor.HOST_KEY, None)
        self.api_key = conf.get_string(BaseDatafoldExtractor.API_KEY_KEY, None)

        if self.host is None or self.api_key is None:
            raise Exception("Both host and api_key must be configured")

    def get_graphql_client(self):
        # Select your transport with a defined url endpoint
        headers = {"Authorization": "Key " + self.api_key,
                   "Content-type": "application/json"}
        transport = RequestsHTTPTransport(url=self.host,
                                          use_json=True,
                                          headers=headers,
                                          verify=False,
                                          retries=3)

        return Client(transport=transport, fetch_schema_from_transport=True)

    def extract(self) -> Any:
        try:
            return next(self.iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.datafold_lineage_extractor'

    def unquote_path(self, p: str) -> Tuple[str, ...]:
        buf = _Buffer(p)

        quote = '"'
        token = []
        tokens = []

        while True:
            c = buf.pop()
            # print(c)
            if c == quote:
                while True:
                    c = buf.pop()
                    # print('q:', c)
                    if c == quote:
                        if buf.peek() == quote:
                            buf.pop()
                            # print('q+:', c)
                            token.append(quote)
                        else:
                            # print('q close')
                            break
                    elif c is None:
                        raise ValueError('Unclosed quote in `{}` at char {}'
                                         .format(p, buf.i))
                    else:
                        # print('q+:', c)
                        token.append(c)
            elif c == '.' or c is None:
                # print('eof token', token)
                if not token:
                    raise ValueError('Empty token in `{}` at char {}'
                                     .format(p, buf.i))
                else:
                    tokens.append(''.join(token))
                    token = []
                if c is None:
                    break
            else:
                token.append(c)

        return tuple(tokens)
