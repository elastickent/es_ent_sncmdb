#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
ServiceNow CMDB connector for Elastic Enterprise Search.

"""

import requests
import json
import asyncio
from connectors.logger import logger
from connectors.source import BaseDataSource


sn_headers = {"Accept": "application/json"}

sn_params = {'sysparm_limit': '10000',
             'sysparm_display_value': 'true',
             'sysparm_exclude_reference_link': 'true', }


class SncmdbDataSource(BaseDataSource):
    """ServiceNow CMDB Connector"""

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

    @classmethod
    def get_default_configuration(cls):
        return {
            "domain": {
                "value": "dev138640.service-now.com",
                "label": "ServiceNow Domain",
                "type": "str",
            },
            "user": {
                "value": "admin",
                "label": "User",
                "type": "str",
            },
            "password": {
                "value": "CHANGME",
                "label": "Password",
                "type": "str",
            },
            "sn_items": {
                "value": "cmdb_ci_hpux_server,cmdb_ci_computer",
                "label": "Comma separated list of ServiceNow tables",
                "type": "list",
            }
        }
    
    async def ping(self):
        cfg = self.configuration
        url = 'https://%s/api/now/table/%s' % (cfg["domain"], 
                                               cfg["sn_items"][0])
        try:
            resp = requests.get(url, params=sn_params,
                                auth=(cfg["user"], cfg["password"]),
                                headers=sn_headers, stream=True)
        except Exception:
            logger.exception("Error while connecting to the ServiceNow.")
        if resp.status_code != 200:
            logger.exception("Error while connecting to the ServiceNow.")
            raise NotImplementedError
        return True

    def _clean_empty(self, data):
        if data is None:
            return None
        res_data = [{item: value for item, value in row.items() if value} 
                    for row in data]
        return res_data

    async def get_docs(self, filtering=None):
        cfg = self.configuration
        sysparm_offset = 0
        sysparm_limit = 1000
        while True:
            for sn_table in cfg['sn_items']:
                sn_params = {
                    'sysparm_limit': sysparm_limit,
                    'sysparm_offset': sysparm_offset,
                    'sysparm_display_value': 'true',
                    'sysparm_exclude_reference_link': 'true',
                }
                url = f'https://{cfg["domain"]}/api/now/table/{sn_table}'
                resp = requests.get(url, params=sn_params, auth=(cfg["user"], 
                                    cfg["password"]), headers=sn_headers, 
                                    stream=True)
                
                if resp.status_code != 200:
                    logger.warning('Status:', resp.status_code, 'Headers:', 
                                   resp.headers, 'Error Response:', 
                                   resp.json())
                    raise NotImplementedError
            
                data = resp.json()
                
                if data is not None:
                    table = self._clean_empty(data['result'])
                    for row in table:
                        row['_id'] = row['sys_id']
                        row['@timestamp'] = row['sys_updated_on']
                        lazy_download = None
                        doc = row, lazy_download
                        yield doc
            
            # Update the offset for the next page
            sysparm_offset += sysparm_limit
            if len(data['result']) < sysparm_limit:
                break

