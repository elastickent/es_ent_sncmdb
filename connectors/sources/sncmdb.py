#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
ServiceNow CMDB connector for Elastic Enterprise Search.

"""

import requests
import os
import json
import asyncio
from connectors.logger import logger
from connectors.source import BaseDataSource
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

sn_headers = {"Accept": "application/json"}

sn_params = {'sysparm_limit': '10000',
             'sysparm_display_value': 'true',
             'sysparm_exclude_reference_link': 'true', }


class SncmdbDataSource(BaseDataSource):
    """ServiceNow CMDB Connector"""

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

    def _one_year_ago():
        # Takes in account leap years
        one_year_ago_date = datetime.now() - relativedelta(years=1)
        return one_year_ago_date.strftime('%Y-%m-%d %H:%M:%S')
    
    def _validate_date(date_string):
        try:
            datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD HH:MM:SS")

    @classmethod
    def get_default_configuration(cls):
        one_year_ago_date = datetime.now() - relativedelta(years=1)
        def_start_date = one_year_ago_date.strftime('%Y-%m-%d %H:%M:%S')
        return {
            "domain": {
                "order": 1,
                "value": "dev138640.service-now.com",
                "label": "ServiceNow Domain",
                "type": "str"
            },
            "user": {
                "order": 2,
                "value": "admin",
                "label": "User",
                "type": "str"
            },
            "password": {
                "order": 3,
                "label": "Password",
                "type": "str",
                "sensitive": True,
                "value": ""
            },
            "sn_items": {
                "order": 4,
                "value": "cmdb_ci_hpux_server",
                "label": "Comma separated list of ServiceNow tables",
                "type": "list"
            },
             "start_date": {
                "order": 5,
                "default_value": def_start_date,
                "value": def_start_date,
                "label": "Start Date (defaults to 1 year ago)",
                "tooltip": "format: YYYY-MM-DD HH:MM:SS, e.g. 2023-06-21 15:45:30",
                "type": "str",
                "required": False
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

    def _string_to_datetime(self, date_string):
        return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')

    async def get_docs(self, filtering=None):
        cfg = self.configuration
        sysparm_offset = 0
        sysparm_limit = 1000
        # Read the latest sys_updated_on value if its set from the last sync.
        state_file = './last_sys_update_on'
        running_sys_updated_on = ""
        if os.path.isfile(state_file):
            with open(state_file, 'r') as file:
                max_sys_updated_on = file.read().strip()
                logger.info(f'found state file:{state_file} with date {max_sys_updated_on}')
        else:
            max_sys_updated_on = cfg['start_date']
        while True:
            for sn_table in cfg['sn_items']:
                logger.info(f"Parsing table: {sn_table}")
                sn_params = {
                    'sysparm_limit': sysparm_limit,
                    'sysparm_offset': sysparm_offset,
                    'sysparm_display_value': 'true',
                    'sysparm_exclude_reference_link': 'true',
                    'sysparm_query': 'sys_updated_on>=' + max_sys_updated_on + '^ORDERBYsys_updated_on'
                }
                print(sn_params)
                url = f'https://{cfg["domain"]}/api/now/table/{sn_table}'
                resp = requests.get(url, params=sn_params, auth=(cfg["user"],
                                    cfg["password"]), headers=sn_headers,
                                    stream=True)

                if resp.status_code != 200:
                    logger.warning(f"Status: {resp.status_code} Headers: {resp.headers} Error Response: {resp.json()}")
                    raise NotImplementedError

                data = resp.json()

                if data is not None:
                    table = self._clean_empty(data['result'])
                    for row in table:
                        try:
                            row['_id'] = row['sys_id']
                            row['url.domain'] = cfg["domain"]
                            lazy_download = None
                            doc = row, lazy_download
                            this_sys_update_ts = self._string_to_datetime(row['sys_updated_on'])
                            max_sys_updated_on_ts = self._string_to_datetime(max_sys_updated_on)
                            if this_sys_update_ts > max_sys_updated_on_ts:
                                running_sys_updated_on = this_sys_update_ts
                            yield doc
                        except Exception as err:
                            logger.error(f"Error processing: {row} Exception: {err}")

            # Update the offset for the next page
            sysparm_offset += sysparm_limit
            if len(data['result']) < sysparm_limit:
                # Sync is finished, save the latest sys_updated_on value for the next sync.
                if not running_sys_updated_on == "":
                    with open(state_file, 'w') as file:
                        file.write(running_sys_updated_on.strftime('%Y-%m-%d %H:%M:%S'))
                break