import unittest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from connectors.source import BaseDataSource
from connectors.sources.sncmdb import SncmdbDataSource
from connectors.source import DataSourceConfiguration


class TestSncmdbDataSource(unittest.TestCase):
    def setUp(self):
        config = {
            "domain": {"value": "dev138640.service-now.com"},
            "user": {"value": "admin"},
            "password": {"value": "vxOx5RkXL5@$"},
            "sn_items": {"value": "cmdb_ci_hpux_server,cmdb_ci_computer"}
        }
        self.configuration = DataSourceConfiguration(config=config)
        self.data_source = SncmdbDataSource(self.configuration)

    @patch('connectors.sources.sncmdb.requests.get')

    @patch('connectors.sources.sncmdb.requests.get')
    def test_ping_success(self, mock_get):
        response_mock = MagicMock()
        response_mock.status_code = 200
        mock_get.return_value = response_mock

        result = asyncio.run(self.data_source.ping())

        self.assertTrue(result)

        mock_get.assert_called_once_with(
            'https://dev138640.service-now.com/api/now/table/cmdb_ci_hpux_server',
            params={
                'sysparm_limit': '10000',
                'sysparm_display_value': 'true',
                'sysparm_exclude_reference_link': 'true',
            },
            auth=('admin', 'vxOx5RkXL5@$'),
            headers={"Accept": "application/json"},
            stream=True
        )

    @patch('connectors.sources.sncmdb.requests.get')
    def test_ping_failure(self, mock_get,):
        response_mock = MagicMock()
        response_mock.status_code = 500
        mock_get.return_value = response_mock

        with self.assertRaises(NotImplementedError):
            asyncio.run(self.data_source.ping())

        mock_get.assert_called_once()

    def test_clean_empty(self):
        data = [{'item1': 'value1', 'item2': ''}, {'item1': '', 'item2': 'value2'}]
        expected_result = [{'item1': 'value1'}, {'item2': 'value2'}]
        result = self.data_source._clean_empty(data)
        self.assertEqual(result, expected_result)

    async def test_get_docs(self):
        response_mock = AsyncMock()
        response_mock.status_code = 200
        response_mock.json.return_value = {
            'result': [
                {'sys_id': 'id1', 'sys_updated_on': 'timestamp1'},
                {'sys_id': 'id2', 'sys_updated_on': 'timestamp2'}
            ]
        }
        mock_get.return_value = response_mock

        docs = self.data_source.get_docs()
        result = [doc async for doc in docs]

        expected_result = [
            ({'_id': 'id1', '@timestamp': 'timestamp1', 'sys_id': 'id1', 'sys_updated_on': 'timestamp1'}, None),
            ({'_id': 'id2', '@timestamp': 'timestamp2', 'sys_id': 'id2', 'sys_updated_on': 'timestamp2'}, None)
        ]
        self.assertEqual(result, expected_result)

        mock_get.assert_called_once_with(
            'https://dev138640.service-now.com/api/now/table/cmdb_ci_hpux_server',
            params={
                'sysparm_limit': 1000,
                'sysparm_offset': 0,
                'sysparm_display_value': 'true',
                'sysparm_exclude_reference_link': 'true',
            },
            auth=('admin', 'vxOx5RkXL5@$'),
            headers={'Accept': 'application/json'},
            stream=True
        )
