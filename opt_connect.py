#!/usr/bin/env python3
"""
This module connects to the optconnect API to collect a single day's cellular usage summary
which it then stores in the PostgreSQL database for reporting and analysis by others.
"""

import json
import requests
import sqlalchemy
import sys
import arrow
from sqlalchemy import create_engine, Column, Integer, String
from config import PSQL_STRING, OPT_CONNECT_CONFIG


class Cred:
    """
    This class contains login credentials
    This class is used in the fetch_auth function
    """
    url = 'https://api.optconnect.com/summit/beta/accounts/login/app_secret'
    data = OPT_CONNECT_CONFIG
    data = json.dumps(data)
    headers = {'accept': 'application/json', 'content-type': 'application/json'}


def fetch_auth():
    """
    This function is used to get authentication from the opt_connect api.
        Sending a request returns a dict with an api_Key and a token.
        https://docs.optconnect.com/documentation
    """
    response = requests.request(
        "POST",
        Cred.url,
        data=json.dumps(OPT_CONNECT_CONFIG),
        headers=Cred.headers
    ).json()

    token = response["token"]
    api_key = response["apiKey"]
    return api_key, token


def fetch_summit_ids(api_key, token):
    """
    This function is used to get all summit ids
        Sending a request returns a list of integer ids
    """
    response = requests.request(
        "GET",
        'https://api.optconnect.com/summit/beta/devices',
        headers={'accept': 'application/json',
                 'Authorization': token,
                 'x-api-key': api_key}
    ).json()

    try:
        summit_ids = [d['summitId'] for d in response]
    except TypeError as e:
        msg = response.get('message', e)
        raise OptConnectAPIException(msg)
    return summit_ids


def flatten(detail):
    """
    Returns the detail data with the customer, usages, and dynamifcFields flattened.
    So to start with `detail` will contain all the api data, in a nested object structure, like:
    {
        'summitId': 81056,
        'carrier': 'Verizon',
        'customer': {'id': 3384, 'name': 'Byte Foods-OC'},
        'serialNumber': '70B3D5D3B93A',
        'description': '',
        'yourDeviceId': '',
        'deviceModel': 'OC-4300 Neo Wireless Unit',
        'lastCheckInTime': 1525912883589,
        'snapshotRefreshTime': 1525898485579,
        'deviceUpTime': '8 days and 03:56',
        'signalStrength': '-59 dBm (93.1%)',
        'signalQuality': '3',
        'dynamicFields': [{'key': 'staticIP', 'value': '10.145.188.51'}],
        'dataPlan': 104857600,
        'usages': [{'date': '2018-05-08', 'dataUsed': 43063500}],
        'customer_name': 'Byte Foods-OC',
        'customer_id': 3384,
        'data_used_date': '2018-05-08',
        'data_used': 43063500
    }
    This function will flatten the nested objects, returning something like this:
    {
        'summitId': 81056,
        'carrier': 'Verizon',
        'serialNumber': '70B3D5D3B93A',
        'description': '',
        'yourDeviceId': '',
        'deviceModel': 'OC-4300 Neo Wireless Unit',
        'lastCheckInTime': 1525914683593,
        'snapshotRefreshTime': 1525898485579,
        'deviceUpTime': '8 days and 03:56',
        'signalStrength': '-59 dBm (93.1%)',
        'signalQuality': '3',
        'dataPlan': 104857600,
        'customer_name': 'Byte Foods-OC',
        'customer_id': 3384,
        'data_used_date': '2018-05-08',
        'data_used': 43063500
    }
    """
    detail["customer_name"] = detail["customer"]["name"]
    detail["customer_id"] = detail["customer"]["id"]
    detail.pop("customer")
    if detail["usages"]:
        detail["data_used_date"] = detail["usages"][0].get("date", None)
        arrow.get(detail["data_used_date"], 'YYYY-MM-DD')
        detail["data_used"] = detail["usages"][0].get("dataUsed", None)
    else:
        detail["data_used_date"] = None
        detail["data_used"] = None
    detail.pop("usages")
    if detail["dynamicFields"][0]["key"] == "staticIP":
        detail["static_ip"] = detail["dynamicFields"][0]["value"]
    detail.pop("dynamicFields")
    return detail


def fetch_detailed_info(summit_ids, api_key, token):
    """
    This function is used to get all devices detailed info
    """
    all_rows = []
    end = start = arrow.utcnow().to('US/Pacific').shift(days=-1).format('YYYY-MM-DD')
    for summit_id in summit_ids:
        details = requests.request(
            "GET",
            'https://api.optconnect.com/summit/beta/devices/{}'.format(summit_id),
            headers={'accept': 'application/json',
                     'Authorization': token,
                     'x-api-key': api_key}
        ).json()

        usage = requests.request(
            "GET",
            'https://api.optconnect.com/summit/beta/devices/usage/{summit_id}'
            '?range.end={end}&range.start={start}'.format(
                summit_id=summit_id, start=start, end=end
            ),
            headers={'accept': 'application/json',
                     'Authorization': token,
                     'x-api-key': api_key}
        ).json()
        new_row = {}
        new_row.update(details)
        new_row.update(usage)
        all_rows.append(flatten(new_row))
    return all_rows


def insert_info(all_rows):
    """
    This function is used to insert all devices info in the database
    """
    engine = create_engine(PSQL_STRING,
                           convert_unicode=True)
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table('opt_connect', metadata,
                             Column('summit_id', Integer, key='summitId'),
                             Column('carrier', String),
                             Column('customer_name', String),
                             Column('customer_id', Integer),
                             Column('your_device_id', String, key='yourDeviceId'),
                             Column('signal_strength', String, key='signalStrength'),
                             Column('description', String),
                             Column('device_model', String, key='deviceModel'),
                             Column('serial_number', String, key='serialNumber'),
                             Column('device_up_time', String, key='deviceUpTime'),
                             Column('static_ip', String),
                             Column('snapshot_refresh_time', sqlalchemy.types.BigInteger,
                                    key='snapshotRefreshTime'
                                    ),
                             Column('last_check_in_time', sqlalchemy.types.BigInteger,
                                    key='lastCheckInTime'
                                    ),
                             Column('signal_quality', String, key='signalQuality'),
                             Column('data_plan', sqlalchemy.types.BigInteger, key='dataPlan'),
                             Column('data_used_date', sqlalchemy.types.Date),
                             Column('data_used', sqlalchemy.types.BigInteger),
                             schema='schema1'
                             )

    ins = table.insert().values(all_rows)
    engine.execute(ins)


class OptConnectAPIException(Exception):
    pass


def main():
    authorization = fetch_auth()
    summit_id = fetch_summit_ids(authorization[0], authorization[1])
    all_rows = fetch_detailed_info(summit_id, authorization[0], authorization[1])
    insert_info(all_rows)


if __name__ == "__main__":
    sys.exit(main())
