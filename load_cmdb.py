#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Load test data for ServiceNow CMDB connector for Elastic Enterprise Search.

"""


import requests
import random
from random_word import RandomWords


# Example usage
configuration = {
    "domain": "YOUR_SANDBOX.service-now.com",
    "user": "admin",
    "password:"CHANGEME",
}

r = RandomWords()


def add_host_entry(configuration, host_name, ip_address):
    url = f"https://{configuration['domain']}/api/now/table/cmdb_ci_computer"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    auth = (configuration["user"], configuration["password"])
    payload = {
        "name": host_name,
        "ip_address": ip_address,
        "sys_class_name": "cmdb_ci_computer",
        "location": "815 E Street, San Diego,CA",
        "class": "Computer"
    }
    
    response = requests.post(url, json=payload, auth=auth, headers=headers)
    
    if response.status_code == 201:
        print("Host entry added successfully.")
    else:
        print("Failed to add host entry.")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")


count = 0
for i in range(10101):
    host_name = r.get_random_word()
    ip = ".".join(map(str, (random.randint(0, 255) 
                            for _ in range(4))))
    print(count, host_name, ip)
    add_host_entry(configuration, host_name, ip)
    count += 1
