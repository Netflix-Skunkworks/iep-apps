#!/usr/bin/env python3

# Copyright 2014-2019 Netflix, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import gzip
import json
import pprint
from argparse import Namespace
from datetime import datetime
from typing import Dict, List

import boto3
import requests
import sys
from boto3.dynamodb.types import Binary
from botocore.exceptions import ClientError, ProfileNotFound


def parse_args() -> Namespace:
    parser = argparse.ArgumentParser(description='Lift slotting data from Edda into DynamoDB')
    parser.add_argument('--profile', type=str, required=True,
                        help='AWS credentials profile used to write to the Atlas Slotting DynamoDB table')
    parser.add_argument('--region', type=str, nargs='+', required=True,
                        choices=['eu-west-1', 'us-east-1', 'us-west-1', 'us-west-2'],
                        help='List of AWS regions where data will be lifted from Edda into DynamoDB')
    parser.add_argument('--edda_name', type=str, required=True,
                        help='Edda DNS name, with a region placeholder, where data will be read')
    parser.add_argument('--slotting_table', type=str, required=True,
                        help='Atlas Slotting DynamoDB table name, where data will be written')
    parser.add_argument('--app_name', type=str, nargs='+', required=True,
                        help='List of application names that will be lifted')
    parser.add_argument('--dryrun', action='store_true', required=False, default=False,
                        help='Enable dryrun mode, to preview changes')
    return parser.parse_args()


def get_edda_data(args: Namespace, region: str) -> List[Dict]:
    url = f'http://{args.edda_name.format(region)}/api/v2/group/autoScalingGroups;_expand'
    r = requests.get(url)
    if not r.ok:
        print(f'ERROR: Failed to load Edda data from {url}')
        sys.exit(1)
    else:
        return [asg for asg in r.json() if asg['name'].split('-')[0] in args.app_name]


def get_ddb_table(args: Namespace, region: str):
    try:
        session = boto3.session.Session(profile_name=args.profile)
    except ProfileNotFound:
        print(f'ERROR: AWS profile {args.profile} does not exist')
        sys.exit(1)

    dynamodb = session.resource('dynamodb', region_name=region)
    table = dynamodb.Table(args.slotting_table)

    try:
        table.table_status
    except ClientError as e:
        code = e.response['Error']['Code']

        if code == 'ExpiredTokenException':
            print(f'ERROR: Security token in AWS profile {args.profile} has expired')
        elif code == 'ResourceNotFoundException':
            print(f'ERROR: Table {args.slotting_table} does not exist in {region}')
        else:
            pprint.pprint(e.response)

        sys.exit(1)

    return table


def lift_data(args: Namespace, region: str):
    asgs = get_edda_data(args, region)
    table = get_ddb_table(args, region)

    for asg in asgs:
        item = {
            'name':  asg['name'],
            'active': True,
            'data': Binary(gzip.compress(bytes(json.dumps(asg), encoding='utf-8'))),
            'timestamp': int(datetime.utcnow().timestamp() * 1000)
        }

        if args.dryrun:
            print(f'DRYRUN: PUT {asg["name"]}')
        else:
            print(f'PUT {asg["name"]}')
            table.put_item(Item=item)


def main():
    args = parse_args()

    print('==== config ====')
    print(f'AWS Profile: {args.profile}')
    print(f'Source Edda: {args.edda_name}')
    print(f'Destination Table: {args.slotting_table}')

    for region in args.region:
        print(f'==== {region} ====')
        lift_data(args, region)


if __name__ == "__main__":
    main()
