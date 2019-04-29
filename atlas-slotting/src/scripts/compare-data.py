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
import sys
from argparse import Namespace
from typing import Dict, List

import requests


def parse_args() -> Namespace:
    parser = argparse.ArgumentParser(description='Compare Atlas Slotting data with Edda data')
    parser.add_argument('--region', type=str, nargs='+', required=True,
                        choices=['eu-west-1', 'us-east-1', 'us-west-1', 'us-west-2'],
                        help='List of AWS regions where data will be compared')
    parser.add_argument('--edda_name', type=str, required=True,
                        help='Edda DNS name, with a region placeholder')
    parser.add_argument('--slotting_name', type=str, required=True,
                        help='Atlas Slotting DNS name, with a region placeholder')
    parser.add_argument('--app_name', type=str, nargs='+', required=True,
                        help='List of application names that will be compared')
    return parser.parse_args()


def get_edda_data(args: Namespace, region: str) -> List[Dict]:
    url = f'http://{args.edda_name.format(region)}/api/v2/group/autoScalingGroups;_expand'
    r = requests.get(url)
    if not r.ok:
        print(f'ERROR: Failed to load Edda data from {url}')
        sys.exit(1)
    else:
        return [asg for asg in r.json() if asg['name'].split('-')[0] in args.app_name]


def get_slotting_data(args: Namespace, region: str) -> List[Dict]:
    url = f'http://{args.slotting_name.format(region)}/api/v1/autoScalingGroups?verbose=true'
    r = requests.get(url)
    if not r.ok:
        print(f'ERROR: Failed to load Edda data from {url}')
        sys.exit(1)
    else:
        return [asg for asg in r.json() if asg['name'].split('-')[0] in args.app_name]


def build_slot_map(asgs):
    res = {}

    for asg in asgs:
        slots = {}
        for i in asg['instances']:
            slots[i['instanceId']] = i['slot']
        res[asg['name']] = slots

    return res


def compare_data(args: Namespace, region: str):
    edda_slots = build_slot_map(get_edda_data(args, region))
    slotting_slots = build_slot_map(get_slotting_data(args, region))

    matching = []
    differing = []

    for asg in slotting_slots:
        set1 = set(slotting_slots[asg].items())
        set2 = set(edda_slots[asg].items())

        if set1 ^ set2 == set():
            matching.append(asg)
        else:
            differing.append(asg)

    print(f'asg slots match: {sorted(matching)}')
    print(f'asg slots differ: {sorted(differing)}')


def main():
    args = parse_args()

    print('==== config ====')
    print(f'Edda: {args.edda_name}')
    print(f'Atlas Slotting: {args.slotting_name}')

    for region in args.region:
        print(f'==== {region} ====')
        compare_data(args, region)


if __name__ == "__main__":
    main()
