import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import astuple, fields
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Tuple, Union

import pandas
import requests
from bs4 import BeautifulSoup
from inflection import underscore  # camelCase to snake_case

sys.path.append('../') # FIXME

from custom_dclasses import Doctor, Address


class AsoprsBasicData:
    DIRECTORY_URL = "https://www.asoprs.org/ui-directory-search/v2/search-directory-paged/"
    SEARCH_ID = '88a8ebb7-93f5-4cd1-8ecf-c748e32bac33' # TODO: make dynamics

    @classmethod
    def get_asoprs_lst(cls,
                       page_size: int = 15,
                       addl_payload: Dict[str, str] = {}
                       ) -> pandas.DataFrame:

        all_asoprs = pandas.DataFrame(columns=["idx", "name", "photo_url"]) \
            .set_index("idx")

        s = requests.Session()
        s.cookies.set("serviceID", "8304", domain="www.asoprs.org")

        internal_url = None
        while True:
            data = cls._get_asoprs_list_internal(
                s, page_size, addl_payload, internal_url)

            for surgeon in data['results']:
                all_asoprs.loc[surgeon['id'], :] = (
                    surgeon['title'], surgeon['avatar_url'])

            if data['current_page'] == data['total_page_count']:
                break

            internal_url = data['next_page_url']

        return all_asoprs

    @classmethod
    def _get_asoprs_list_internal(cls,
                                  s: requests.Session,
                                  page_size: int,
                                  addl_payload: Dict[str, str],
                                  internal_url: Optional[str] = None,
                                  ) -> "JSON_response":

        if internal_url is None:
            internal_url = "http://service-router.prod01.memberclicks.io/search-results/v2/results/" + \
                f"{cls.SEARCH_ID}?pageSize={page_size}&pageNumber=1"

        payload = {
            "url": internal_url
        }
        payload.update(addl_payload)

        # few enough post calls where async/multithreading isn't necessary
        resp: requests.Response = s.post(cls.DIRECTORY_URL, payload)
        data: "JSON_response" = json.loads(resp.text)

        return data


class AsoprsAdvancedData:
    GET_JSON = re.compile('attributesView\s*:\s*(\{.+\})')

    @classmethod
    def get_detailed_asoprs_data(cls, df: pandas.DataFrame, workers: int, sleep_time: float) -> pandas.DataFrame:
        df = df.copy()
        df.index = df.index.astype(int)

        target_cols = [f.name for f in fields(Doctor)]
        for c in target_cols:
            df[c] = 0
            df[c] = df[c].astype(object)

        tpe = ThreadPoolExecutor(max_workers=workers)

        futs_to_idxs = {tpe.submit(
            cls._worker_get_detailed, i, sleep_time): i for i in df.index}

        for fut in as_completed(futs_to_idxs):
            res = fut.result()
            idx = futs_to_idxs[fut]

            try:
                # for col, val in zip(target_cols, res):
                #df.at[idx, col] = val
                df.loc[[idx], target_cols] = res
            except Exception as e:
                print('EXCEPTION')
                #print(e)
                print(idx)
                print(type(idx))
                #print(target_cols)
                #print(len(target_cols), len(res))
                #raise e
                print('EXCEPTION')
            print(idx, 'done')

        return df

    @classmethod
    def _worker_get_detailed(cls, idx: str, sleep_time: float) -> Tuple[str]:
        print(idx)
        url = f'https://www.asoprs.org/index.php?option=com_community&view=profile&userid={idx}'

        while True:  # rate limit
            print('getting request')
            try:
                resp = requests.get(url)
            except:
                print('failed', resp)
                time.sleep(sleep_time)
                continue

            status = resp.status_code
            if status in [429, 443]:
                print('failed')
                time.sleep(sleep_time)
                
                continue
            print('success')
            break

        match = cls.GET_JSON.search(resp.text)
        if not match:
            with open(f'error_{idx}.html', 'w+') as f:
                f.write(resp.text)
            assert match
        json_ = json.loads(match.group(1))
        doctor = cls._parse_detailed_json(json_)
        return astuple(doctor)

    @classmethod
    def _parse_detailed_json(cls, json_: Dict) -> Doctor:
        tup = Doctor()
        tup.other_attrs = json_['customAttributes']

        for attr in json_['builtInAttributes']:

            label = attr['label']

            normal_attr_flag = False

            if label == 'Full Name':
                for name in ['prefix', 'firstName', 'middleName', 'middleInitial', 'lastName', 'suffix']:
                    tup.__setattr__(underscore(name), attr[name])
                normal_attr_flag = True
            if label == 'Organization':
                tup.name = attr['name']
                normal_attr_flag = True
            if 'phone' in label.lower():
                tup.phones.append(attr['phone'])
                normal_attr_flag = True
            if 'address' in label.lower():
                tup.addresses.append(
                    Address(*[attr[i]
                            for i in ['line1', 'line2', 'city', 'state', 'zip']])
                )
                normal_attr_flag = True

            if not normal_attr_flag:
                tup.other_attrs.append(attr)

        return tup


if __name__ == '__main__':
    df = AsoprsAdvancedData.get_detailed_asoprs_data(
        pandas.read_csv('data/basic_asoprs.csv', index_col='idx'), 20, 5
    )
    df.to_csv('data/asoprs.csv')
