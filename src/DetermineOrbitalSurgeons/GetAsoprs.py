import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, fields, astuple
from typing import Dict, List, NamedTuple, Optional, Union, Tuple

import time
import pandas
import requests
from bs4 import BeautifulSoup
from inflection import underscore  # camelCase to snake_case


def list_default(): return field(default_factory=list)

@dataclass
class Address():
    line1: Optional[str] = None
    line2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_: Optional[str] = None


@dataclass
class Doctor():
    prefix: Optional[str] = None
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    middle_initial: Optional[str] = None
    last_name: Optional[str] = None
    suffix: Optional[str] = None
    orgs: List[str] = list_default()
    phones: List[str] = list_default()
    addresses: List[Address] = list_default()
    other_attrs: List[Dict] = list_default()


class AsoprsBasicData:
    DIRECTORY_URL = "https://www.asoprs.org/ui-directory-search/v2/search-directory-paged/"

    def __init__(self):
        pass

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
                f"c9ab7fd9-ec40-439d-8ca6-824da56455eb?pageSize={page_size}&pageNumber=1"

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

    def __init__(self, workers: int, sleep_time: int = 10):
        self.tpe = ThreadPoolExecutor(max_workers=workers)
        self.sleep_time = sleep_time

    def get_detailed_asoprs_data(self, df: pandas.DataFrame) -> pandas.DataFrame:
        df = df.copy()
        df.index = df.index.astype(int)

        target_cols = [f.name for f in fields(Doctor)]
        for c in target_cols:
            df[c] = 0
            df[c] = df[c].astype(object)


        futs_to_idxs = {self.tpe.submit(self._worker_get_detailed, i): i for i in df.index}


        for fut in as_completed(futs_to_idxs):
            res = fut.result()
            idx = futs_to_idxs[fut]

            try:
                #for col, val in zip(target_cols, res):
                    #df.at[idx, col] = val
                df.loc[[idx], target_cols] = res
            except Exception as e:
                print(e)
                print(idx)
                print(type(idx))
                print(target_cols)
                print(len(target_cols), len(res))
                print(res)
                raise e
            print(idx, 'done')
        

        return df

    def _worker_get_detailed(self, idx: str) -> Tuple[str]:
        print(idx)
        url = f'https://www.asoprs.org/index.php?option=com_community&view=profile&userid={idx}'

        resp = requests.get(url)
        status = resp.status_code
        while status in [429, 443]: # rate limit
            time.sleep(self.sleep_time)
            resp = requests.get(url)
            status = resp.status_code



            
        match = self.GET_JSON.search(resp.text)
        assert match, f"failed to find json: {resp.text}"
        json_ = json.loads(match.group(1))
        doctor = self._parse_detailed_json(json_)
        return astuple(doctor)



    def _parse_detailed_json(self, json_: Dict) -> Doctor:
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
    FILEPATH = 'data/all_ASOPRS.csv'
    # AsoprsInterface.get_asoprs_lst().to_csv(FILEPATH)

    df = pandas.read_csv(FILEPATH, index_col='idx')

    
    new_frame = AsoprsAdvancedData(20, 5).get_detailed_asoprs_data(df)
    new_frame.to_csv('full_data.csv')
    
    # AsoprsInterface.get_detailed_asoprs_data(df).to_csv(FILEPATH)
