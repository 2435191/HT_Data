import requests
import pandas
from bs4 import BeautifulSoup
import re
import json
from typing import Dict, Union, Optional


class AsoprsInterface():
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
            data = cls._get_asoprs_list_internal(s, page_size, addl_payload, internal_url)

            for surgeon in data['results']:
                all_asoprs.loc[surgeon['id'], :] = (surgeon['title'], surgeon['avatar_url'])

            if data['current_page'] == data['total_page_count']:
                break

            internal_url = data['next_page_url']

        return all_asoprs

    @classmethod
    def _get_asoprs_list_internal(cls,
                                  s: requests.Session,
                                  page_size: int, 
                                  addl_payload: Dict[str, str],
                                  internal_url: Optional[str],
                                  ) -> "JSON_response":

        if internal_url is None:
            internal_url = "http://service-router.prod01.memberclicks.io/search-results/v2/results/" + \
                f"c9ab7fd9-ec40-439d-8ca6-824da56455eb?pageSize={page_size}&pageNumber=1"

        payload = {
            "url": internal_url
        }
        payload.update(addl_payload)

        resp: requests.Response = s.post(cls.DIRECTORY_URL, payload)
        data: "JSON_response" = json.loads(resp.text)

        return data




if __name__ == '__main__':
    AsoprsInterface.get_asoprs_lst().to_csv('data/all_ASOPRS.csv')
