import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import astuple, fields
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

import requests
import pandas
from bs4 import BeautifulSoup
from inflection import underscore # camelCase to snake_case
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


class _CustomWaitForAllData:
    def __init__(self, locator, expected_number_of_elements):
        self.locator = locator
        self.expected = expected_number_of_elements

    def __call__(self, driver):
        # Finding the referenced element
        elements = driver.find_elements(*self.locator)
        if len(elements) >= self.expected:
            return elements
        return False


class _CustomWaitForChange:
    def __init__(self, locator, prev):
        self.locator = locator
        self.prev = prev

    def __call__(self, driver):
        try:
            elements = EC.presence_of_all_elements_located(
                self.locator)(driver)
        except WebDriverException:
            return False

        if elements != self.prev:
            return elements
        return False


class AsoprsBasicDataApi:
    @classmethod
    def get_asoprs_lst(cls) -> pandas.DataFrame:
        all_asoprs = pandas.DataFrame(columns=["name", "photo_url"])

        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.get(
            'https://www.asoprs.org/index.php?option=com_mcdirectorysearch&view=search&id=12029#/')

        select_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "6-field"))
        )
        Select(select_elem).select_by_visible_text('United States')

        logger.debug("selected country")

        success = False
        for elem in driver.find_elements_by_class_name('gen-button'):
            if elem.text.lower() == 'search':
                elem.click()
                success = True
        assert success, "couldn't find button"

        paging_dropdown = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, 'per-page-select'))
        )
        Select(paging_dropdown).select_by_index(2)
        logger.debug("selected paging value")

        WebDriverWait(driver, 10).until(
            _CustomWaitForAllData(
                (By.CSS_SELECTOR, ".search-profile.ng-scope"), 15)
        )  # wait for all data to load

        logger.debug("all data loaded")

        num_pages = int(driver.find_element_by_id('total-pages').text)
        prev = []

        for _ in range(num_pages):
            profiles = WebDriverWait(driver, 10).until(
                _CustomWaitForChange(
                    (By.CSS_SELECTOR, ".search-profile.ng-scope"), prev)
            )
            logger.debug("new data loaded")
            for elem in profiles:
                name = elem.find_element_by_css_selector(
                    '.ds-contact-name .ng-scope').text
                img_url = elem.find_element_by_css_selector(
                    '.ds-avatar > img').get_attribute('src')  # contains profile link

                all_asoprs.loc[len(all_asoprs), :] = [name, img_url]
                logger.debug(f"name: {name}, img_url: {img_url}")

            prev = profiles
            driver.find_element_by_id('next').click()

            logger.debug('button clicked')

        return all_asoprs


class AsoprsAdvancedDataApi:
    GET_JSON = re.compile('attributesView\s*:\s*(\{.+\})')
    EXCLUDED_LABELS = ['label', 'type', 'typeLabelId', 'attributeId', 'displayType', 'maxLength']

    @classmethod
    def get_detailed_asoprs_data(cls, df: pandas.DataFrame, id_column_name: str, workers: int, sleep_time: float) -> pandas.DataFrame:
        df = df.copy()
        df[id_column_name] = df[id_column_name].astype(int)
        df.index = df[id_column_name
        ]
        tpe = ThreadPoolExecutor(max_workers=workers)

        futs_to_idxs = {tpe.submit(
            cls._worker_get_detailed, i, sleep_time): i for i in df.index}

        i = 0
        for fut in as_completed(futs_to_idxs):
            res = fut.result()
            
            idx = futs_to_idxs[fut]

            logger.debug(f"result recieved: {res} for {idx}")

            for k, v in res.items():
                df.at[idx, k] = v
            df.to_csv('data/raw/_asoprs_raw.csv')
                
            i += 1

            logger.info(f"Done with {i}/{len(df)}")

        return df

    @classmethod
    def _worker_get_detailed(cls, idx: str, sleep_time: float) -> Dict[str, Any]:
        url = f'https://www.asoprs.org/index.php?option=com_community&view=profile&userid={idx}'

        logger.info(f"Getting url: {url}")

        while True:  # rate limit
            resp = None
            try:
                resp = requests.get(url)
            except Exception as e:
                logger.error(f"sleeping because exception trying to access {url}")
                time.sleep(sleep_time)
                continue

            status = resp.status_code
            if status in [429, 443]:
                logger.warning(f"sleeping because of status: {status} trying to access {url}")
                time.sleep(sleep_time)
                continue
            break
        
        logger.debug(f"resp.text: {resp.text} trying to access {url}")

        match = cls.GET_JSON.search(resp.text)
        d = json.loads(match.group(1))

        
        logger.info(f"network success, got dict d: {d} trying to access {url}")

        attrs = d['builtInAttributes'] + d['customAttributes']

        logger.debug(f"attrs: {attrs} trying to access {url}")

        formatted_attrs = {}

        for item_dict in attrs:
            label = item_dict['label']
            for k, v in item_dict.items():

                if k in cls.EXCLUDED_LABELS:
                    continue

                formatted_attrs_key = f"{label}_{k}"
                i = 1
                while formatted_attrs_key in formatted_attrs:
                    formatted_attrs_key = f"{label}_{k}_{i}"
                    i += 1
                formatted_attrs[formatted_attrs_key] = v


        return formatted_attrs

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if __name__ == '__main__':
    # basic_df = AsoprsBasicDataApi.get_asoprs_lst()
    # basic_df.to_csv('data/raw/_basic_asoprs_raw.csv')

    basic_df = pandas.read_csv('data/raw/_basic_asoprs_raw.csv', index_col=0)
    ids = basic_df['photo_url'].apply(lambda s: s.split('/')[-2])
    basic_df['idx'] = ids

    advanced_df = AsoprsAdvancedDataApi.get_detailed_asoprs_data(basic_df, 'idx', 5, 10)
    
    advanced_df.to_csv('data/raw/_asoprs_raw.csv')
