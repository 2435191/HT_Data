import time
from typing import Dict, Tuple
import pandas
from selenium import webdriver
from selenium.common.exceptions import (StaleElementReferenceException,
                                        TimeoutException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from doctor_class import Doctor




class BasicItedsApi:
    URL = 'https://thyroideyedisease.org/physician-directory-member-list/'
    BASIC_CSV_PATH = 'data/_iteds_basic_data.csv'

    def __init__(self):
        self._driver = webdriver.Chrome(ChromeDriverManager().install())
        self._df = pandas.DataFrame(columns=['name', 'url'])
    
        
    def _scrape_page(self) -> None:
        doctors = WebDriverWait(self._driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'item-entry'))
        )
        print(len(doctors))

        for d in doctors:
            name = d.find_element_by_class_name('member-name').text
            url = d.find_element_by_class_name('member-name')\
                .find_element_by_css_selector('a')\
                .get_attribute('href')
            
            self._df.loc[len(self._df), :] = [name, url]
        

    def get_urls_lst(self) -> pandas.DataFrame:
        self._driver.get(self.URL)
        self._scrape_page()

        while True:
            

            try:
                next_ = WebDriverWait(self._driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[class="next page-numbers"]'))
                )
            except TimeoutException:
                print('not found')
                break

            next_.click()
            time.sleep(2)

            self._scrape_page()




        self._driver.quit()
        return self._df

def get_doctor_data(name: str, url: str) -> Tuple[bool, Dict[str, str]]:
    out = {'__DOCTOR_NAME_FROM_CSV': name}
    
    try:
        for df in pandas.read_html(url):
            out.update(df.set_index(0)[1].to_dict())
    except Exception as e:
        print(name, url)
        raise e
    
    cleaned_keys = {str(i).upper() for i in set(out.keys())}
    success = cleaned_keys != {'__DOCTOR_NAME_FROM_CSV', 'NAME'}
    print(name, url, success)
        

    return success, out

if __name__ == '__main__':
    BasicItedsApi().get_urls_lst().to_csv(BasicItedsApi.BASIC_CSV_PATH)
    df = pandas.read_csv(BasicItedsApi.BASIC_CSV_PATH)
    df['has_interesting_data'] = df.apply(lambda row: get_doctor_data(row['name'], row['url'] + 'profile/')[0], axis=1)
    df.to_csv('data/_iteds_raw.csv')
