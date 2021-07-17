import re
from typing import Dict, List, Optional, Type

import pandas
import requests
from bs4 import BeautifulSoup
from pydantic import (BaseModel, Field, ValidationError, root_validator,
                      validator)

from doctor_class import (Doctor, DoctorInfo, Title, flatten,
                          recursively_get_all_fields)


class EndocrinologistInfo(DoctorInfo):
    @validator('languages', pre=True)
    def lang_val(cls, lang_str):
        if isinstance(lang_str, list):
            return lang_str
        return lang_str.replace(', ', ',').split(',')


class Endocrinologist(Doctor):
    info: Optional[EndocrinologistInfo] = None
    source: str = 'endocrinologists'
        


class _EndocrinologistImpl:
    URL = 'https://www.hormone.org/find-an-endocrinologist/find-an-endocrinologist-results?specialty=Thyroid&country=UNITED+STATES&page=0'
    # https://regex101.com/r/gZVDIk/1
    TITLE_REGEX = re.compile(
        r'^(.+\.) *([A-Za-z]+?) +(?:((?:(?:[A-Z] )+[A-Z](?= ))|(?:[A-Za-z\-]+)) )?([A-Za-z\- ]+) *(?:, ?([A-Za-z\-,\. ]+))?$'
    )

    def __init__(self, soup: BeautifulSoup):
        self.d = {}

        self.soup = soup

        for field, info in Endocrinologist.__fields__.items():
            try:
                func = getattr(self, f'_get_{field}')
                res = func()
                self.d[field] = res
            except Exception as e:
                #print(field, e)
                if info.required:
                    pass
                    # raise e


    def _get_title(self) -> Title:
        raw_text = self.soup.find(
            class_='endocrinologist-list-item__title').get_text()
        fields = self.TITLE_REGEX.search(raw_text).groups()
        prefix, first, middle, last, degrees = fields

        middle = middle.strip().split(' ') if middle else []  # TODO: make validators
        last = last.strip().split(' ') if last else []
        degrees = degrees.replace(' ', '').split(',') if degrees else []

        return Title(**{
            'prefix': prefix,
            'first_name': first,
            'middle_name': middle,
            'last_name': last,
            'degrees': degrees
        })

    def _get_info(self) -> DoctorInfo:
        contact = self.soup.find(class_='endocrinologist-list-item__contact')

        d = {'misc': {}}
        for elem in contact.find_all('p'):
            k = elem.find('strong').get_text()
            v = elem.get_text().strip(k)
            k = k.strip(':').lower()

            if k in DoctorInfo.__fields__ and k != 'misc':
                d[k] = v
            else:
                d['misc'][k] = v

        return DoctorInfo(**d)

    def _get_areas_of_concentration(self) -> List[str]:
        concentrations_elem = None
        for elem in self.soup.find_all(class_='endocrinologist-list-item__area-title'):
            if elem.get_text() == 'Area of Concentration':
                concentrations_elem = elem.parent.find(
                    class_='endocrinologist-list-item__description')
                break

        concentrations = [
            i for i in concentrations_elem.children if str(i) != '<br/>']

        return concentrations

    def _get_board_cert(self) -> str:
        for elem in self.soup.find_all(class_='endocrinologist-list-item__area-title'):
            if elem.get_text() == 'General Board Certification':
                cert = elem.parent.find(
                    class_='endocrinologist-list-item__area-description').get_text()
                return cert


if __name__ == '__main__':
    
    df = Endocrinologist.to_empty_DataFrame()
    print(df)
    r = requests.get(_EndocrinologistImpl.URL)
    base_soup = BeautifulSoup(r.text, features='lxml')
    for soup in base_soup.find_all(class_='endocrinologist-list-item'):
        try:
            d = _EndocrinologistImpl(soup).d
            end = Endocrinologist(**d)
        except ValidationError:
            print('ERROR')
            continue
        df = df.append(end.to_DataFrame(), ignore_index=True)
        print(len(df))

    df.to_csv('data/endocrinologists.csv')
    


