import re
from typing import Any, Dict, List, Optional, Type, Union

import pandas
import requests
from bs4 import BeautifulSoup


class RE_CONSTANTS:
    _TITLE_CHARS = r'[A-Za-z]'
    _FIRST_NAME_CHARS = r'[A-Za-z\-\.\']'
    _MIDDLE_NAME_CHARS = r'[A-Za-z\-\.\' ]'
    _MIDDLE_NAME_LOOKAHEAD_CHARS = r'[A-Z]'
    _LAST_NAME_CHARS = r'[A-Za-z\-\' ]'
    _DEGREE_LIST_CHARS = r'[A-Za-z\-,\. \(\)\'/]'

    _TITLE_RE_STRING = fr'^({_TITLE_CHARS}+\.)? *({_FIRST_NAME_CHARS}*) +' +\
        fr'((?:{_MIDDLE_NAME_CHARS})+ (?={_MIDDLE_NAME_LOOKAHEAD_CHARS}))?' +\
        fr'({_LAST_NAME_CHARS}+) *(?:, ?({_DEGREE_LIST_CHARS}+))?$'
    TITLE_REGEX = re.compile(_TITLE_RE_STRING)

    _LOCATION_RE_STRING = r'([a-zA-Z ]+)\,+ ([A-Z]{2}) ([\d\-]+)'
    LOCATION_REGEX = re.compile(_LOCATION_RE_STRING)


class EndocrinologistApi:
    URL = 'https://www.hormone.org/find-an-endocrinologist/find-an-endocrinologist-results?specialty=Thyroid&country=UNITED+STATES&page=0'

    FIELD_GROUPS = ['title', 'info',
                    'areas_of_concentration', 'board_cert', 'address']

    def get_dict(self, soup: BeautifulSoup) -> Dict[str, Any]:
        self.d = {}
        self.soup = soup

        for field in self.FIELD_GROUPS:
            func = getattr(self, f'_get_{field}', lambda: NotImplemented)
            try:
                res = func()
            except Exception as e:
                print(field, e)
                res = None

            if isinstance(res, dict):
                self.d.update(res)
            else:
                self.d[field] = res

        for k, v in self.d.items():
            if isinstance(v, list):
                for i, val in enumerate(v):
                    v[i] = getattr(val, 'strip', lambda: val)()
                self.d[k] = v
            else:
                self.d[k] = getattr(v, 'strip', lambda: v)()

        return self.d

    def _get_title(self) -> Dict[str, Union[str, List[str]]]:
        raw_text = self.soup.find(
            class_='endocrinologist-list-item__title').get_text()
        match = RE_CONSTANTS.TITLE_REGEX.search(raw_text)
        assert match, raw_text
        fields = match.groups()
        prefix, first, middle, last, degrees = fields

        middle = middle.strip().split(' ') if middle else []  # TODO: make validators
        last = last.strip().split(' ') if last else []
        degrees = degrees.replace(' ', '').split(',') if degrees else []

        return {
            'full_name': raw_text,
            'prefix': prefix,
            'first_name': first,
            'middle_name': middle,
            'last_name': last,
            'degrees': degrees
        }

    def _get_info(self) -> Dict[str, str]:
        contact = self.soup.find(class_='endocrinologist-list-item__contact')
        d = {}

        for elem in contact.find_all('p'):
            k = elem.find('strong').get_text()
            v = elem.get_text().strip(k)
            k = k.strip(':').lower()

            if k == 'languages':
                v = v.split(', ')

            d[k] = v

        return d

    def _get_address(self) -> Dict[str, str]:
        elem = self.soup.find(
            class_='endocrinologist-list-item__info')
        source = elem.prettify()

        match = RE_CONSTANTS.LOCATION_REGEX.search(source)
        assert match, elem
        fields = match.groups()
        city, state, zip_ = fields

        return {
            'address_and_occupation': NotImplemented,
            'city': city,
            'state': state,
            'zipcode': zip_
        }

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

    df = pandas.DataFrame(columns=[
                          'full_name', 'prefix', 'first_name', 'middle_name', 'last_name', 'degrees', 'zipcode'])
    r = requests.get(EndocrinologistApi.URL)
    base_soup = BeautifulSoup(r.text, features='lxml')
    for soup in base_soup.find_all(class_='endocrinologist-list-item'):
        api = EndocrinologistApi()
        d = api.get_dict(soup)

        df = df.append(d, ignore_index=True)
        df.to_csv('data/raw/_endocrinologists_raw.csv')
