import requests
import pandas
import re
from pydantic import BaseModel, Field, validator, ValidationError
from bs4 import BeautifulSoup

from typing import (
    Dict, List, Optional, Type
)

class Title(BaseModel):
    prefix:      str
    first_name:  str
    middle_name: Optional[List[str]] = []
    last_name:   List[str]
    degrees:     List[str]


class Address(BaseModel):
    line1: Optional[str] = None
    line2: Optional[str] = None
    city: Optional[str]  = None
    state: Optional[str] = None
    zip_: Optional[str]  = Field(None, alias='zip')

class DoctorInfo(BaseModel):
    phone:                  Optional[str] = None
    fax:                    Optional[str] = None
    website:                Optional[str] = None
    languages:              List[str] = []
    misc:                   Dict[str, str] = {}

    @validator('languages', pre=True)
    def lang_val(cls, lang_str):
        return lang_str.strip().replace(', ', ',').split(',')

    @validator('phone')
    def phone_val(cls, phone_str):
        return phone_str.strip()

    @validator('fax')
    def fax_val(cls, fax_str):
        return fax_str.strip()


class Endocrinologist(BaseModel):
    title: Title

    # TODO
    #job:                    str
    #place:                  str
    #address:                Address

    info:                   Optional[DoctorInfo] = None
    areas_of_concentration: List[str] = []
    board_cert:             Optional[str] = None
    



URL = 'https://www.hormone.org/find-an-endocrinologist/find-an-endocrinologist-results?specialty=Thyroid&country=UNITED+STATES&page=0'

# TODO: Move from Impl test class to pydantic class
class _EndocrinologistImpl:
    # https://regex101.com/r/gZVDIk/1
    TITLE_REGEX = re.compile(
        r'^(.+\.) *([A-Za-z]+?) +(?:((?:(?:[A-Z] )+[A-Z](?= ))|(?:[A-Za-z\-]+)) )?([A-Za-z\- ]+) *(?:, ?([A-Za-z\-,\. ]+))?$'
    )


    def __init__(self, soup: BeautifulSoup):
        self.d = {}
        r = requests.get(URL)

        self.soup = soup

        for field, info in Endocrinologist.__fields__.items():
            try:
                func = getattr(self, f'_get_{field}')
                self.d[field] = func()
            except Exception as e:
                # print(field, e)
                if info.required: 
                    pass
                    #raise e

    def to_endocrinologist(self):
            return Endocrinologist(**self.d)
        



    def _get_title(self) -> Title:
        raw_text = self.soup.find(class_='endocrinologist-list-item__title').get_text()
        fields = self.TITLE_REGEX.search(raw_text).groups()
        prefix, first, middle, last, degrees = fields

        middle  = middle.strip().split(' ') if middle else [] # TODO: make validators
        last    = last.strip().split(' ') if last else []
        degrees = degrees.replace(' ', '').split(',') if degrees else []

        return Title(**{
            'prefix'     : prefix,
            'first_name' : first,
            'middle_name': middle,
            'last_name'  : last,
            'degrees'    : degrees
        })

    def _get_info(self) -> DoctorInfo:
        contact = self.soup.find(class_='endocrinologist-list-item__contact')

        d = {'misc': {}}
        for elem in contact.find_all('p'):
            k = elem.find('strong').get_text()
            v = elem.get_text().strip(k)
            k = k.strip(':').lower()

            # TODO: instead of lower() use pydantic aliases
            if k in DoctorInfo.__fields__ and k != 'misc':
                d[k] = v
            else:
                d['misc'][k] = v

        return DoctorInfo(**d)

    def _get_areas_of_concentration(self) -> List[str]:
        concentrations_elem = None
        for elem in self.soup.find_all(class_='endocrinologist-list-item__area-title'):
            if elem.get_text() == 'Area of Concentration':
                concentrations_elem = elem.parent.find(class_='endocrinologist-list-item__description')
                break

        concentrations = [i for i in concentrations_elem.children if str(i) != '<br/>']

        return concentrations

    def _get_board_cert(self) -> str:
        for elem in self.soup.find_all(class_='endocrinologist-list-item__area-title'):
            if elem.get_text() == 'General Board Certification':
                cert = elem.parent.find(class_='endocrinologist-list-item__area-description').get_text()
                return cert
        

def recursively_get_all_fields(model: Type[BaseModel]) -> List[str]:
    fields = []
    def _recurse(model: Type[BaseModel]) -> None:
        for info in model.__fields__.values():
            if issubclass(info.type_, BaseModel):
                _recurse(info.type_)
            else:
                fields.append(info.name)
    _recurse(model)
    return fields

def flatten(d: Dict) -> Dict[str, str]:
    out = {}
    def _recurse(d: Dict) -> None:
        for k, v in d.items():
            if isinstance(v, dict) and k != 'misc':
                _recurse(v)
            else:
                out[k] = v

    _recurse(d)
    return out

if __name__ == '__main__':
    df = pandas.DataFrame(columns=recursively_get_all_fields(Endocrinologist))
    print(df.columns)
    soup = BeautifulSoup(requests.get(URL).text, features='lxml')
    for elem in soup.find_all(class_='endocrinologist-list-item'):
        try:
            ei = _EndocrinologistImpl(elem).to_endocrinologist()
            print(flatten(ei.dict()))
            df = df.append(flatten(ei.dict()), ignore_index=True)
            df.to_csv('data/endocrinologists.csv')
        except Exception as e:
            print(e) # FIXME
        
        
