import requests
import pandas
from pydantic import BaseModel
from bs4 import BeautifulSoup

from typing import (
    Dict, List, Optional
)

class Title(BaseModel):
    prefix:      str
    first_name:  str
    middle_name: Optional[str] = None
    last_name:   str
    degrees:     List[str]


class Address(BaseModel): #FIXME
    line1: Optional[str] = None
    line2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_: Optional[str] = None

class Endocrinologist(BaseModel):
    title:                  Title

    job:                    str
    place:                  str
    address:                Address

    phone:                  Optional[str] = None
    fax:                    Optional[str] = None
    website:                Optional[str] = None
    languages:              List[str] = []
    areas_of_concentration: List[str]
    board_cert:             str

    misc:                   Dict[str, str] = {}



URL = 'https://www.hormone.org/find-an-endocrinologist/find-an-endocrinologist-results?specialty=Thyroid&country=UNITED+STATES&page=0'

def find_endocrinologists() -> pandas.DataFrame:
    r = requests.get(URL)
    soup = BeautifulSoup(r.text, features='lxml')

    doctors = soup.find_all(class_='endocrinologist-list-item')
    print(len(doctors))

if __name__ == '__main__':
    find_endocrinologists().to_csv(
        '/Users/eab06/Desktop/WJB/PythonProjects/HT_Data/src/GetAllDoctors/DetermineEndocrinologists/endocrinologists.csv'
    )