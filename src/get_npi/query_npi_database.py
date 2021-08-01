from pydantic import BaseModel, validator
import requests
import json
from typing import Dict, Any, Optional

class DoctorQuery(BaseModel):
    # TODO: add taxonomy
    _URL = 'https://npiregistry.cms.hhs.gov/api'
    _VALID_VERSIONS = ('2.0', '2.1')
    _VALID_NPI_TYPES = ('NPI-1', 'NPI-2')

    version: str = '2.1'
    enumeration_type: str = 'NPI-1' # doctors vs. orgs

    first_name: str
    last_name: str
    
    city:  str = ''
    state: str = ''
    postal_code: str = ''
    country_code: str = 'US'

    subspecialty_code: Optional[str] = None


    @validator('version')
    def assert_version_valid(cls, v):
        assert v in cls._VALID_VERSIONS

    @validator('enumeration_type', pre=True)
    def assert_npi_enum_valid(cls, v):
        assert v is None or v in cls._VALID_NPI_TYPES
        return '' if v is None else v


    _PAGE_SIZE_RANGE = range(10, 201)
    _MAX_PAGE_SKIP = 1000
    _MAX_DATA_RETURN = _MAX_PAGE_SKIP + max(_PAGE_SIZE_RANGE)

    def query_result_paged(self, page_size: int = 100, stop_after: int = 1200) -> Dict[str, Any]:
        assert page_size in self._PAGE_SIZE_RANGE, \
            f'invalid page_size of {page_size}: must be in {self._PAGE_SIZE}'
        assert stop_after <= self._MAX_DATA_RETURN, \
            f'invalid stop_after of {stop_after}: must be <= {self._MAX_DATA_RETURN}'
        
        params = self.dict()
        for page_skip in range(0, stop_after+1, page_size):
            params['limit'] = page_size
            params['skip']  = page_skip

            resp = requests.get(self._URL, params=params)
            res = json.loads(resp.text)
            if res['result_count'] == 0:
                return
            yield res

    
    



    


if __name__ == '__main__':
    dq = DoctorQuery(first_name='David', last_name='Bradley')
    for i, val in enumerate(dq.query_result_paged()):
        with open(f'test_{i}.json', 'w+') as f:
            json.dump(val, f, indent=2)

