from pydantic import BaseModel, validator
from pydantic.error_wrappers import ValidationError
import requests
import logging
import json
from typing import Dict, Any, Optional, List, Iterator
import pandas


class DoctorQuery(BaseModel):
    # TODO: add taxonomy
    _URL = 'https://npiregistry.cms.hhs.gov/api'
    _VALID_VERSIONS = ('2.0', '2.1')
    _VALID_NPI_TYPES = ('NPI-1', 'NPI-2')

    version: str = '2.1'
    enumeration_type: str = 'NPI-1'  # doctors (NPI-1) vs. orgs (NPI-2)

    first_name: str
    last_name: str

    city:  str = ''
    state: str = ''
    postal_code: str = ''
    country_code: str = 'US'

    specialty_code: Optional[str] = None

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

    def query_result_paged(self, page_size: int = 100, stop_after: int = 1200) -> Iterator[Dict[str, Any]]:
        assert page_size in self._PAGE_SIZE_RANGE, \
            f'invalid page_size of {page_size}: must be in {self._PAGE_SIZE}'
        assert stop_after <= self._MAX_DATA_RETURN, \
            f'invalid stop_after of {stop_after}: must be <= {self._MAX_DATA_RETURN}'

        params = self.dict()
        for page_skip in range(0, stop_after+1, page_size):
            params['limit'] = page_size
            params['skip'] = page_skip

            resp = requests.get(self._URL, params=params)
            res = json.loads(resp.text)
            if res['result_count'] == 0:
                return
            yield res


if __name__ == '__main__':
    try:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logging.basicConfig()

        DF_SOURCE = 'data/processed/all.csv'
        DF_DEST = 'data/processed/all_with_npi3.csv'
        OVERWRITE = True

        df: pandas.DataFrame = pandas.read_csv(DF_SOURCE, index_col=0)

        if 'npi' not in df.columns:
            df['npi'] = -1

        # least restrictive to most restrictive query params.
        # multiple elements in a tuple means they are queried together (or not at all)
        DROP_ORDER = (
            ('city',),
            ('postal_code',),
            ('state',),
            ('specialty_code',),
            ('first_name', 'last_name')
        )
        START_IDX = 3  # "bottom" of search

        def get_queries(row: pandas.Series, min_: int, max_: int) -> DoctorQuery:
            param_names = sum(DROP_ORDER[min_: max_], ())
            logger.debug(f'param_names: {param_names}')

            query_params = {k: v for k, v in row.to_dict().items()
                            if k in param_names}
            return DoctorQuery(**query_params)

        for df_index, row in df.iterrows():
            if not OVERWRITE and row['npi'] != -1:
                continue

            logger.debug(f"finding npi for:\n{row}")

            idx = START_IDX
            prev_indices = []

            df.at[df_index, 'npi'] = None
            df.to_csv(DF_DEST)

            while True:
                try:
                    if idx in prev_indices:
                        logger.warning(
                            f'no solution found for:\n{row} (ping ponging concern: idx = {idx}, prev_indices = {prev_indices})')
                        break
                    if idx < 0 or idx >= len(DROP_ORDER):
                        logger.warning(
                            f'no solution found for:\n{row} (exceeded array bounds: idx = {idx})')
                        break

                    prev_indices.append(idx)

                    count = 0
                    result = None

                    try:
                        results = get_queries(row, idx, len(
                            DROP_ORDER)).query_result_paged()
                    except ValidationError:
                        count = 2
                    else:
                        for result in results:
                            count += result['result_count']
                            if count > 1:
                                break

                    if count == 0:
                        idx += 1
                        logger.debug(
                            f'too restrictive, increasing floor to {DROP_ORDER[idx:]} (idx = {idx})')
                    elif count == 1:
                        npi = result['results'][0]['number']
                        logger.info(f'found {npi} for:\n{row}')
                        df.at[df_index, 'npi'] = npi
                        break
                    elif count > 1:
                        idx -= 1
                        logger.debug(
                            f'not restrictive enough, decreasing floor to {DROP_ORDER[idx:]} (idx = {idx})')
                except KeyError as e:
                    logger.critical('exception:', exc_info=e)
    except:
        pass
