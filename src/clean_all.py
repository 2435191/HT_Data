import json
from difflib import SequenceMatcher
import functools
from typing import Optional
import pandas

from get_npi.query_npi_database import DoctorQuery

COLUMNS = ['first_name', 'last_name', 'city', 'postal_code', 'state', 'specialty_code']

def clean_asoprs(in_df: pandas.DataFrame) -> pandas.DataFrame:
    out_df: pandas.DataFrame = in_df.loc[:, ['Full Name_firstName', 'Full Name_lastName']]
    
    all_address_columns   = {col                for col in in_df.columns if 'address' in col.lower()}
    all_address_subfields = {col.split('_')[-1] for col in all_address_columns}

    def get_first_address_subfield(row: pandas.Series):
        out_dict = {i: None for i in all_address_subfields}
        for col in set(row.index).intersection(all_address_columns):
            
            subfield = col.split('_')[-1]

            if not pandas.isnull(value := row[col]) and out_dict[subfield] is None:
                out_dict[subfield] = row[col]

        target_cols = ['city', 'zip', 'state']
        out_list = [out_dict[i] for i in target_cols]
        
        return out_list

    address_data = in_df.apply(get_first_address_subfield, 1, False, 'expand')
    
    out_df = pandas.concat([out_df, address_data], 1)
    out_df.loc[:, 'specialty_code'] = None # TODO

    out_df.columns = COLUMNS
    return out_df

def clean_endocrinologists(in_df: pandas.DataFrame) -> pandas.DataFrame:
    out_df: pandas.DataFrame = in_df.loc[:, [
        'first_name', 
        'last_name',
        'city',
        'zipcode',
        'state'
        ]
    ]
    out_df['last_name'] = out_df['last_name'].apply(
        lambda l: l[1:-1].split(', ')[-1].strip('\'') # FIXME: use better string to list
    )
    out_df.loc[:, 'specialty_code'] = None # TODO
    out_df.columns = COLUMNS
    return out_df

def clean_tepezza(in_df: pandas.DataFrame) -> pandas.DataFrame:
    out_df: pandas.DataFrame = in_df.loc[:, [
            'FIRST_NAME',
            'LAST_NAME',
            'CITY',
            'ZIP',
            'STATE'
        ]
    ]

    #def is_close(s1: str, s2: str, threshold: float = 0.9) -> bool:
        #return SequenceMatcher(None, s1, s2).ratio() >= threshold
    

    specialty_codes_crosswalk = pandas.read_csv(
        'data/specialty_codes.csv', index_col=0, comment='#')\
        .loc[:, ['Code', 'Specialization']]
    
    @functools.lru_cache(maxsize=None)
    def get_codes_from_strs(specialty: Optional[str], threshold=0.95) -> str: # FIXME
        if pandas.isnull(specialty):
            return ''

        for index, row in specialty_codes_crosswalk.iterrows():
            specialty_to_test_against = row['Specialization']
            if pandas.isnull(specialty_to_test_against):
                continue
            similarity = SequenceMatcher(None, specialty_to_test_against, specialty).ratio()
            if similarity >= threshold:
                return row['Code']

        return ''
    
    out_df['specialty_code'] = in_df['AMA_SPECIALITY'].apply(get_codes_from_strs, 1)

    out_df.columns = COLUMNS
    return out_df

if __name__ == '__main__':
    for name in ('asoprs', 'endocrinologists', 'tepezza'):
        df = pandas.read_csv(f'data/_{name}_raw.csv')
        func = globals()[f'clean_{name}']
        out_df = func(df)
        assert out_df.columns.to_list() == COLUMNS
        out_df.to_csv(f'data/{name}.csv')
