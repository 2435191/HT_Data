from difflib import SequenceMatcher
import functools
from typing import Optional
import pandas

__doc__ = """Get specialty codes and consolidate data from different sources in basic_data."""

COLUMNS = ['first_name', 'last_name', 'city', 'postal_code', 'state', 'specialty_code']
GENERIC_OPHTHALMOLOGY_CODE = '207W00000X'
GENERIC_ENDOCRINOLOGY_CODE = '207RE0101X'

def _convert_zip9_to_zip5(z: Optional[str]) -> Optional[str]:
    if pandas.isnull(z):
        return
    return z.split('-')[0].zfill(5)

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
        out_dict = {i: out_dict[i] for i in target_cols}
    
        return out_dict

    address_data = in_df.apply(get_first_address_subfield, 1, False, 'expand')
    
    out_df = pandas.concat([out_df, address_data], 1)
    out_df.loc[:, 'specialty_code'] = GENERIC_OPHTHALMOLOGY_CODE
    out_df['zip'] = out_df['zip'].apply(_convert_zip9_to_zip5)

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
    out_df['zipcode'] = out_df['zipcode'].apply(_convert_zip9_to_zip5)
    out_df.loc[:, 'specialty_code'] = GENERIC_ENDOCRINOLOGY_CODE
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
    
    REPLACEMENTS = {
        'Optometry': 'Optometrist',
        'Pediatric Ophthalmology': 'Pediatric Ophthalmology and Strabismus Specialist',
        'OPR': 'Ophthalmic Plastic and Reconstructive Surgery'
    }

    specialty_codes_crosswalk = pandas.read_csv(
        'data/util/specialty_codes.csv', index_col=0, comment='#')\
        .loc[:, ['Code', 'Classification', 'Specialization']]
    
    @functools.lru_cache(maxsize=None)
    def get_codes_from_strs(specialty: Optional[str], threshold: float=0.95) -> Optional[str]: # FIXME
        if pandas.isnull(specialty):
            return
        
        specialty = REPLACEMENTS.get(specialty, specialty)

        for index, row in specialty_codes_crosswalk.iterrows():
            specialty2 = row['Specialization']
            if pandas.isnull(specialty2):
                specialty2 = row['Classification'] 
                if pandas.isnull(specialty2):
                    continue

            similarity = SequenceMatcher(None, specialty2, specialty).ratio()
            if similarity >= threshold:
                return row['Code']

        return
    
    out_df['ZIP'] = out_df['ZIP'].apply(_convert_zip9_to_zip5)

    out_df['specialty_code'] = in_df['AMA_SPECIALITY'].apply(get_codes_from_strs, 1)

    missed_specialties = {
        i for i in \
        in_df.loc[pandas.isnull(out_df['specialty_code']), 'AMA_SPECIALITY'] \
        if not pandas.isnull(i)
    }
    if missed_specialties:
        logger.warning(f"missed AMA specialty names: {missed_specialties}")

    out_df.columns = COLUMNS
    out_df = out_df.drop_duplicates(ignore_index=True).dropna(how='all')
    return out_df

if __name__ == '__main__':
    import logging
    logging.basicConfig()
    logger = logging.getLogger('clean_all')

    all_dfs = []

    for name in ('asoprs', 'endocrinologists', 'tepezza'):

        df = pandas.read_csv(f'data/raw/_{name}_raw.csv')
        if name == 'tepezza':
            df = df.append(pandas.read_csv('data/raw/_tepezza_raw_old.csv'))
        func = globals()[f'clean_{name}']

        out_df = func(df)
        assert out_df.columns.to_list() == COLUMNS
        out_df['src'] = name
        out_df.to_csv(f'data/processed/{name}.csv')
        all_dfs.append(out_df)

    concat = pandas.concat(all_dfs)

    ignore_cols = ['specialty_code', 'src']
    columns_for_drop_duplicates = [i for i in concat.columns if i not in ignore_cols]
    
    full_data = concat\
    .sort_values(
        'last_name', 
        ignore_index=True, 
        key=lambda ser: ser.apply(lambda s: s.strip('"').lower())
    )
    full_data.to_csv('data/processed/all_with_duplicates.csv')

    full_data = full_data\
    .drop_duplicates(
        columns_for_drop_duplicates, 
        ignore_index=True)
    
    full_data.to_csv('data/processed/all.csv')