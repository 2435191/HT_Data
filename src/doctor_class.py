import pandas

from pydantic import (
    BaseModel, Field, validator, ValidationError
)

from typing import (
    List, Dict, Optional, Type, Any
)
    
class Address(BaseModel):
    line1: Optional[str] = None
    line2: Optional[str] = None
    city:  Optional[str] = None
    state: Optional[str] = None
    zip_:  Optional[str] = Field(None, alias='zip')

class DoctorInfo(BaseModel):
    phones:    List[str] = []
    websites:  List[str] = []
    languages: List[str] = []
    misc:      Dict[str, Any] = {}

    class Config:
        anystr_strip_whitespace = True

class Title(BaseModel):
    prefix:      Optional[str] = None
    first_name:  str
    middle_name: Optional[List[str]] = []
    last_name:   List[str]
    degrees:     List[str] = []

    _VALID_PREFIXES = ['Dr.', 'Prof.', 'Mrs.', 'Mr.', 'Ms.']

    @validator('prefix')
    def assert_prefix_is_valid(cls, prefix_str: str) -> str:
        if prefix_str is None:
            return None
        if prefix_str not in cls._VALID_PREFIXES:
            raise ValidationError('must be one of Title._VALID_PREFIXES')
        return prefix_str

class Doctor(BaseModel):
    title:                  Title
    address:                Optional[Address] = None
    info:                   Optional[DoctorInfo] = None
    areas_of_concentration: List[str] = []
    board_cert:             Optional[str] = None
    source:                 Optional[str] = None


    class Config:
        extra = 'ignore'

    @classmethod
    def to_empty_DataFrame(cls) -> pandas.DataFrame:
        fields = recursively_get_all_fields(cls)
        df = pandas.DataFrame(columns=fields)
        return df


    def to_DataFrame(self) -> pandas.DataFrame:
        fields = recursively_get_all_fields(self.__class__)
        df = pandas.DataFrame.from_dict(flatten(self.dict()), orient='index').transpose()
        return df


def recursively_get_all_fields(model_cls: Type[BaseModel]) -> List[str]:
    fields = []

    def _recurse(model_cls: Type[BaseModel]) -> None:
        for info in model_cls.__fields__.values():
            try:
                if issubclass(info.type_, BaseModel):
                    _recurse(info.type_)
                else:
                    fields.append(info.name)
            except TypeError:
                fields.append(info.name)
    _recurse(model_cls)
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
    doc = Doctor(
        title = Title(prefix='Dr.', first_name='Liz', last_name=['Bradley'])
    )
    print(flatten(doc.dict()))
    print(
        doc.to_DataFrame()
    )