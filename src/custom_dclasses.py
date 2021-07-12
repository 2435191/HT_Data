# TODO: use pydantic

from typing import Optional, List, Dict
from dataclasses import dataclass, field


def list_default(): return field(default_factory=list)


@dataclass
class Address:
    line1: Optional[str] = None
    line2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_: Optional[str] = None


@dataclass
class Doctor:
    prefix: Optional[str] = None
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    middle_initial: Optional[str] = None
    last_name: Optional[str] = None
    suffix: Optional[str] = None
    orgs: List[str] = list_default()
    phones: List[str] = list_default()
    addresses: List[Address] = list_default()
    other_attrs: List[Dict] = list_default()
