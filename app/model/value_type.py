from enum import Enum


class ValueType(Enum):
    VALUE_TYPE_UNSPECIFIED = 0
    BOOL = 1
    INT64 = 2
    DOUBLE = 3
    STRING = 4
    DISTRIBUTION = 5
    MONEY = 6
