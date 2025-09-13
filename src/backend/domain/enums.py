from enum import Enum

STATE_ORDER = [
    "NEW",
    "CURRENT",
    "REACTIVATED",
    "RESURRECTED",
    "AT_RISK_WAU",
    "AT_RISK_MAU",
    "DORMANT",
]


class State(str, Enum):
    NEW = "NEW"
    CURRENT = "CURRENT"
    REACTIVATED = "REACTIVATED"
    RESURRECTED = "RESURRECTED"
    AT_RISK_WAU = "AT_RISK_WAU"
    AT_RISK_MAU = "AT_RISK_MAU"
    DORMANT = "DORMANT"
