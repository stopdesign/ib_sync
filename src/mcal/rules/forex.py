from datetime import time
from zoneinfo import ZoneInfo

from pandas import date_range
from pandas_market_calendars.calendar_registry import CMEGlobexFXExchangeCalendar

mondays = date_range("2020-01-01", "2030-01-01", freq="W-MON")
fridays = date_range("2020-01-01", "2030-01-01", freq="W-FRI")


class Forex(CMEGlobexFXExchangeCalendar):
    """
    Расписание Forex.
    """

    aliases = []

    regular_market_times = {
        "market_open": ((None, time(16, 15), -1),),
        "market_close": ((None, time(16)),),
    }

    @property
    def name(self):
        return "Forex"

    @property
    def tz(self):
        return ZoneInfo("America/Chicago")


class ForexEURNZD(Forex):
    """
    Расписание Forex для пары EUR/NZD в IBKR.
    """

    aliases = []

    regular_market_times = {
        "market_open": ((None, time(14, 15), -1),),
        "market_close": ((None, time(14)),),
    }

    @property
    def name(self):
        return "ForexEURNZD"

    @property
    def tz(self):
        return ZoneInfo("America/Chicago")

    @property
    def special_opens_adhoc(self):
        return [
            ((time(16, 15), -1), mondays),
        ]

    @property
    def special_closes(self):
        """
        USThanksgivingFriday конфликтует с special_closes_adhoc,
        поэтому приходится очистить special_closes.
        """
        return []

    @property
    def special_closes_adhoc(self):
        return [
            (time(16), fridays),
        ]
