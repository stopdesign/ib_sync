from datetime import time
from zoneinfo import ZoneInfo

from pandas import date_range
from pandas_market_calendars.exchange_calendar_ice import ICEExchangeCalendar

mondays = date_range("2020-01-01", "2030-01-01", freq="W-MON")


class ICEUSFutures(ICEExchangeCalendar):
    aliases = []
    regular_market_times = {
        "market_open": ((None, time(20), -1),),  # offset by -1 day
        "market_close": ((None, time(17)),),
    }

    @property
    def name(self):
        return "ICEUSFutures"

    @property
    def tz(self):
        return ZoneInfo("US/Eastern")

    @property
    def special_opens_adhoc(self):
        return [
            ((time(18), -1), mondays),
        ]
