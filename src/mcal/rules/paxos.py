from datetime import time
from zoneinfo import ZoneInfo

from pandas import date_range
from pandas.tseries.offsets import CustomBusinessDay
from pandas_market_calendars import MarketCalendar

mask_sun = CustomBusinessDay(weekmask="Sun")  # type: ignore
sundays = date_range("2020-01-01", "2030-01-01", freq=mask_sun)


class Paxos(MarketCalendar):
    """
    Расписание для крипты в IB.
    """

    regular_market_times = {
        "market_open": ((None, time(16, 1), -1),),
        "market_close": ((None, time(16)),),
    }

    @property
    def name(self):
        return "Paxos"

    @property
    def weekmask(self):
        return "Mon Tue Wed Thu Fri Sun"

    @property
    def special_opens_adhoc(self):
        return [
            (time(3), sundays),
        ]

    @property
    def tz(self):
        return ZoneInfo("US/Eastern")
