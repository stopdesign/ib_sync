from datetime import time
from zoneinfo import ZoneInfo

from pandas import date_range
from pandas.tseries.offsets import CustomBusinessDay

mask_mon = CustomBusinessDay(weekmask="Mon")  # type: ignore
mask_fri = CustomBusinessDay(weekmask="Fri")  # type: ignore
mondays = date_range("2020-01-01", "2030-01-01", freq=mask_mon)
fridays = date_range("2020-01-01", "2030-01-01", freq=mask_fri)


from pandas_market_calendars.calendar_registry import CMEGlobexFXExchangeCalendar


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
        # TODO: попробовать описать это через перерыв
        return [
            ((time(16, 15), -1), mondays),
        ]

    @property
    def special_closes_adhoc(self):
        return [
            (time(16), fridays),
        ]
