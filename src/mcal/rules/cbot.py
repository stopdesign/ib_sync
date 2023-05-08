from datetime import time
from zoneinfo import ZoneInfo

from pandas_market_calendars.exchange_calendar_cme import CMEAgricultureExchangeCalendar


class CBOTGrainFutures(CMEAgricultureExchangeCalendar):
    aliases = []
    regular_market_times = {
        "market_open": ((None, time(19), -1),),
        "market_close": ((None, time(13, 20)),),
        "break_start": ((None, time(7, 45)),),
        "break_end": ((None, time(8, 30)),),
    }

    @property
    def name(self):
        return "CBOTGrainFutures"

    @property
    def tz(self):
        return ZoneInfo("America/Chicago")


class CBOTRiseFutures(CMEAgricultureExchangeCalendar):
    aliases = []
    regular_market_times = {
        "market_open": ((None, time(19), -1),),
        "market_close": ((None, time(13, 20)),),
        "break_start": ((None, time(21, 00), -1),),
        "break_end": ((None, time(8, 30)),),
    }

    @property
    def name(self):
        return "CBOTRiseFutures"

    @property
    def tz(self):
        return ZoneInfo("America/Chicago")
