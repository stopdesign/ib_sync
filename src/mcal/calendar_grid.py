from datetime import datetime, timedelta, timezone

import pandas_market_calendars as mcal

from .rules import *

IBKR_TO_MCAL = {
    "ARCA": "NYSE",
    "CBOT": "CBOTGrainFutures",
    "CME": "CME_Rate",
    "GLOBEX": "CME_Rate",
    "COMEX": "CMEGlobex_EnergyAndMetals",
    "FOREX": "Forex",
    "IDEALPRO": "Forex",
    "NASDAQ": "NASDAQ",
    "NYMEX": "CME_Rate",
    "NYSE": "NYSE",
    "PAXOS": "Paxos",
    "NYBOT": "ICEUS",
}

# for n in mcal.get_calendar_names():
#     print(n)


def get_mcal_exchange(sid: str) -> str:
    exchange, rest = sid.split("_", 1)
    symbol = rest.split("_")[0]
    if exchange == "CBOT" and symbol == "ZR":
        exchange = "CBOTRiseFutures"
    elif exchange == "NYMEX" and symbol == "NG":
        exchange = "CMEGlobex_NG"
    elif symbol == "EUR.NZD":
        exchange = "ForexEURNZD"
    elif exchange in IBKR_TO_MCAL:
        exchange = IBKR_TO_MCAL[exchange]
    return exchange


class CalendarGrid:
    """
    Строит сетку для периода, кэширует сетки для указанных инструментов.
    Проверяет is_rth по сетке.
    """

    def __init__(self, sids: list[str], dt_1: datetime, dt_2: datetime | None = None):
        self.dt_1 = dt_1 - timedelta(days=15)
        if dt_2:
            self.dt_2 = dt_2 + timedelta(days=15)
        else:
            self.dt_2 = datetime.today() + timedelta(days=300)
        self.grids = self._init_grids(sids)

    def _init_grids(self, sids: list[str]) -> dict:
        res = {}
        for sid in sids:
            exchange = get_mcal_exchange(sid)
            if exchange in res:
                continue
            calendar = self.get_calendar(sid)
            res[exchange] = self._get_grid(calendar)
        return res

    def _get_grid(self, calendar: mcal.MarketCalendar) -> set[int]:
        """
        Рабочие минутные интервалы от dt_1 до dt_2 в виде списка timestamps.
        """
        # Расписание нужной биржи (все доступные интервалы)
        schedule = calendar.schedule(self.dt_1, self.dt_2)  # market_times="all"

        # Минутные интервалы ETH
        # times = calendar.regular_market_times
        # if "pre" in times and "post" in times:
        #     schedule[["market_open", "market_close"]] = schedule[["pre", "post"]]

        # Минутные интервалы RTH
        open = mcal.date_range(schedule, "1T", force_close=True)

        # Смещение на одну минуту нужно, чтобы интервал
        # HH:00 был как следующие интервалы этого часа
        res = {dt - 60 for dt in set(open.view("int64") // 10**9)}

        return res

    def get_calendar(self, sid: str) -> mcal.MarketCalendar:
        exchange = get_mcal_exchange(sid)
        return mcal.get_calendar(exchange)  # type: ignore

    def is_rth(self, sid: str, dt: datetime) -> bool:
        exchange = get_mcal_exchange(sid)
        dt = dt.replace(second=0).replace(tzinfo=timezone.utc)
        return int(dt.timestamp()) in self.grids[exchange]
