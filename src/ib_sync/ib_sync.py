import inspect
import logging
import queue
import random
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from time import monotonic, sleep

from ibapi.comm import read_fields
from ibapi.common import BarData, TickerId
from ibapi.contract import Contract as IbContract
from ibapi.contract import ContractDetails
from ibapi.execution import ExecutionFilter

from .client import IBClient

# Логгер для этого файла
log = logging.getLogger("ib_api.ib_sync")
log.setLevel(logging.INFO)

logging.getLogger("ibapi.utils").setLevel(logging.WARNING)
logging.getLogger("ibapi.client").setLevel(logging.WARNING)
logging.getLogger("ibapi.wrapper").setLevel(logging.WARNING)


TIMEOUT = 5.0


def dataclassNonDefaults(obj) -> dict:
    """
    For a ``dataclass`` instance get the fields that are different from the
    default values and return as ``dict``.
    """
    if not is_dataclass(obj):
        raise TypeError(f"Object {obj} is not a dataclass")
    values = [getattr(obj, field.name) for field in fields(obj)]
    return {
        field.name: value
        for field, value in zip(fields(obj), values)
        if value != field.default
        and value == value
        and not (isinstance(value, list) and value == [])
    }


@dataclass
class Contract(IbContract):
    secType: str = ""
    conId: int = 0
    symbol: str = ""
    lastTradeDateOrContractMonth: str = ""
    strike: float = 0.0
    right: str = ""
    multiplier: str = ""
    exchange: str = ""
    primaryExchange: str = ""
    currency: str = ""
    localSymbol: str = ""
    tradingClass: str = ""
    includeExpired: bool = False
    secIdType: str = ""
    secId: str = ""
    description: str = ""
    issuerId: str = ""
    comboLegsDescrip: str = ""
    comboLegs: None = None
    deltaNeutralContract: None = None

    def __eq__(self, other):
        if not isinstance(other, Contract):
            return False
        if bool(self.conId) and self.conId == other.conId:
            return True
        return hash(self) == hash(other)

    def __hash__(self):
        if self.conId:
            # CONTFUT gets the same conId as the front contract, invert it here
            h = self.conId if self.secType != "CONTFUT" else -self.conId
        else:
            self_str = (
                "{secType}-{conId}-{symbol}-{lastTradeDateOrContractMonth}-"
                "{exchange}-{primaryExchange}-{currency}-{localSymbol}"
            ).format(**self.__dict__)
            h = hash(self_str)
        # log.info(f"conid, {self.conId}, h: {h}")
        return int(h)

    def __repr__(self):
        # attrs = self.__dict__
        attrs = dataclassNonDefaults(self)
        attrs.pop("details", "")
        attrs.pop("tradingClass", "")
        clsName = self.__class__.__qualname__
        kwargs = ", ".join(f"{k}={v!r}" for k, v in attrs.items())
        return f"{clsName}({kwargs})"

    __str__ = __repr__


class Timer:
    def __init__(self, timeout: float = TIMEOUT):
        stack = inspect.stack()
        fn = stack[1][3]
        if fn == "wait" and len(stack) > 2:
            fn = stack[2][3]
        self.err = f"Timeout {timeout} sec in '{fn}' function"
        self.timeout = timeout

    def __enter__(self):
        self.start_time = monotonic()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type and exc_type.__name__ == "TimeoutError":
            return
        if monotonic() - self.start_time > self.timeout:
            raise TimeoutError(self.err)

    def wait(self, delay: float = 0.001) -> bool:
        sleep(delay)
        if monotonic() - self.start_time > self.timeout:
            raise TimeoutError(self.err)
        return True


def parse_futures_ticker(local_symbol, decade=None) -> tuple[str, date]:
    symbol = local_symbol[:-2]
    year = int(local_symbol[-1])
    months = "FGHJKMNQUVXZ"
    if decade:
        year += decade
    else:
        # 2017, 2018, 2019, 2020, 2021, ... 2026
        if year <= 6:
            year += 2020
        else:
            year += 2010
    month = months.index(local_symbol[-2]) + 1
    expiration = datetime(year, month, 10).date()
    return symbol, expiration


@dataclass
class Position:
    account: str
    contract: Contract
    amount: Decimal
    av_cost: float
    market_price: float = float("NaN")
    market_value: float = float("NaN")
    unrealized_pnl: float = float("NaN")
    realized_pnl: float = float("NaN")


class Results:
    """
    list с фильтрами.
    """

    def __init__(self) -> None:
        self.requests_by_id: dict[int, Request] = {}

    def request(self, type: str, id: int = 0):
        # Удалить слишком старые запросы, если накопились
        if len(self.requests_by_id) > 10:
            old = datetime.utcnow() - timedelta(minutes=10)
            for key, request in list(self.requests_by_id.items()):
                if request.started_at < old:
                    del self.requests_by_id[key]
        # Новый запрос
        if not id:
            id = random.randint(10000000, 99999999)
        # log.info(f"Start {type}, {id}")
        self.requests_by_id[id] = Request(type, id)
        return self.requests_by_id[id]

    def active(self, type: str | None = None) -> list["Request"]:
        return self.filter(type=type, finished=False)

    def get(self, id: int = 0) -> "Request":
        return self.requests_by_id.get(id, Request())

    def finish(self, type: str | None = None) -> None:
        for r in self.filter(type=type):
            r.finish()

    def filter(
        self,
        id: int | None = None,
        finished: bool | None = None,
        type: str | None = None,
        before: datetime | None = None,
    ) -> list["Request"]:
        res = list(self.requests_by_id.values())

        if id is not None:
            res = [o for o in res if o.id == id]

        if finished is not None:
            res = [o for o in res if o.finished == finished]

        if type is not None:
            res = [o for o in res if o.type == type]

        if before is not None:
            res = [o for o in res if o.started_at < before]

        return res


class Request(list):
    """
    TWS возвращает данные частями, асинхронно.
    Results хранит результаты, пока ответ не завершен.
    Ответ завершается специальным сообщением или ошибкой.
    Группа ответов идентифицируется по r_id.
    """

    def __init__(self, type: str = "", id: int = 0):
        self.id: int = id
        self.type: str = type
        self.started_at: datetime = datetime.utcnow()
        self.finished = False
        self.code: int = 0
        self.error: str = ""

    def __str__(self) -> str:
        return (
            f"Request(id={self.id} type={self.type} "
            f"finished={self.finished} len={len(self)})"
        )

    def __repr__(self) -> str:
        return str(self)

    def wait(self, timeout: float = TIMEOUT):
        with Timer(timeout) as t:
            while not self.finished:
                t.wait()

    def finish(self):
        self.finished = True


class IBSync(IBClient):

    def __init__(self):
        super().__init__()

        # Управление результатами запросов
        self.results = Results()

        self._cached_details = {}

        # TODO: приделать протухание
        self._orders_by_pid = {}
        self._positions_by_conid = {}

    @property
    def r_id(self):
        return random.randint(10000000, 99999999)

    def sid_for_contract(self, contract: Contract) -> str:
        exchange = str(contract.primaryExchange or contract.exchange)
        symbol = str(contract.symbol)

        exchange = exchange.replace("ISLAND", "NASDAQ")

        sid = f"{exchange}_{symbol}"

        if contract.secType == "STK":
            sid = f"{exchange}_{symbol}"

        elif contract.secType == "FUT":
            try:
                _, exp_dt = parse_futures_ticker(contract.localSymbol)
                exp_str = exp_dt.strftime("%y%m")
            except:
                exp_str = contract.lastTradeDateOrContractMonth
                if len(exp_str) == 8:
                    exp_dt = datetime.strptime(exp_str, "%Y%m%d").date()
                    # Контракты с датой экспирации в конце месяца
                    # почему-то называются по следующему месяцу.
                    if exp_dt.day > 22:
                        exp_dt += timedelta(days=10)
                    exp_str = exp_dt.strftime("%y%m")
                elif len(exp_str) == 6:
                    exp_dt = datetime.strptime(exp_str, "%Y%m").date()
                    exp_str = exp_dt.strftime("%y%m")
                else:
                    exp_str = exp_str[-4:]

            sid = f"{exchange}_{symbol}_{exp_str}"

        elif contract.secType == "CRYPTO":
            sid = f"{exchange}_{symbol}"

        elif contract.secType == "CASH":
            local_symbol = str(contract.localSymbol)
            sid = f"{exchange}_{local_symbol}"

        return sid.upper()

    def contract_for_sid(self, sid: str) -> Contract:
        """
        Делает IB Contract по строке SID.

        TODO: поддержка "FUT+CONTFUT" и/или "CONTFUT".
        """

        sid = sid.upper()

        contract = Contract()

        # cash
        if "IDEALPRO_" in sid or "." in sid:
            exch_str, security_str = sid.split("_")
            cur_1, cur_2 = security_str.split(".")
            contract.secType = "CASH"
            contract.symbol = cur_1
            contract.exchange = exch_str
            contract.primaryExchange = exch_str
            contract.currency = cur_2

        # crypto
        elif "PAXOS_" in sid:
            exch_str, security_str = sid.split("_")
            contract.secType = "CRYPTO"
            contract.symbol = security_str
            contract.exchange = exch_str
            contract.primaryExchange = exch_str
            contract.currency = "USD"

        # stocks
        elif sid.count("_") == 1:
            exch_str, security_str = sid.split("_")
            exch_str = exch_str.replace("NASDAQ", "ISLAND")
            contract.secType = "STK"
            contract.symbol = security_str
            contract.exchange = "SMART"
            contract.primaryExchange = exch_str
            contract.currency = "USD"

        # futures
        elif sid.count("_") == 2:
            exch_str, security_str, exp_str = sid.split("_")
            contract.secType = "FUT"
            contract.symbol = security_str
            contract.exchange = exch_str
            contract.primaryExchange = exch_str
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = f"20{exp_str}"

        if contract.symbol:
            return contract
        else:
            raise Exception(f"Can't make a contract from SID: {sid}")

    ##########################
    ### Next Order ID

    def get_next_order_id(self):
        request = self.results.request(type="next_order_id")
        self.reqIds(request.id)
        request.wait()
        self.nextValidOrderId += 1
        return self.nextValidOrderId - 1

    def nextValidId(self, orderId):
        self.nextValidOrderId = orderId
        self.results.finish(type="next_order_id")

    ##########################
    ### Errors

    def error(
        self,
        reqId: TickerId,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson="",
    ):
        if errorCode in [2104, 2106, 2107, 2119, 2158, 2100]:
            # Это не ошибки, а сообщения connection is OK и другое
            log.info(errorString)
        else:
            # Если не найден такой запрос, то создается пустой
            request = self.results.get(id=reqId)
            request.code = errorCode
            request.error = errorString
            request.finish()

            # Стандартная ошибка слишком длинная
            if errorCode == 502:
                errorString = "Couldn't connect to TWS"

            txt = f"r_id: {reqId}, code: {errorCode}, msg: {errorString}"
            if advancedOrderRejectJson:
                txt += f", '{advancedOrderRejectJson}'"
            log.error(txt)

    ##########################
    ### Account

    def get_account_info(self, qualify=True) -> tuple[dict, list[Position]]:
        if not self.account_id:
            return {}, []

        request_account = self.results.request("account")
        request_portfolio = self.results.request("portfolio")

        self.reqAccountUpdates(True, self.account_id)

        with Timer() as t:
            while not (request_account.finished and request_portfolio.finished):
                t.wait()

        # Представляете, так выглядит отписка!
        self.reqAccountUpdates(False, self.account_id)

        # qualify_contract занимает приличное время,
        # поэтому иногда лучше выключить
        if qualify:
            for position in request_portfolio:
                self.qualify_contract(position.contract)

        # Распарсить ответы
        account_fields = {}
        for rec in request_account:
            if rec.get("accountName") != self.account_id:
                continue
            if rec.get("currency") not in ["", "USD"]:
                continue
            key = rec.get("key")
            value = rec.get("value")
            if key and value is not None:
                account_fields[key] = value

        return account_fields, list(request_portfolio)

    def updateAccountValue(self, key: str, value: str, currency: str, accountName: str):
        super().updateAccountValue(key, value, currency, accountName)

        # FIXME: для проверки. Убрать по результатам.
        # If an accountReady value of false is returned that
        # means that the IB server is in the process of resetting
        if key == "accountReady":
            log.error(f"accountReady: {value}")

        if accountName != self.account_id:
            return

        if requests := self.results.active(type="account"):
            # Данные добавляются в последний активный запрос
            requests[-1].append(
                {
                    "key": key,
                    "value": value,
                    "currency": currency,
                    "accountName": accountName,
                }
            )

    def updatePortfolio(
        self,
        contract: Contract,
        position: Decimal,
        marketPrice: float,
        marketValue: float,
        averageCost: float,
        unrealizedPNL: float,
        realizedPNL: float,
        accountName: str,
    ):
        super().updatePortfolio(
            contract,
            position,
            marketPrice,
            marketValue,
            averageCost,
            unrealizedPNL,
            realizedPNL,
            accountName,
        )

        if accountName != self.account_id:
            return

        if requests := self.results.active(type="portfolio"):
            p = Position(
                contract=contract,
                amount=position,
                market_price=marketPrice,
                market_value=marketValue,
                av_cost=averageCost,
                unrealized_pnl=unrealizedPNL,
                realized_pnl=realizedPNL,
                account=accountName,
            )
            requests[-1].append(p)

    def accountDownloadEnd(self, accountName: str):
        super().accountDownloadEnd(accountName)
        self.results.finish(type="account")
        self.results.finish(type="portfolio")

    ##########################
    ### Positions

    def get_positions(self) -> list[tuple]:
        self._positions_by_conid = {}
        request = self.results.request(type="positions")
        self.reqPositions()

        request.wait()

        for _, contract, _, _ in request:
            self.qualify_contract(contract)

        return list(request)

    def position(
        self, account: str, contract: Contract, position: Decimal, avgCost: float
    ):
        super().position(account, contract, position, avgCost)

        if account != self.account_id:
            return

        self._positions_by_conid[contract.conId] = (position, avgCost)

        if requests := self.results.active(type="positions"):
            # Данные добавляются в последний активный запрос
            requests[-1].append((account, contract, position, avgCost))

    def positionEnd(self):
        super().positionEnd()
        self.results.finish(type="positions")

    ##########################
    ### Orders

    def place_order(self, contract, order):
        request = self.results.request("place_order", id=order.orderId)

        self.placeOrder(request.id, contract, order)

        with Timer() as t:
            while not (request.finished or len(request) > 0):
                t.wait()

        for contract, _, _ in request:
            self.qualify_contract(contract)

        if request.error:
            # TODO: передать code и msg
            raise Exception(request.error)
        else:
            return list(request)[0]

    def get_orders(self):
        request_open_orders = self.results.request("open_orders")
        self.reqAllOpenOrders()  # not a subscription
        request_open_orders.wait()

        request_completed_orders = self.results.request("completed_orders")
        self.reqCompletedOrders(apiOnly=False)  # not a subscription
        request_completed_orders.wait()

        # qualify_contract происходят здесь,
        # т.к. openOrder вызывается другим потоком,
        # который заблокируется при любом запросе к IB

        for contract, _, _ in request_open_orders:
            self.qualify_contract(contract)

        for contract, _, _ in request_completed_orders:
            self.qualify_contract(contract)

        for perm_id in self._orders_by_pid.keys():
            _, contract, _ = self._orders_by_pid[perm_id]
            self.qualify_contract(contract)

        return list(request_open_orders + request_completed_orders)

    def get_completed_api_orders(self):
        """
        На самом деле не apiOnly, чтобы не испортить "completed_orders".
        """
        request = self.results.request("completed_orders")
        self.reqCompletedOrders(apiOnly=False)  # not a subscription
        request.wait()

        request.wait()

        for contract, _, _ in request:
            self.qualify_contract(contract)

        for perm_id in self._orders_by_pid.keys():
            _, contract, _ = self._orders_by_pid[perm_id]
            self.qualify_contract(contract)

        return list(request)

    def orderStatus(
        self,
        orderId,
        status: str,
        filled: Decimal,
        remaining: Decimal,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ):
        """
        Событие orderStatus приходит, когда меняется статус ордера,
        и после любых изменений ордера. Но событие не содержит ордер
        и контракт. Это добывается из сохраненных событий openOrder.

        В openOrder приходят данные ордера и контракта, но нет
        оставшегося количества.
        """
        if permId and permId in self._orders_by_pid:
            order, contract, state = self._orders_by_pid[permId]
            state.status = status
            self.qualify_contract(contract)  # надеюсь, что это будет из кэша
            self._orders_by_pid[permId] = order, contract, state
        else:
            log.error(
                f"Order not found in cache, perm: {permId}, "
                f"client: {clientId}, oid: {orderId}"
            )

    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, order, orderState)

        # Ответ на place_order можно найти по id
        request = self.results.get(id=orderId)

        if not request.id:
            # Ответ на open_orders - по типу
            if requests := self.results.active(type="open_orders"):
                request = requests[-1]
        request.append((contract, order, orderState))
        if order.permId:
            self._orders_by_pid[order.permId] = order, contract, orderState
        else:
            log.error(f"OpenOrder without permId: {order}")

    def completedOrder(self, contract, order, orderState):
        super().completedOrder(contract, order, orderState)
        if requests := self.results.active(type="completed_orders"):
            # Данные добавляются в последний активный запрос
            requests[-1].append((contract, order, orderState))
        if order.permId:
            self._orders_by_pid[order.permId] = order, contract, orderState
        else:
            log.error(f"CompletedOrder without permId: {order}")

    def openOrderEnd(self):
        super().openOrderEnd()
        self.results.finish(type="open_orders")

    def completedOrdersEnd(self):
        super().completedOrdersEnd()
        self.results.finish(type="completed_orders")

    ##########################
    ### Executions

    def get_executions(self):
        request = self.results.request(type="executions")

        self.reqExecutions(request.id, ExecutionFilter())

        request.wait()

        for contract, _ in request:
            self.qualify_contract(contract)

        return list(request)

    def execDetails(self, reqId, contract, execution):
        # super().execDetails(reqId, contract, execution)
        request = self.results.get(id=reqId)
        if not request.finished:
            request.append((contract, execution))

    def execDetailsEnd(self, reqId):
        # super().execDetailsEnd(reqId)
        self.results.get(id=reqId).finish()

    ##########################
    ### Contracts

    def get_contract_details(self, contract: Contract) -> list[ContractDetails]:
        """
        Метод умеет разбирать очередь сообщений в своем потоке.
        """

        if contract.exchange == "NASDAQ":
            contract.exchange = "ISLAND"
        if contract.primaryExchange == "NASDAQ":
            contract.primaryExchange = "ISLAND"

        request = self.results.request(type="contract_details")

        self.reqContractDetails(request.id, contract)

        with Timer() as t:
            # Разбор очереди сообщений от GW.
            # Обрабатываются только сообщения про контракт.
            while not request.finished:
                try:
                    text = self.msg_queue.get(block=True, timeout=0.2)
                except queue.Empty:
                    pass
                else:
                    f = read_fields(text)
                    # Details
                    if f[0] == b"10" and f[1]:
                        self.decoder.interpret(f)  # type: ignore
                    # DetailsEnd
                    elif f[0] == b"52" and f[2]:
                        self.decoder.interpret(f)  # type: ignore
                    # Другие сообщения возвращаются в очередь
                    else:
                        self.msg_queue.put(text, block=True, timeout=0.1)
                t.wait()

        return list(request)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        # Все результаты кешируются по conId
        c = Contract(**contractDetails.contract.__dict__)
        h = hash(c)
        if h not in self._cached_details:
            self._cached_details[h] = [contractDetails]
        request = self.results.get(id=reqId)
        if request.finished:
            log.error(f"contractDetails request is already finished: {reqId}")
        else:
            request.append(contractDetails)

    def contractDetailsEnd(self, reqId: int):
        sleep(0.01)  # время на обработку contractDetails другим потоком
        self.results.get(id=reqId).finish()

    def _get_cached_details(self, contract: Contract) -> list[ContractDetails]:
        if contract.conId:
            c = Contract(conId=contract.conId)
        else:
            c = Contract(**contract.__dict__)
            c.includeExpired = True

        if hash(c) in self._cached_details:
            return self._cached_details[hash(c)]

        log.warning(f"MISS: {c}, h: {hash(c)}")

        result = self.get_contract_details(c)

        return result

    def qualify_contract(self, contract: Contract, keep_exchange=False) -> Contract:
        """
        IB возвращает контракты с пустыми полями, нужно их заполнять.
        """
        if getattr(contract, "details", None):
            return contract

        details = self._get_cached_details(contract)

        if not details:
            raise ValueError(f"Unknown contract: {contract}")

        elif len(details) > 1:
            v = [d.contract.conId for d in details]
            raise ValueError(f"Ambiguous contract: {contract}, variants: {v}")

        else:
            c = details[0].contract

            if expiry := c.lastTradeDateOrContractMonth:
                # remove time and timezone part as it will cause problems
                expiry = expiry.split()[0]
                c.lastTradeDateOrContractMonth = expiry

            # Выключил по умолчанию. Контракт ZW приходил с биржей SMART.
            # Если будут проблемы, можно добавить параметр keep_exchange.
            if keep_exchange:
                # overwriting 'SMART' exchange can create invalid contract
                c.exchange = contract.exchange

            for k, v in c.__dict__.items():
                setattr(contract, k, v)

            setattr(contract, "details", details[0])

        return contract

    ##########################
    ### History

    def get_historical_data(
        self,
        contract: Contract,
        end_dt: str,
        duration: str = "300 S",
        bar_size: str = "1 min",
        data_type: str = "TRADES",
        use_rth: int = 0,
        timeout: float = TIMEOUT,
    ):
        """
        The correct end_dt format: [yyyymmdd] hh:mm:ss [xx/xxxx]
        E.g.: 20031126 15:59:00 US/Eastern
        If no date is specified, current date is assumed.
        If no time-zone is specified, local time-zone is assumed (deprecated).
        You can also provide yyyymmddd-hh:mm:ss time is in UTC.
        Note that there is a dash between the date and time in UTC notation.
        """
        if contract.secType == "CRYPTO" and data_type == "TRADES":
            data_type = "AGGTRADES"

        request = self.results.request("historical_data")

        self.reqHistoricalData(
            request.id,
            contract,
            endDateTime=end_dt,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=data_type,
            useRTH=use_rth,
            formatDate=2,
            keepUpToDate=False,
            chartOptions=[],
        )

        request.wait(timeout)

        if request.error:
            if "query returned no data" in request.error:
                return []
            else:
                raise Exception(request.error)
        else:
            return list(request)

    def historicalData(self, reqId: int, bar: BarData):
        # super().historicalData(reqId, bar)
        request = self.results.get(id=reqId)
        if not request.finished:
            request.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        # super().historicalDataEnd(reqId, start, end)
        self.results.get(id=reqId).finish()

    ##########################
    ### Contract head date

    def get_head_timestamp(self, contract, whatToShow="TRADES", useRTH=0, formatDate=2):
        # даты для BID/ASK иногда нет (например, CL.NYMEX)
        request = self.results.request("head_timestamp")
        self.reqHeadTimeStamp(request.id, contract, whatToShow, useRTH, formatDate)
        request.wait()
        return list(request)[0]

    def headTimestamp(self, reqId: int, headTimestamp: str):
        request = self.results.get(id=reqId)
        if not request.finished:
            request.append(headTimestamp)
            request.finish()
