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
TIMEOUT_SHORT = 2.0


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


class Timeout(AssertionError):

    """Thrown when a timeout occurs in the `timeout` context manager."""

    def __init__(self, value="Timed Out"):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Timer:
    def __init__(self, timeout: float = TIMEOUT):
        self.timeout = timeout

    def __enter__(self):
        self.start_time = monotonic()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if monotonic() - self.start_time > self.timeout:
            raise Timeout(f"Execution time exceeded {self.timeout} seconds")

    def wait(self, delay: float = 0.001) -> bool:
        sleep(delay)
        if monotonic() - self.start_time > self.timeout:
            raise Timeout(f"Execution time exceeded {self.timeout} seconds")
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


class Results(list):
    """
    TWS возвращает данные частями, асинхронно.
    Results хранит результаты, пока ответ не завершен.
    Ответ завершается специальным сообщением или ошибкой.
    Группа ответов идентифицируется по r_id.
    """

    def __init__(self, r_id: int = 0):
        self.r_id = r_id
        self.finished = False
        self.error = ""

    def finish(self, r_id: int = 0):
        self.finished = True


class IBSync(IBClient):
    """
    Positions
    Executions (execDetails + commissionReport)
    Open + Completed Orders

    TODO: добавить остановку ожидания Results при ошибке с данным r_id.
    """

    def __init__(self):
        super().__init__()
        self._open_orders = Results()
        self._completed_orders = Results()
        self._positions = Results()
        self._executions = Results()
        self._contract_details = Results()
        self._historical_data = Results()
        self._head_timestamp = Results()
        self._next_order_id = Results()

        self._account_info = Results()
        self._portfolio = Results()

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
        r_id = self.r_id
        self._next_order_id = Results(r_id)
        self.reqIds(r_id)
        with Timer(TIMEOUT_SHORT) as t:
            while not self._next_order_id.finished:
                t.wait()
        self.nextValidOrderId += 1
        return self.nextValidOrderId - 1

    def nextValidId(self, orderId):
        self.nextValidOrderId = orderId
        self._next_order_id.finish()

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
            # TODO: сделать универсальный механизм для всех видов данных
            # Остановить запрос, если такой есть
            if reqId > 0 and reqId == self._historical_data.r_id:
                self._historical_data.error = errorString
                self._historical_data.finish()

            if reqId > 0 and reqId == self._open_orders.r_id:
                self._open_orders.error = errorString
                self._open_orders.finish()

            if reqId > 0 and reqId == self._contract_details.r_id:
                self._contract_details.error = errorString
                self._contract_details.finish()

            # Стандартная ошибка слишком длинная
            if errorCode == 502:
                errorString = "Couldn't connect to TWS"

            txt = f"r_id: {reqId}, code: {errorCode}, msg: {errorString}"
            if advancedOrderRejectJson:
                txt += f", '{advancedOrderRejectJson}'"
            log.error(txt)

    ##########################
    ### Account

    def get_account_info(self) -> tuple[dict, list[Position]]:
        if not self.account_id:
            return {}, []

        self._account_info = Results()
        self._portfolio = Results()

        self.reqAccountUpdates(True, self.account_id)

        with Timer() as t:
            while not (self._account_info.finished and self._portfolio.finished):
                t.wait()

        # Представляете, так выглядит отписка!
        self.reqAccountUpdates(False, self.account_id)

        for position in self._portfolio:
            self.qualify_contract(position.contract)

        # Распарсить ответы
        account_fields = {}
        for rec in self._account_info:
            if rec.get("accountName") != self.account_id:
                continue
            if rec.get("currency") not in ["", "USD"]:
                continue
            key = rec.get("key")
            value = rec.get("value")
            if key and value is not None:
                account_fields[key] = value

        return account_fields, list(self._portfolio)

    def updateAccountValue(self, key: str, value: str, currency: str, accountName: str):
        super().updateAccountValue(key, value, currency, accountName)
        # FIXME: для проверки. Убрать по результатам.
        # If an accountReady value of false is returned that
        # means that the IB server is in the process of resetting
        if key == "accountReady":
            log.error(f"accountReady: {value}")
        if not self._account_info.finished and accountName == self.account_id:
            self._account_info.append(
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
        if not self._portfolio.finished and accountName == self.account_id:
            self._portfolio.append(
                Position(
                    contract=contract,
                    amount=position,
                    market_price=marketPrice,
                    market_value=marketValue,
                    av_cost=averageCost,
                    unrealized_pnl=unrealizedPNL,
                    realized_pnl=realizedPNL,
                    account=accountName,
                )
            )

    def accountDownloadEnd(self, accountName: str):
        # print("accountDownloadEnd")
        super().accountDownloadEnd(accountName)
        self._account_info.finish()
        self._portfolio.finish()

    ##########################
    ### Positions

    def get_positions(self) -> list[tuple]:
        self._positions_by_conid = {}
        self._positions = Results()
        self.reqPositions()

        with Timer() as t:
            while not self._positions.finished:
                t.wait()

        for _, contract, _, _ in self._positions:
            self.qualify_contract(contract)

        return list(self._positions)

    def position(
        self, account: str, contract: Contract, position: Decimal, avgCost: float
    ):
        super().position(account, contract, position, avgCost)
        # log.warning(f"position: {contract.conId} {position} {avgCost}")
        if account == self.account_id:
            self._positions_by_conid[contract.conId] = (position, avgCost)
        if not self._positions.finished:
            self._positions.append((account, contract, position, avgCost))

    def positionEnd(self):
        super().positionEnd()
        self._positions.finish()

    ##########################
    ### Orders

    def place_order(self, contract, order):
        r_id = order.orderId
        self._open_orders = Results(r_id)
        self.placeOrder(r_id, contract, order)

        with Timer() as t:
            while not (self._open_orders.finished or self._open_orders):
                t.wait()

        for contract, _, _ in self._open_orders:
            self.qualify_contract(contract)

        if self._open_orders.error:
            raise Exception(self._open_orders.error)
        else:
            return list(self._open_orders)[0]

    def get_orders(self):
        self._open_orders = Results()
        self._completed_orders = Results()

        self.reqAllOpenOrders()  # not a subscription
        self.reqCompletedOrders(apiOnly=False)  # not a subscription

        with Timer() as t:
            while not (self._open_orders.finished and self._completed_orders.finished):
                t.wait()

        # qualify_contract происходят здесь,
        # т.к. openOrder вызывается другим потоком,
        # который заблокируется при любом запросе к IB

        for contract, _, _ in self._open_orders:
            self.qualify_contract(contract)

        for contract, _, _ in self._completed_orders:
            self.qualify_contract(contract)

        for perm_id in self._orders_by_pid.keys():
            _, contract, _ = self._orders_by_pid[perm_id]
            self.qualify_contract(contract)

        return list(self._open_orders + self._completed_orders)

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
        if not self._open_orders.finished:
            self._open_orders.append((contract, order, orderState))
        if order.permId:
            self._orders_by_pid[order.permId] = order, contract, orderState
        else:
            log.error(f"OpenOrder without permId: {order}")

    def completedOrder(self, contract, order, orderState):
        super().completedOrder(contract, order, orderState)
        if not self._completed_orders.finished:
            self._completed_orders.append((contract, order, orderState))
        if order.permId:
            self._orders_by_pid[order.permId] = order, contract, orderState
        else:
            log.error(f"CompletedOrder without permId: {order}")

    def openOrderEnd(self):
        super().openOrderEnd()
        self._open_orders.finish()

    def completedOrdersEnd(self):
        super().completedOrdersEnd()
        self._completed_orders.finish()

    ##########################
    ### Executions

    def get_executions(self):
        self._executions = Results()

        self.reqExecutions(self.r_id, ExecutionFilter())

        with Timer() as t:
            while not self._executions.finished:
                t.wait()

        for contract, _ in self._executions:
            self.qualify_contract(contract)

        return list(self._executions)

    def execDetails(self, reqId, contract, execution):
        # super().execDetails(reqId, contract, execution)
        if not self._executions.finished:
            self._executions.append((contract, execution))

    def execDetailsEnd(self, reqId):
        # super().execDetailsEnd(reqId)
        self._executions.finish(reqId)

    ##########################
    ### Contracts

    def get_contract_details(self, contract: Contract) -> list[ContractDetails]:
        if contract.exchange == "NASDAQ":
            contract.exchange = "ISLAND"
        if contract.primaryExchange == "NASDAQ":
            contract.primaryExchange = "ISLAND"
        r_id = self.r_id

        self._contract_details = Results(r_id)

        messages = []
        self.reqContractDetails(r_id, contract)
        with Timer() as t:
            while not self._contract_details.finished:
                # Здесь у нас свой разбор очереди ответов IB.
                # Обычно очередь разбирается в отдельном потоке,
                # но иногда он занят. get_contract_details нужно
                # уметь вызывать даже из потока обработки очереди.
                try:
                    text = self.msg_queue.get(block=True, timeout=0.2)
                except queue.Empty:
                    pass
                else:
                    fields = read_fields(text)
                    if fields[0] == b"10" and fields[1] == str(r_id).encode():
                        self.decoder.interpret(fields)  # type: ignore
                    elif fields[0] == b"52" and fields[2] == str(r_id).encode():
                        self.decoder.interpret(fields)  # type: ignore
                        break
                    else:
                        messages.append(text)
                t.wait(0.001)  # чтобы поток успел обработать предыдущее

        # Вернуть перехваченные чужие сообщения в очередь
        for message in messages:
            self.msg_queue.put(message, block=True, timeout=0.1)

        return list(self._contract_details)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        # Все результаты кешируются по conId
        c = Contract(**contractDetails.contract.__dict__)
        h = hash(c)
        if h not in self._cached_details:
            # log.warning(f"Save in cache: {c.conId}, {c.symbol}, {c.localSymbol}, h: {h}")
            self._cached_details[h] = [contractDetails]

        if self._contract_details.r_id == reqId and not self._contract_details.finished:
            self._contract_details.append(contractDetails)

    def contractDetailsEnd(self, reqId: int):
        # FIXME: без паузы может получиться так, что finish наступит раньше
        # FIXME: чем закончится обработка contractDetails в другом потоке.
        sleep(0.01)
        if self._contract_details.r_id == reqId:
            self._contract_details.finish()

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

    def qualify_contract(self, contract: Contract) -> Contract:
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

            if contract.exchange == "SMART":
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
        End Date/Time: The date, time, or time-zone entered is invalid.
        The correct format: [yyyymmdd] hh:mm:ss [xx/xxxx]
        E.g.: 20031126 15:59:00 US/Eastern
        If no date is specified, current date is assumed.
        If no time-zone is specified, local time-zone is assumed (deprecated).
        You can also provide yyyymmddd-hh:mm:ss time is in UTC.
        Note that there is a dash between the date and time in UTC notation.
        """
        if contract.secType == "CRYPTO" and data_type == "TRADES":
            data_type = "AGGTRADES"

        r_id = self.r_id
        self._historical_data = Results(r_id)
        self.reqHistoricalData(
            r_id,
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

        with Timer(timeout) as t:
            while not self._historical_data.finished:
                t.wait()

        if self._historical_data.error:
            if "query returned no data" in self._historical_data.error:
                return []
            else:
                raise Exception(self._historical_data.error)
        else:
            return list(self._historical_data)

    def historicalData(self, reqId: int, bar: BarData):
        # super().historicalData(reqId, bar)
        if not self._historical_data.finished and self._historical_data.r_id == reqId:
            self._historical_data.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        # super().historicalDataEnd(reqId, start, end)
        if self._historical_data.r_id == reqId:
            self._historical_data.finish()

    ##########################
    ### Contract head date

    def get_head_timestamp(self, contract, whatToShow="TRADES", useRTH=0, formatDate=2):
        # даты для BID/ASK иногда нет (например, CL.NYMEX)
        self._head_timestamp = Results()
        self.reqHeadTimeStamp(self.r_id, contract, whatToShow, useRTH, formatDate)

        with Timer() as t:
            while not self._head_timestamp.finished:
                t.wait()

        return list(self._head_timestamp)[0]

    def headTimestamp(self, reqId: int, headTimestamp: str):
        if not self._head_timestamp.finished:
            self._head_timestamp.append(headTimestamp)
            self._head_timestamp.finish()
