import logging
import random
import time
from dataclasses import dataclass
from decimal import Decimal

from timeout_decorator import timeout

from ibapi.common import BarData, TickerId
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import ExecutionFilter

from .client import IBClient

# Логгер для этого файла
log = logging.getLogger("ib_api.ib_sync")
log.setLevel(logging.INFO)

logging.getLogger("ibapi.client").setLevel(logging.WARNING)
logging.getLogger("ibapi.wrapper").setLevel(logging.WARNING)


TIMEOUT = 5
TIMEOUT_HISTORICAL = 150


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

        self._account_info = Results()
        self._portfolio = Results()

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

        # FIXME: Это полная ерунда, но пока другое не придумал.
        if exchange == "QBALGO":
            if symbol[0] == "Z":
                exchange = "CBOT"  # зерновые фьючерсы
            else:
                exchange = "NYMEX"  # нефть, газ

        sid = f"{exchange}_{symbol}"

        if contract.secType == "STK":
            sid = f"{exchange}_{symbol}"

        if contract.secType == "FUT":
            sid = f"{exchange}_{symbol}"
            exp_str = contract.lastTradeDateOrContractMonth
            sid += "_" + exp_str[2:6]

        if contract.secType == "CRYPTO":
            sid = f"{exchange}_{symbol}"

        if contract.secType == "CASH":
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
    ### Errors

    def error(
        self,
        reqId: TickerId,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson="",
    ):
        if errorCode in [2104, 2106, 2158]:
            # Это не ошибки, а сообщения connection is OK
            log.info(errorString)
        else:
            # TODO: сделать универсальный механизм для всех видов данных
            # Остановить запрос, если такой есть
            if reqId > 0 and reqId == self._historical_data.r_id:
                self._historical_data.error = errorString
                self._historical_data.finish()

            # Стандартная ошибка слишком длинная
            if errorCode == 502:
                errorString = "Couldn't connect to TWS"

            txt = f"r_id: {reqId}, code: {errorCode}, msg: {errorString}"
            if advancedOrderRejectJson:
                txt += f", '{advancedOrderRejectJson}'"
            log.error(txt)

    ##########################
    ### Account

    @timeout(TIMEOUT)
    def get_account_info(self) -> tuple[dict, list[Position]]:
        if not self.account_id:
            return {}, []

        self._account_info = Results()
        self._portfolio = Results()

        self.reqAccountUpdates(True, self.account_id)

        while not (self._account_info.finished and self._portfolio.finished):
            time.sleep(0.001)

        # Представляете, так выглядит отписка!
        self.reqAccountUpdates(False, self.account_id)

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
        print("accountDownloadEnd")
        super().accountDownloadEnd(accountName)
        self._account_info.finish()
        self._portfolio.finish()

    ##########################
    ### Positions

    @timeout(TIMEOUT)
    def get_positions(self) -> list[tuple]:
        self._positions_by_conid = {}
        self._positions = Results()
        self.reqPositions()

        while not self._positions.finished:
            time.sleep(0.001)

        return list(self._positions)

    def position(
        self, account: str, contract: Contract, position: Decimal, avgCost: float
    ):
        super().position(account, contract, position, avgCost)
        log.warning(f"position: {contract.conId} {position} {avgCost}")
        if account == self.account_id:
            self._positions_by_conid[contract.conId] = (position, avgCost)
        if not self._positions.finished:
            self._positions.append((account, contract, position, avgCost))

    def positionEnd(self):
        super().positionEnd()
        self._positions.finish()

    ##########################
    ### Orders

    @timeout(TIMEOUT)
    def get_orders(self):
        self._open_orders = Results()
        self._completed_orders = Results()

        self.reqAllOpenOrders()  # not a subscription
        self.reqCompletedOrders(apiOnly=False)  # not a subscription

        while not (self._open_orders.finished and self._completed_orders.finished):
            time.sleep(0.001)

        return list(self._open_orders + self._completed_orders)

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

    @timeout(TIMEOUT)
    def get_executions(self):
        self._executions = Results()
        self.reqExecutions(self.r_id, ExecutionFilter())

        while not self._executions.finished:
            time.sleep(0.001)

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

    @timeout(TIMEOUT)
    def get_contract_details(self, contract: Contract) -> list[ContractDetails]:
        self._contract_details = Results()
        if contract.exchange == "NASDAQ":
            contract.exchange = "ISLAND"
        if contract.primaryExchange == "NASDAQ":
            contract.primaryExchange = "ISLAND"
        self.reqContractDetails(self.r_id, contract)
        while not self._contract_details.finished:
            time.sleep(0.001)
        return list(self._contract_details)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        # super().contractDetails(reqId, contractDetails)
        if not self._contract_details.finished:
            self._contract_details.append(contractDetails)

    def contractDetailsEnd(self, reqId: int):
        # super().contractDetailsEnd(reqId)
        self._contract_details.finish()

    ##########################
    ### History

    # FIXME: для загрузки старой истории нужен большой timeout
    # FIXME: для tradis нужен обычный timeout

    @timeout(TIMEOUT)
    def get_historical_data(
        self,
        contract: Contract,
        end_dt: str,
        duration: str = "300 S",
        bar_size: str = "1 min",
        data_type: str = "TRADES",
        use_rth: int = 0,
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

        while not self._historical_data.finished:
            time.sleep(0.001)

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

    @timeout(TIMEOUT)
    def get_head_timestamp(self, contract, whatToShow="TRADES", useRTH=0, formatDate=2):
        # даты для BID/ASK иногда нет (например, CL.NYMEX)
        self._head_timestamp = Results()
        self.reqHeadTimeStamp(self.r_id, contract, whatToShow, useRTH, formatDate)

        while not self._head_timestamp.finished:
            time.sleep(0.001)

        return list(self._head_timestamp)[0]

    def headTimestamp(self, reqId: int, headTimestamp: str):
        if not self._head_timestamp.finished:
            self._head_timestamp.append(headTimestamp)
            self._head_timestamp.finish()
