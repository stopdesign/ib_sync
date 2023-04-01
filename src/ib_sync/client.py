import logging
import threading
from datetime import datetime

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper
from termcolor import colored, cprint

# Логгер для этого файла
log = logging.getLogger("ib_api.client")
log.setLevel(logging.INFO)


class IBClient(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.order_status = {}
        self.nextValidOrderId = -1
        self.account_id = None
        self.values = {}
        self.tws_time = datetime.min

    def connectAck(self):
        """
        Callback initially acknowledging connection attempt.
        Connection handshake not complete until nextValidID is received.
        """
        super().connectAck()
        log.warn("Connection attempt...")

    def connectionClosed(self):
        super().connectionClosed()
        self.nextValidOrderId = -1
        log.warn("connectionClosed")

    def nextValidId(self, orderId):
        # print("Next Valid Id", orderId)
        self.nextValidOrderId = orderId

    def managedAccounts(self, accountsList:str):
        """Receives a comma-separated string with the managed account ids."""
        super().managedAccounts(accountsList)
        self.account_id = accountsList.split(",")[0]
        self.values[self.account_id] = {}
        log.info(f"Managed account: {self.account_id}")

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        print(f'Order status updated for order ID {orderId}: status={status}, filled={filled}, remaining={remaining}, permId={permId}')

    def updateAccountValue(self, key, value, currency, accountName):
        self.values[accountName][key] = value
        # cprint('Account value updated: key={}, value={}, currency={}, accountName={}'.format(key, value, currency, accountName), "blue")

    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        cprint('Portfolio updated: contract={}, position={}, marketPrice={}, marketValue={}, averageCost={}, unrealizedPNL={}, realizedPNL={}, accountName={}'.format(contract.conId, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName), "red")

    def updateAccountTime(self, timestamp: str):
        pass
        # super().updateAccountTime(timestamp)
        # TODO: проверить, можно ли использовать для группировки приходящих updateAccountValue
        # cprint(f"AccountTime: {timestamp}", "yellow")

    def orderBound(self, orderId: int, apiClientId: int, apiOrderId: int):
        super().orderBound(orderId, apiClientId, apiOrderId)
        cprint(f"OrderBound. OrderId: {orderId}, ApiClientId: {apiClientId}, ApiOrderId: {apiOrderId}", "blue")

    def currentTime(self, time):
        self.tws_time = datetime.utcfromtimestamp(time)
        # print(f'Current TWS time: {self.tws_time} UTC')

    def pnl(self, reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float):
        cprint(f"Account dailyPnL: {dailyPnL}, unrealizedPnL: {unrealizedPnL}, realizedPnL: {realizedPnL}", "cyan")

    def accountSummary(self, reqId, account, tag, value, currency):
        super().accountSummary(reqId, account, tag, value, currency)
        dictionary = {"ReqId":reqId, "Account": account, "Tag": tag, "Value": value, "Currency": currency}
        cprint(f"Account Summary: {dictionary}", "magenta")

    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        print('//// accountSummary\n')

    def historicalData(self, reqId:int, bar):
        cprint(f"HistoricalData. BarData: {bar}", "blue")

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        cprint(f"HistoricalDataEnd. ReqId: {reqId} from {start} to {end}", "red")

    def historicalDataUpdate(self, reqId: int, bar):
        cprint(f"HistoricalDataUpdate. BarData: {bar}", "magenta")

    def commissionReport(self, commissionReport):
        # super().commissionReport(commissionReport)
        pass


class IBThread(threading.Thread):
    def __init__(self, app):
        self.app = app
        super().__init__(target=self.run, daemon=True)

    def run(self):
        log.info("Run Message Thread")
        self.app.run()


# Фабрика контрактов
def IbContract(
        symbol,
        secType="STK",
        exchange="SMART",
        currency="USD",
        localSymbol="",
    ):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = secType
    contract.exchange = exchange
    contract.currency = currency
    if secType == "FUT":
        contract.localSymbol = localSymbol
    return contract
