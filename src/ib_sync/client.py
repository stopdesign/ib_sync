import logging
import threading
from datetime import datetime

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

# Логгер для этого файла
log = logging.getLogger("ib_api.client")
log.setLevel(logging.INFO)


class IBClient(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextValidOrderId: int = -1
        self.account_id: str = ""
        self.values: dict = {}
        self.tws_time: datetime = datetime.min

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
        self.nextValidOrderId = orderId

    def managedAccounts(self, accountsList: str):
        """
        Receives a comma-separated string with the managed account ids.
        """
        super().managedAccounts(accountsList)
        self.account_id = accountsList.split(",")[0]
        # Это событие приходит после соединения.
        # Сбрасываю старые значения аккаунта.
        self.values[self.account_id] = {}
        log.info(f"Managed account: {self.account_id}")

    def updateAccountValue(self, key, value, currency, accountName):
        self.values[accountName][key] = value

    def orderBound(self, orderId: int, apiClientId: int, apiOrderId: int):
        super().orderBound(orderId, apiClientId, apiOrderId)
        log.error(
            f"OrderBound. OrderId: {orderId}, "
            f"ApiClientId: {apiClientId}, "
            f"ApiOrderId: {apiOrderId}"
        )

    def currentTime(self, time):
        self.tws_time = datetime.utcfromtimestamp(time)


class IBThread(threading.Thread):
    def __init__(self, app):
        self.app = app
        super().__init__(target=self.run, daemon=True, name="IBThread")

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
