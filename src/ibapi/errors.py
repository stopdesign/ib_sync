"""
Copyright (C) 2019 Interactive Brokers LLC. All rights reserved. This code is subject to the terms
 and conditions of the IB API Non-Commercial License or the IB API Commercial License, as applicable.
"""


"""
This is the interface that will need to be overloaded by the customer so
that his/her code can receive info from the TWS/IBGW.
"""


class CodeMsgPair:
    def __init__(self, code, msg):
        self.errorCode = code
        self.errorMsg = msg

    def code(self):
        return self.errorCode

    def msg(self):
        return self.errorMsg


ALREADY_CONNECTED = CodeMsgPair(501,	"Already connected.")
CONNECT_FAIL = CodeMsgPair(502, "Couldn't connect to TWS.")
UPDATE_TWS = CodeMsgPair(503, "The TWS is out of date and must be upgraded.")
NOT_CONNECTED = CodeMsgPair(504, "Not connected")
UNKNOWN_ID = CodeMsgPair(505, "Fatal Error: Unknown message id.")
UNSUPPORTED_VERSION = CodeMsgPair(506, "Unsupported version")
BAD_LENGTH = CodeMsgPair(507, "Bad message length")
BAD_MESSAGE = CodeMsgPair(508, "Bad message")
SOCKET_EXCEPTION = CodeMsgPair(509, "Exception caught while reading socket - ")
FAIL_CREATE_SOCK = CodeMsgPair(520, "Failed to create socket")
SSL_FAIL = CodeMsgPair(530, "SSL specific error: ")
INVALID_SYMBOL = CodeMsgPair(579, "Invalid symbol in string - ")
