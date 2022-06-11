import time
from threading import Thread

from .frame import Frame
import websocket
import logging
import ssl

VERSIONS = '1.0,1.1,1.2'

logger = logging.getLogger(__name__)

class Client:

    def __init__(self, url, header = None):

        self.url = url
        self.header = header
        self.ws = websocket.WebSocketApp(self.url)
        self.ws.header = self.header
        self.ws.on_open = self._on_open
        self.ws.on_message = self._on_message
        self.ws.on_error = self._on_error
        self.ws.on_close = self._on_close
        self.ws.on_ping = self._on_ping
        self.ws.on_pong = self._on_pong

        self.opened = False

        self.connected = False

        self.counter = 0
        self.subscriptions = {}

        self._connectCallback = None
        self.errorCallback = None

    def _connect(self, timeout=0, ping_interval=0, ping_timeout=None, verify_ssl=True):
        thread = Thread(target=self.ws.run_forever, kwargs={
            "ping_interval": ping_interval,
            "ping_timeout": ping_timeout,
            "sslopt": None if verify_ssl else {
                "cert_reqs": ssl.CERT_NONE,
                "check_hostname": False
            }
        })
        thread.daemon = True
        thread.start()

        total_ms = 0
        while self.opened is False:
            time.sleep(.25)
            total_ms += 250
            if 0 < timeout < total_ms:
                raise TimeoutError(f"Connection to {self.url} timed out")

    def _on_open(self, ws):
        self.opened = True

    def _on_close(self, ws, close_status_code, close_msg):
        # prevent infinite wait on _connect if there is a connection error
        if not self.opened:
            self.opened = True
        self.connected = False
        logger.debug("Whoops! Lost connection to " + self.ws.url)
        self._clean_up()

    def _on_error(self, ws, error):
        # prevent infinite wait on _connect if there is a connection error
        if not self.opened:
            self.opened = True
        logger.debug(error)

    def _on_ping(self, ws, data):
        self.ws.send('pong')

    def _on_pong(self, ws, data):
        # Outgoing ping received
        logger.debug(">>> PING")

    def _on_message(self, ws, message):
        if message == '\n':
            # Incoming ping
            logger.debug("<<< PONG")
            self.pingCallback(message)
            return
        logger.debug("\n<<< " + str(message))
        frame = Frame.unmarshall_single(message)
        _results = []
        if frame.command == "CONNECTED":
            self.connected = True
            logger.debug("connected to server " + self.url)
            if self._connectCallback is not None:
                _results.append(self._connectCallback(frame))
        elif frame.command == "MESSAGE":

            subscription = frame.headers['subscription']

            if subscription in self.subscriptions:
                onreceive = self.subscriptions[subscription]
                messageID = frame.headers['message-id']

                def ack(headers):
                    if headers is None:
                        headers = {}
                    return self.ack(messageID, subscription, headers)

                def nack(headers):
                    if headers is None:
                        headers = {}
                    return self.nack(messageID, subscription, headers)

                frame.ack = ack
                frame.nack = nack

                _results.append(onreceive(frame))
            else:
                info = "Unhandled received MESSAGE: " + str(frame)
                logger.debug(info)
                _results.append(info)
        elif frame.command == 'RECEIPT':
            pass
        elif frame.command == 'ERROR':
            if self.errorCallback is not None:
                _results.append(self.errorCallback(frame))
        else:
            info = "Unhandled received MESSAGE: " + frame.command
            logger.debug(info)
            _results.append(info)

        return _results

    def _transmit(self, command, headers, body=None):
        out = Frame.marshall(command, headers, body)
        logger.debug("\n>>> " + out)
        self.ws.send(out)

    def connect(self, login=None, passcode=None, host=None, headers=None, connectCallback=None, 
                errorCallback=None, pingCallback=None, connect_timeout=0,
                ping_interval=0, ping_timeout=None, verify_ssl=True):

        logger.debug("Opening web socket...")
        self._connect(connect_timeout, ping_interval, ping_timeout, verify_ssl=verify_ssl)

        headers = headers if headers is not None else {}
        headers['host'] = host if host is not None else self.url
        headers['accept-version'] = VERSIONS
        headers['heart-beat'] = f'{ping_interval*1000},{ping_interval*1000}'

        if login is not None:
            headers['login'] = login
        if passcode is not None:
            headers['passcode'] = passcode

        self._connectCallback = connectCallback
        self.errorCallback = errorCallback
        self.pingCallback = pingCallback

        self._transmit('CONNECT', headers)

    def disconnect(self, disconnectCallback=None, headers=None):
        if headers is None:
            headers = {}

        subscription_ids = list(self.subscriptions.keys())
        for id in subscription_ids:
            self.unsubscribe(id)

        self._transmit("DISCONNECT", headers)
        self.ws.on_close = None
        self.ws.close()
        self._clean_up()

        if disconnectCallback is not None:
            disconnectCallback()

    def _clean_up(self):
        self.connected = False

    def send(self, destination, headers=None, body=None):
        if headers is None:
            headers = {}
        if body is None:
            body = ''
        headers['destination'] = destination
        return self._transmit("SEND", headers, body)

    def subscribe(self, destination, callback=None, headers=None):
        if headers is None:
            headers = {}
        if 'id' not in headers:
            headers["id"] = "sub-" + str(self.counter)
            self.counter += 1
        headers['destination'] = destination
        self.subscriptions[headers["id"]] = callback
        self._transmit("SUBSCRIBE", headers)

        def unsubscribe():
            self.unsubscribe(headers["id"])

        return headers["id"], unsubscribe

    def unsubscribe(self, id):
        del self.subscriptions[id]
        return self._transmit("UNSUBSCRIBE", {
            "id": id
        })

    def ack(self, message_id, subscription, headers):
        if headers is None:
            headers = {}
        headers["message-id"] = message_id
        headers['subscription'] = subscription
        return self._transmit("ACK", headers)

    def nack(self, message_id, subscription, headers):
        if headers is None:
            headers = {}
        headers["message-id"] = message_id
        headers['subscription'] = subscription
        return self._transmit("NACK", headers)
