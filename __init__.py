import json
import websocket
import _thread as thread
from .parser import parse_message
from pprint import pprint

class Pyfi:
    def __init__(self, symbols, on_data):
        self.subscription = { "subscribe": symbols }
        self.thread = 0
        self.on_data = on_data
        self.symbol_data = {}
        self.socket = websocket.WebSocketApp("wss://streamer.finance.yahoo.com/", on_message=self.on_message, on_error=self.on_error, on_close=self.on_close)
        self.socket.on_open = self.on_open
        self.socket.run_forever()

    def on_message(self, socket, message):
        flag = 0
        symbol_data = parse_message(message)
        for k,v in symbol_data.items():
            if k not in self.symbol_data:
                self.symbol_data = symbol_data
                flag = 1
            else:
                if self.symbol_data[k] != v:
                    self.symbol_data = symbol_data
                    flag = 1

        if flag == 1:
            self.on_data(socket, thread, symbol_data)

    def on_error(self, socket, error):
        print(error)

    def on_close(self, socket, *args):
        print("connection closed.")

    def on_open(self, socket):
        def run(*args):
            socket.send(json.dumps(self.subscription))

        self.thread = thread.start_new_thread(run, ())

if __name__ == '__main__':
    p = Pyfi(symbols=["TSLA"], on_data=None)
