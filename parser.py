import base64
from struct import *
from pprint import pprint

QUOTE = {
    0: "NONE",
    5: "ALTSYMBOL",
    7: "HEARTBEAT",
    8: "EQUITY",
    9: "INDEX",
    11: "MUTUALFUND",
    12: "MONEYMARKET",
    13: "OPTION",
    14: "CURRENCY",
    15: "WARRANT",
    17: "BOND",
    18: "FUTURE",
    20: "ETF",
    23: "COMMODITY",
    28: "ECNQUOTE",
    41: "CRYPTOCURRENCY",
    42: "INDICATOR",
    1000: "INDUSTRY",
}

MARKETHOURS = {
    0: 'PRE_MARKET',
    1: 'REGULAR_MARKET',
    2: 'POST_MARKET',
    3: 'EXTENDED_HOURS_MARKET',
}

# sint64 -> get value, convert binary, shave off LSB, convert decimal
# sint64 expireDate = 14;
# float openPrice = 15;
# float previousClose = 16;
# string underlyingSymbol = 18;
# sint64 openInterest = 19;
# OptionType optionsType = 20;
# sint64 miniOption = 21;
# sint64 bidSize = 24;
# sint64 askSize = 26;
# sint64 vol_24hr = 28;
# string fromcurrency = 30;
# string lastMarket = 31;
# double circulatingSupply = 32;
# double marketcap = 33;

def parse_message(message):
    binary = base64.b64decode(message)
    _bytes = bytearray(binary)
    symbol_data = {}

    while len(_bytes) > 0:
        msg = _bytes[0] >> 3

        if msg == 0:
            break
        if msg not in switch:
            print(msg, _bytes, 'not implemented')
            break

        _bytes = shift(_bytes, [1])
        data = switch[msg](_bytes)
        symbol_data[data['key']] = data['val']
        _bytes = data['bytes']

    return symbol_data

def shift(_bytes, offset):
    if len(offset) > 1:
        return bytearray(_bytes[offset[0]:offset[1]:])
    return bytearray(_bytes[offset[0]::])

def parse_sint64(buffer):
    bucket = [bin(i) for i in buffer]
    bstr = "1"
    for b in bucket[::-1]:
        bstr += b[3::]
    bstr = bstr[0:len(bstr)-1]
    return int(bstr,2)

def get_symbol(message):
    size = message[0]
    symbol = ""
    for b in unpack(size*'c', message[1:size+1]):
        symbol += b.decode("utf-8")

    message = shift(message, [size+1])
    return {'key':"symbol", 'val': symbol, 'bytes': message}

def get_price(message):
    price = unpack('f', message[0:4:])[0]
    message = shift(message, [4])
    return {'key':"price", 'val': price, 'bytes': message}

def get_time(message):
    time = parse_sint64(message[0:6])
    message = shift(message, [6])
    return {'key':"time", 'val': time, 'bytes': message}

def get_currency(message):
    size = message[0]
    currency = ""
    for b in unpack(size*'c', message[1:size+1]):
        currency += b.decode("utf-8")

    message = shift(message, [size+1])
    return {'key':"currency", 'val': currency, 'bytes': message}

def get_exchange(message):
    size = message[0]
    exchange = ""
    for b in unpack(size*'c', message[1:size+1]):
        exchange += b.decode("utf-8")

    message = shift(message, [size+1])
    return {'key':"exchange", 'val': exchange, 'bytes': message}

def get_quoteType(message):
    quote = QUOTE[message[0]]
    message = shift(message, [1])
    return {'key':"quote", 'val': quote, 'bytes': message}

def get_marketHours(message):
    marketHours = MARKETHOURS[message[0]]
    message = shift(message, [1])
    return {'key':"marketHours", 'val': marketHours, 'bytes': message}

def get_changePct(message):
    change = unpack('f', message[0:4])[0]
    message = shift(message, [4])
    return {'key':"changePct", 'val': change, 'bytes': message}

def get_short_name(message):
    size = message[0]
    name = ""
    for b in unpack(size*'c', message[1:size+1]):
        name += b.decode("utf-8")

    message = shift(message, [size+1])
    return {'key':"shortName", 'val': name, 'bytes': message}

def get_strike_price(message):
    strike = unpack('f', message[0:4])[0]
    message = shift(message, [4])
    return {'key':"strikePrice", 'val': strike, 'bytes': message}

def get_day_volume(message):
    volume = parse_sint64(message[0:4])
    message = shift(message, [4])
    return {'key':"dayVolume", 'val': volume, 'bytes': message}

def get_day_high(message):
    high = unpack('f', message[0:4])[0]
    message = shift(message, [4])
    return {'key':"dayHigh", 'val': high, 'bytes': message}

def get_day_low(message):
    low = unpack('f', message[0:4])[0]
    message = shift(message, [4])
    return {'key':"dayLow", 'val': low, 'bytes': message}

def get_change(message):
    change = unpack('f', message[0:4])[0]
    message = shift(message, [4])
    return {'key':"change", 'val': change, 'bytes': message}

def get_last_size(message):
    size = parse_sint64(message[0:4])
    message = shift(message, [4])
    return {'key':"lastSize", 'val': size, 'bytes': message}

def get_bid(message):
    bid = unpack('f', message[0:4])[0]
    message = shift(message, [4])
    return {'key':"bid", 'val': bid, 'bytes': message}

def get_ask(message):
    message = shift(message, [1])
    return {'key':"dayLow", 'val': 0, 'bytes': message}

def get_price_hint(message):
    hint = parse_sint64(message[0:2])
    message = shift(message, [1])
    return {'key':"priceHint", 'val': hint, 'bytes': message}

def get_vol_all_currencies(message):
    currency = parse_sint64(message[0:2])
    message = shift(message, [1])
    return {'key':"volumeAllCurrency", 'val': currency, 'bytes': message}

switch = {
    1: get_symbol, #
    2: get_price, #
    3: get_time, #
    4: get_currency, #
    5: get_exchange, #
    6: get_quoteType, #
    7: get_marketHours, #
    8: get_changePct, #
    9: get_day_volume, #
    10: get_day_high,
    11: get_day_low,
    12: get_change, #
    13: get_short_name,
    17: get_strike_price,
    22: get_last_size,
    23: get_bid,
    25: get_ask,
    27: get_price_hint, #
    29: get_vol_all_currencies
}

if __name__ == '__main__':
    # testing
    parse_message("CgdCVEMtVVNEFUQKOkcY4KXt6P9eIgNVU0QqA0NDQzApOAFFdbHWv0iAwJ2JzgFVWIo8R13GljhHZYAdS8RqC0JpdGNvaW4gVVNEsAGAwJ2JzgHYAQTgAYDAnYnOAegBgMCdic4B8gEDQlRD+gENQ29pbk1hcmtldENhcIECAAAAANXycUGJAgAAAP9NFmpC")
