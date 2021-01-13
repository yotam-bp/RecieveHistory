#################### request historical data ####################################

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time as tm
import logging
from datetime import datetime


class IBapi(EWrapper, EClient):
    global price
    global time_stamp

    def __init__(self):
        EClient.__init__(self, self)
        global price
        global time_stamp

    def error(self, req_id, error_code, error_string):
        print('error:', req_id, ' ', error_code, ' ', error_string)

    def historicalData(self, req_id, bar):
        logging.info('HistoricalData. ' + str(req_id) + ' Date: ' + str(bar.date) + ' Open: ' + str(bar.open))
        # logging.info('HistoricalData. ' + str(reqId) + ' Date: ' + str(bar.date) + ' Close: ' + str(bar.close))


def make_contract(symbol, secType, exchange, currency):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = secType
    contract.exchange = exchange
    contract.currency = currency
    return contract


def run_loop():
    app.run()


global price
global time_stamp
global oid
global last_action

# Start logging
# data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
# logging.basicConfig(filemode='w', filename='Historical Data' + str(data_and_time) + '.log', format='%(message)s',
#                     level=logging.INFO)

for f in range(1, 13):
    app = IBapi()
    app.connect('127.0.0.1', 7497, 200)

    log = logging.getLogger()

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    tm.sleep(1)  # Sleep interval to allow time for connection to server

    # choose year for data
    year = 2020
    stock = 'AMD'
    candle = '5 mins'

    # Create contract object
    stock_contract = make_contract(stock, 'STK', 'SMART', 'USD')

    if year == 2019:
        if f < 10:
            if f == 1:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(2, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '21 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 2:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '28 23:55:00', '19 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 3:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '21 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 4:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '30 23:55:00', '21 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 5:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '22 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 6:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '30 23:55:00', '20 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 7:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '22 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 8:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '22 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 9:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '30 23:55:00', '20 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 10:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + str(f) + '31 23:55:00', '23 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 11:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + str(f) + '30 23:55:00', '20 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 12:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + str(f) + '31 23:55:00', '21 D', candle, 'MIDPOINT', 0, 1, False, [])

        tm.sleep(5)
        log.handlers.clear()
        # app.cancelHistoricalData(1)
        tm.sleep(3)
        app.disconnect()
        tm.sleep(3)

    if year == 2020:
        if f < 10:
            if f == 1:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '21 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 2:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '28 23:55:00', '19 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 3:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '22 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 4:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '30 23:55:00', '21 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 5:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '20 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 6:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '30 23:55:00', '22 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 7:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '22 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 8:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '31 23:55:00', '21 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 9:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + '0' + str(f) + '30 23:55:00', '21 D', candle, 'MIDPOINT', 0, 1, False, [])
        else:
            if f == 10:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + str(f) + '31 23:55:00', '22 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 11:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + str(f) + '30 23:55:00', '20 D', candle, 'MIDPOINT', 0, 1, False, [])
            if f == 12:
                data_and_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                logging.basicConfig(filemode='w', filename=stock + '_Historical_Data' + '_Year-' + str(year) + '_Month-' + str(f) + '_Candle-' + str(candle) + '.log', format='%(message)s', level=logging.INFO)
                logging.info('Start Requesting real time data')
                app.reqHistoricalData(1, stock_contract, str(year) + str(f) + '31 23:55:00', '22 D', candle, 'MIDPOINT', 0, 1, False, [])

        tm.sleep(10)
        log.handlers.clear()
        # app.cancelHistoricalData(1)
        tm.sleep(3)
        app.disconnect()
        tm.sleep(3)
