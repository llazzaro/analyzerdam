import logging
import datetime
from time import sleep
try:
    from urllib2 import URLError
except ImportError:
    from urllib.error import URLError

from cexapi.cexapi import API

from analyzer.dam import BaseDAM

log=logging.getLogger(__name__)


class CexDAM(BaseDAM):

    def __init__(self, config):
        super(CexDAM, self).__init__()
        self.username = config.get('dam', 'username')
        self.api_secret = config.get('dam', 'api_secret')
        self.api_key = config.get('dam', 'api_key')
        self.api = API(self.username, self.api_key, self.api_secret)
        self.last_price = -1

    def read_quotes(self, securities, start, end):
        for security in securities:
            try:
                quote = self.api.ticker(security.symbol + '/USD')
                sleep(1.1)  # to avoid getting banned on cex
            except URLError as ex:
                log.info('Exception was {0}'.format(ex))
                log.info('Error from cex api. Sleeping 5 sec')
                sleep(5)
                yield (security, [])
                continue

            if 'error' in quote:
                log.info('Error while retrieving quotes {0}'.format(quote))
                raise Exception('API error')
            tick_date = datetime.datetime.fromtimestamp(int(quote['timestamp']))
            if start <= tick_date <= end and self.last_price != quote['last']:
                self.last_price = quote['last']
                yield (security, [quote])

    def read_ticks(self, securities, start, end):
        pass

    def read_fundamental(self, symbols):
        return self.google_finance.financials(self.symbol)
