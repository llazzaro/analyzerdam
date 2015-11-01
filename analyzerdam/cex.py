import logging
import datetime

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
            quote = self.api.ticker(security.symbol + '/USD')
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
