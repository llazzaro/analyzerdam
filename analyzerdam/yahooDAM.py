'''
Created on Nov 9, 2011

@author: ppa
'''
from analyzerdam.baseDAM import BaseDAM
from analyzerdam.yahooFinance import YahooFinance

import logging
LOG = logging.getLogger()

class YahooDAM(BaseDAM):
    ''' Yahoo DAM '''

    def __init__(self):
        ''' constructor '''
        super(YahooDAM, self).__init__()
        self.__yf = YahooFinance()

    def readQuotes(self, start, end):
        ''' read quotes from Yahoo Financial'''
        if self.symbol is None:
            LOG.debug('Symbol is None')
            return []

        return self.__yf.getQuotes(self.symbol, start, end)
