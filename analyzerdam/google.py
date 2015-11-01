'''
Created on Nov 9, 2011

@author: ppa
'''
from analyzer.dam import BaseDAM
from analyzerdam.google_finance import GoogleFinance

import logging
LOG = logging.getLogger()


class GoogleDAM(BaseDAM):

    def __init__(self):
        super(GoogleDAM, self).__init__()
        self.google_finance = GoogleFinance()

    def read_quotes(self, securities, start, end):
        for security in securities:
            yield (security, self.google_finance.quotes(security, start, end))

    def read_ticks(self, securities, start, end):
        for security in securities:
            yield (security, self.google_finance.ticks(security, start, end))

    def read_fundamental(self, symbols):
        return self.google_finance.financials(self.symbol)
