'''
Created on July 31, 2011

@author: ppa
'''
import json
import logging
import traceback

import requests
from bs4 import BeautifulSoup
from analyzer.lib.util import convertGoogCSVDate
from analyzer.lib.errors import UfException, Errors
from pyStock.models import Quote

LOG = logging.getLogger(__name__)


class GoogleFinance(object):
    FIELDS = ['Revenue', 'Other Revenue, Total', 'Total Revenue', 'Cost of Revenue, Total', 'Gross Profit', 'Selling/General/Admin. Expenses, Total',
              'Research & Development', 'Depreciation/Amortization', 'Interest Expense(Income) - Net Operating', 'Unusual Expense (Income)',
              'Other Operating Expenses, Total', 'Total Operating Expense', 'Operating Income', 'Interest Income(Expense), Net Non-Operating',
              'Gain (Loss) on Sale of Assets', 'Gain (Loss) on Sale of Assets', 'Income Before Tax', 'Income After Tax', 'Minority Interest',
              'Equity In Affiliates', 'Net Income Before Extra. Items', 'Accounting Change', 'Discontinued Operations', 'Extraordinary Item',
              'Net Income', 'Preferred Dividends', 'Income Available to Common Excl. Extra Items', 'Income Available to Common Incl. Extra Items',
              'Basic Weighted Average Shares', 'Basic EPS Excluding Extraordinary Items', 'Basic EPS Including Extraordinary Items',
              'Dilution Adjustment', 'Diluted Weighted Average Shares', 'Diluted EPS Excluding Extraordinary Items', 'Diluted EPS Including Extraordinary Items',
              'Dividends per Share - Common Stock Primary Issue', 'Gross Dividends - Common Stock', 'Net Income after Stock Based Comp. Expense',
              'Basic EPS after Stock Based Comp. Expense', 'Diluted EPS after Stock Based Comp. Expense', 'Depreciation, Supplemental',
              'Total Special Items', 'Normalized Income Before Taxes', 'Effect of Special Items on Income Taxes', 'Income Taxes Ex. Impact of Special Items',
              'Normalized Income After Taxes', 'Normalized Income Avail to Common', 'Basic Normalized EPS', 'Diluted Normalized EPS']

    def _request(self, url):
        try:
            return requests.get(url)
        except IOError:
            raise UfException(Errors.NETWORK_ERROR, "Can't connect to Google server at %s" % url)
        except Exception:
            raise UfException(Errors.UNKNOWN_ERROR, "Unknown Error in GoogleFinance._request %s" % traceback.format_exc())

    def get_all(self, security):
        """
        Get all available quote data for the given ticker security.
        Returns a dictionary.
        """
        url = 'http://www.google.com/finance?q=%s' % security
        page = self._request(url)

        soup = BeautifulSoup(page)
        snapData = soup.find("table", {"class": "snap-data"})
        if snapData is None:
            raise UfException(Errors.STOCK_SYMBOL_ERROR, "Can find data for stock %s, security error?" % security)
        data = {}
        for row in snapData.findAll('tr'):
            keyTd, valTd = row.findAll('td')
            data[keyTd.getText()] = valTd.getText()

        return data

    def quotes(self, security, start, end):
        """
        Get historical prices for the given ticker security.
        Date format is 'YYYYMMDD'

        Returns a nested list.
        """
        try:
            url = 'http://www.google.com/finance/historical?q=%s&startdate=%s&enddate=%s&output=csv' % (security.symbol, start, end)
            try:
                page = self._request(url)
            except UfException as ufExcep:
                # if symol is not right, will get 400
                if Errors.NETWORK_400_ERROR == ufExcep.getCode:
                    raise UfException(Errors.STOCK_SYMBOL_ERROR, "Can find data for stock %s, security error?" % security)
                raise ufExcep

            days = page.readlines()
            values = [day.split(',') for day in days]
            # sample values:[['Date', 'Open', 'High', 'Low', 'Close', 'Volume'], \
            #              ['2009-12-31', '112.77', '112.80', '111.39', '111.44', '90637900']...]
            for value in values[1:]:
                date = convertGoogCSVDate(value[0])
                try:
                    yield Quote(date,
                                      value[1].strip(),
                                      value[2].strip(),
                                      value[3].strip(),
                                      value[4].strip(),
                                      value[5].strip(),
                                      None)
                except Exception:
                    LOG.warning("Exception when processing %s at date %s for value %s" % (security, date, value))

        except BaseException:
            raise UfException(Errors.UNKNOWN_ERROR, "Unknown Error in GoogleFinance.getHistoricalPrices %s" % traceback.format_exc())
        # sample output
        # [stockDaylyData(date='2010-01-04, open='112.37', high='113.39', low='111.51', close='113.33', volume='118944600', adjClose=None))...]

    def financials(self, security):
        """
        get financials:
        google finance provide annual and quanter financials, if annual is true, we will use annual data
        Up to four lastest year/quanter data will be provided by google
        Refer to page as an example: http://www.google.com/finance?q=TSE:CVG&fstype=ii
        """
        try:
            url = 'http://www.google.com/finance?q=%s&fstype=ii' % security
            try:
                page = self._request(url).read()
            except UfException as ufExcep:
                # if symol is not right, will get 400
                if Errors.NETWORK_400_ERROR == ufExcep.getCode:
                    raise UfException(Errors.STOCK_SYMBOL_ERROR, "Can find data for stock %s, security error?" % security)
                raise ufExcep

            bPage = BeautifulSoup(page)
            target = bPage.find(id='incinterimdiv')

            keyTimeValue = {}
            # ugly do...while
            i = 0
            while True:
                self._parseTarget(target, keyTimeValue)

                if i < 5:
                    i += 1
                    target = target.nextSibling
                    # ugly beautiful soap...
                    if '\n' == target:
                        target = target.nextSibling
                else:
                    break

            return keyTimeValue

        except BaseException:
            raise UfException(Errors.UNKNOWN_ERROR, "Unknown Error in GoogleFinance.getHistoricalPrices %s" % traceback.format_exc())

    def _parseTarget(self, target, keyTimeValue):
        ''' parse table for get financial '''
        table = target.table
        timestamps = self._getTimeStamps(table)

        for tr in table.tbody.findChildren('tr'):
            for i, td in enumerate(tr.findChildren('td')):
                if 0 == i:
                    key = td.getText()
                    if key not in keyTimeValue:
                        keyTimeValue[key] = {}
                else:
                    keyTimeValue[key][timestamps[i - 1]] = self._getValue(td)

    def _getTimeStamps(self, table):
        ''' get time stamps '''
        timeStamps = []
        for th in table.thead.tr.contents:
            if '\n' != th:
                timeStamps.append(th.getText())

        return timeStamps[1:]

    def _getValue(self, td):
        ''' get value from td '''
        if '-' == td.getText():
            return None
        return float(td.getText().replace(',', ''))

    def ticks(self, security, start, end):
        """
        Get tick prices for the given ticker security.
        @security: stock security
        @interval: interval in mins(google finance only support query till 1 min)
        @start: start date(YYYYMMDD)
        @end: end date(YYYYMMDD)

        start and end is disabled since only 15 days data will show

        @Returns a nested list.
        """
        period = 1
        # url = 'http://www.google.com/finance/getprices?q=%s&i=%s&p=%sd&f=d,o,h,l,c,v&ts=%s' % (security, interval, period, start)
        url = 'http://www.google.com/finance/getprices?q=%s&i=61&p=%sd&f=d,o,h,l,c,v' % (security.symbol, period)
        LOG.debug('fetching {0}'.format(url))
        try:
            response = self._request(url)
        except UfException as ufExcep:
            # if symol is not right, will get 400
            if Errors.NETWORK_400_ERROR == ufExcep.getCode:
                raise UfException(Errors.STOCK_SYMBOL_ERROR, "Can find data for stock %s, security error?" % security)
            raise ufExcep

        # use csv reader here
        days = response.text.split('\n')[7:]  # first 7 line is document
        # sample values:'a1316784600,31.41,31.5,31.4,31.43,150911'
        values = [day.split(',') for day in days if len(day.split(',')) >= 6]
        for value in values:
            yield json.dumps({'date': value[0][1:].strip(),
                'close': value[1].strip(),
                'high': value[2].strip(),
                'low': value[3].strip(),
                'open': value[4].strip(),
                'volume': value[5].strip()})

        # sample output
        # [stockDaylyData(date='1316784600', open='112.37', high='113.39', low='111.51', close='113.33', volume='118944600', adjClose=None))...]
