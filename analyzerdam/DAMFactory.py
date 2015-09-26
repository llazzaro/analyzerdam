'''
Created on Nov 9, 2011

@author: ppa
'''

from analyzer.lib.errors import Errors, UfException


class DAMFactory(object):
    ''' DAM factory '''
    @staticmethod
    def createDAM(damType, settings=None):
        ''' create DAM '''
        if 'yahoo' == damType:
            from analyzerdam.yahooDAM import YahooDAM
            dam=YahooDAM()
        elif 'google' == damType:
            from analyzerdam.googleDAM import GoogleDAM
            dam=GoogleDAM()
        elif 'excel' == damType:
            from analyzerdam.excelDAM import ExcelDAM
            dam=ExcelDAM()
        elif 'hbase' == damType:
            from analyzerdam.hbaseDAM import HBaseDAM
            dam=HBaseDAM()
        elif 'sql' == damType:
            from analyzerdam.sqlDAM import SqlDAM
            dam=SqlDAM(settings)
        else:
            raise UfException(Errors.INVALID_DAM_TYPE,
                              "DAM type is invalid %s" % damType)

        return dam

    @staticmethod
    def getAvailableTypes():
        ''' return all available types '''
        return ['yahoo', 'google', 'excel', 'hbase', 'sql']
