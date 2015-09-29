'''
Created on Nov 9, 2011

@author: ppa
'''
import sys
import logging

from sqlalchemy import Column, Integer, String, Float, Sequence, create_engine, and_
from sqlalchemy.orm import sessionmaker, scoped_session

from pyStock.models import Quote, Tick
from pyStock import Base
from analyzer.dam import BaseDAM
from analyzer.lib.util import splitListEqually

LOG=logging.getLogger()


class FmSql(Base):
    __tablename__='fundamental'

    id=Column(Integer, Sequence('user_id_seq'), primary_key=True)
    symbol=Column(String(12))
    field=Column(String(50))
    timeStamp=Column(String(50))
    value=Column(Float)

    def __init__(self, symbol, field, timeStamp, value):
        ''' constructor '''
        self.symbol=symbol
        self.field=field
        self.timeStamp=timeStamp
        self.value=value

    def __repr__(self):
        return "<Fundamentals('%s', '%s', '%s', '%s')>".format(self.symbol, self.field, self.timeStamp, self.value)


class SqlDAM(BaseDAM):

    def __init__(self, setting, echo=False):
        super(SqlDAM, self).__init__()
        self.echo=echo
        self.first=True
        self.engine=None
        self.ReadSession=None
        self.WriteSession=None
        self.writeSession=None

        if 'db' not in setting:
            raise Exception("db not specified in setting")

        self.engine=create_engine(setting['db'], echo=self.echo)

    def getReadSession(self):
        ''' return scopted session '''
        if self.ReadSession is None:
            self.ReadSession=scoped_session(sessionmaker(bind=self.engine))

        return self.ReadSession

    def getWriteSession(self):
        ''' return unscope session, TODO, make it clear '''
        if self.WriteSession is None:
            self.WriteSession=sessionmaker(bind=self.engine)
            self.writeSession=self.WriteSession()

        return self.writeSession

    def read_quotes(self, start, end):
        ''' read quotes '''
        if end is None:
            end=sys.maxint

        session=self.getReadSession()()
        try:
            rows=session.query(Quote).filter(and_(Quote.symbol == self.symbol,
                                                            Quote.time >= int(start),
                                                            Quote.time < int(end)))
        finally:
            self.getReadSession.remove()

        return [self.__sqlToQuote(row) for row in rows]

    def readTupleQuotes(self, start, end):
        ''' read quotes as tuple '''
        if end is None:
            end=sys.maxint

        session=self.getReadSession()()
        try:
            rows=session.query(Quote).filter(and_(Quote.symbol == self.symbol,
                                                       Quote.time >= int(start),
                                                       Quote.time < int(end)))
        finally:
            self.getReadSession().remove()

        return rows

    def readBatchTupleQuotes(self, symbols, start, end):
        '''
        read batch quotes as tuple to save memory
        '''
        if end is None:
            end=sys.maxint

        ret={}
        session=self.getReadSession()()
        try:
            symbolChunks=splitListEqually(symbols, 100)
            for chunk in symbolChunks:
                rows=session.query(Quote.symbol, Quote.time, Quote.close, Quote.volume,
                                     Quote.low, Quote.high).filter(and_(Quote.symbol.in_(chunk),
                                                                              Quote.time >= int(start),
                                                                              Quote.time < int(end)))

                for row in rows:
                    if row.time not in ret:
                        ret[row.time]={}

                    ret[row.time][row.symbol]=self.__sqlToTupleQuote(row)
        finally:
            self.getReadSession().remove()

        return ret

    def read_tuple_ticks(self, start, end):
        ''' read ticks as tuple '''
        if end is None:
            end=sys.maxint

        session=self.getReadSession()()
        try:
            rows=session.query(Tick).filter(and_(Tick.symbol == self.symbol,
                                                      Tick.time >= int(start),
                                                      Tick.time < int(end)))
        finally:
            self.getReadSession().remove()

        return [self.__sqlToTupleTick(row) for row in rows]

    def read_ticks(self, start, end):
        ''' read ticks '''
        if end is None:
            end=sys.maxint

        session=self.getReadSession()()
        try:
            rows=session.query(Tick).filter(and_(Tick.symbol == self.symbol,
                                                      Tick.time >= int(start),
                                                      Tick.time < int(end)))
        finally:
            self.getReadSession().remove()

        return [self.__sqlToTick(row) for row in rows]

    def write_quotes(self, quotes):
        ''' write quotes '''
        if self.first:
            Base.metadata.create_all(self.engine, checkfirst=True)
            self.first=False

        session=self.getWriteSession()
        session.add_all([self.__quoteToSql(quote) for quote in quotes])

    def write_ticks(self, ticks):
        ''' write ticks '''
        if self.first:
            Base.metadata.create_all(self.engine, checkfirst=True)
            self.first=False

        session=self.getWriteSession()
        session.add_all([self.__tickToSql(tick) for tick in ticks])

    def commit(self):
        ''' commit changes '''
        session=self.getWriteSession()
        session.commit()

    def write_fundamental(self, keyTimeValueDict):
        ''' write fundamental '''
        if self.first:
            Base.metadata.create_all(self.__getEngine(), checkfirst=True)
            self.first=False

        sqls=self._fundamentalToSqls(keyTimeValueDict)
        session=self.Session()
        try:
            session.add_all(sqls)
        finally:
            self.Session.remove()

    def read_fundamental(self):
        rows=self.__getSession().query(FmSql).filter(and_(FmSql.symbol == self.symbol))
        return self._sqlToFundamental(rows)

    def _sqlToFundamental(self, rows):
        keyTimeValueDict={}
        for row in rows:
            if row.field not in keyTimeValueDict:
                keyTimeValueDict[row.field]={}

            keyTimeValueDict[row.field][row.timeStamp]=row.value

        return keyTimeValueDict

    def _fundamentalToSqls(self, keyTimeValueDict):
        ''' convert fundament dict to sqls '''
        sqls=[]
        for key, timeValues in keyTimeValueDict.iteritems():
            for timeStamp, value in timeValues.iteritems():
                sqls.append(FmSql(self.symbol, key, timeStamp, value))

        return sqls
