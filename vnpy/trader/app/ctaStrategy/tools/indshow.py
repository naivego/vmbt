# encoding: UTF-8

"""
Mat
"""

import os
import sys
import talib
import numpy as np
import pandas as pd

from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.app.ctaStrategy.ctaBacktesting import *
from vnpy.trader.app.ctaStrategy.vnbt_utils import *
from vnpy.trader.app.ctaStrategy.barLoader import *



def showinds( datain = 'mongo'):
    trdvar = 'IF'
    period = 'd'
    dataStartDate = '2010-03-01'
    dataEndDate = '2017-06-01'
    DB_Rt_Dir = r'D:\ArcticFox\project\hdf5_database'.replace('\\', '/')

    # if datain == 'mongo':
    #     print 'Load bar from mongodb: ' + trdvar
    #     host, port, log = loadMongoSetting()
    #     dbClient = pymongo.MongoClient(host, port)
    #     dbName = '_'.join(['Dom', period])
    #     collection = dbClient[dbName][trdvar]
    #     # 载入初始化需要用的数据
    #     flt = {'datetime': {'$gte': dataStartDate, '$lt': dataEndDate}}
    #     dbCursor = collection.find(flt).sort('datetime', pymongo.ASCENDING)
    #     datas = list(dbCursor)
    #     if len(datas) == 0:
    #         print 'no data'
    #     vada = pd.DataFrame(datas)
    #     vada.drop(['_id'], axis=1, inplace=True)
    #     vada.set_index('datetime', inplace=True)
    #
    # elif datain == 'hd':
    #     Time_Param = [dataStartDate.replace('-', '_'), dataEndDate.replace('-', '_')]
    #     vada = load_Dombar(trdvar, period, Time_Param, DB_Rt_Dir)
    #
    # else:
    #     return

    vada = load_Dombar(trdvar, period, [dataStartDate, dataEndDate], Datain=datain, Host= 'localhost', DB_Rt_Dir= DB_Rt_Dir, Dom = 'DomContract', Adj = True)
    Dat_bar = vada.loc[:]
    Dat_bar['TR1'] = Dat_bar['high'] - Dat_bar['low']
    Dat_bar['TR2'] = abs(Dat_bar['high'] - Dat_bar['close'].shift(1))
    Dat_bar['TR3'] = abs(Dat_bar['low'] - Dat_bar['close'].shift(1))
    TR = Dat_bar.loc[:, ['TR1', 'TR2', 'TR3']].max(axis=1)
    ATR = TR.rolling(14).mean()
    vada['Ma5'] = Dat_bar['close'].rolling(5).mean()
    vada['Ma10'] = Dat_bar['close'].rolling(10).mean()
    vada['Ma20'] = Dat_bar['close'].rolling(20).mean()
    vada['Ma30'] = Dat_bar['close'].rolling(30).mean()
    vada['Ma60'] = Dat_bar['close'].rolling(60).mean()
    vada['ATR'] = ATR.div(vada['Ma10'])

    slowk, slowd = talib.STOCH(Dat_bar['high'].values,
                               Dat_bar['low'].values,
                               Dat_bar['close'].values,
                               fastk_period=9,
                               slowk_period=3,
                               slowk_matype=0,
                               slowd_period=3,
                               slowd_matype=0)
    # 获得最近的kd值
    # slowk = slowk[-1]
    # slowd = slowd[-1]
    vada['KD_k'] = slowk
    vada['KD_d'] = slowd
    plotsdk(vada, symbol=trdvar, disfactors=['Ma10','Ma20'], has2wind= 0)
    print 'ok'


def showspds( ):
    trdvar = 'M'
    period = 'M5'
    dataStartDate = '2015-01-01'
    dataEndDate = '2015-12-01'
    DB_Rt_Dir = r'D:\ArcticFox\project\hdf5_database'.replace('\\', '/')  # '/data/all/project/hdf5_database'.replace('\\', '/')

    Time_Param = [dataStartDate.replace('-', '_'), dataEndDate.replace('-', '_')]

    #-------------------------dom -----
    # vada1 = laod_Dombar(trdvar, period, Time_Param, DB_Rt_Dir, Adj= False)
    # vada2 = laod_Dombar(trdvar, period, Time_Param, DB_Rt_Dir, Dom = 'DomContract2', Adj= False)
    #-------------------------cross----------
    vada1 = load_Dombar(trdvar, period, Time_Param, Datain = 'hd', DB_Rt_Dir=DB_Rt_Dir, Dom='CrossContract', Adj=False)
    vada2 = load_Dombar(trdvar, period, Time_Param, Datain = 'hd', DB_Rt_Dir=DB_Rt_Dir,  Dom='CrossContract2', Adj=False)

    vada = vada1[['open','high','low','close']] - vada2[['open','high','low','close']] + 1000
    # vada.dropna(inplace=True)
    Dat_bar = vada.loc[:]

    # Dat_bar['TR1'] = Dat_bar['high'] - Dat_bar['low']
    # Dat_bar['TR2'] = abs(Dat_bar['high'] - Dat_bar['close'].shift(1))
    # Dat_bar['TR3'] = abs(Dat_bar['low'] - Dat_bar['close'].shift(1))
    # TR = Dat_bar.loc[:, ['TR1', 'TR2', 'TR3']].max(axis=1)
    # ATR = TR.rolling(14).mean()
    # vada['ATR'] = ATR
    # vada['Ma5'] = Dat_bar['close'].rolling(5).mean()
    # vada['Ma10'] = Dat_bar['close'].rolling(10).mean()
    # vada['Ma20'] = Dat_bar['close'].rolling(20).mean()
    # vada['Ma30'] = Dat_bar['close'].rolling(30).mean()
    # vada['Ma60'] = Dat_bar['close'].rolling(60).mean()
    # vada['ATR'] = ATR.div(vada['Ma10'])

    vada['Std'] = Dat_bar['close'].rolling(72).std()
    vada['Boll_M'] = Dat_bar['close'].rolling(72).mean()
    vada['Boll_U'] = vada['Boll_M'] + 2 * vada['Std']
    vada['Boll_D'] = vada['Boll_M'] - 2 * vada['Std']

    vada['Win_U'] = vada['Boll_M'] + 1.5 * vada['Std']
    vada['Win_D'] = vada['Boll_M'] - 1.5 * vada['Std']

    # slowk, slowd = talib.STOCH(Dat_bar['high'].values,
    #                            Dat_bar['low'].values,
    #                            Dat_bar['close'].values,
    #                            fastk_period=9,
    #                            slowk_period=3,
    #                            slowk_matype=0,
    #                            slowd_period=3,
    #                            slowd_matype=0)
    # 获得最近的kd值
    # slowk0 = slowk[-1]
    # slowd0 = slowd[-1]
    # vada['KD_k'] = slowk
    # vada['KD_d'] = slowd
    plotsdk(vada, candk= True, symbol=trdvar, disfactors=['Boll_M','Boll_U','Boll_D','Win_U','Win_D'], has2wind= 0)  #['Ma5','Ma10','Ma20','Ma30','Ma60','KD_k', 'KD_d']
    # vada.loc[:,['open','high','low','close','Ma10','Boll_M','Boll_U','Boll_D']].plot()
    # plt.show()
    print 'ok'




if __name__ == '__main__':
    showinds(datain = 'mongo')  # 'mongo' | hd
    # showspds( )