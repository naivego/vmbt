# encoding: UTF-8

"""
Grst
"""

import os
import sys
import talib
import numpy as np
import pandas as pd

from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate
from vnpy.trader.app.ctaStrategy.ctaBarda import *
from vnpy.trader.app.ctaStrategy.afactor import *
from vnpy.trader.app.ctaStrategy.factosdp import *
from vnpy.trader.app.ctaStrategy.barLoader import *
from vnpy.trader.app.ctaStrategy.ctaBarda import *

class GrstStrategy(CtaTemplate):
    """结合ATR和RSI指标的一个分钟线交易策略"""
    className = 'GrstStrategy'
    author = u'Naivego'

    # 策略参数
    atrLength = 22  # 计算ATR指标的窗口数
    atrMaLength = 10  # 计算ATR均线的窗口数
    trailingPercent = 8.0  # 百分比移动止损
    # 策略变量
    bar = None  # K线对象
    barMinute = EMPTY_STRING  # K线当前的分钟

    bufferSize = 100  # 需要缓存的数据的大小
    bufferCount = 0  # 目前已经缓存了的数据的计数
    highArray = np.zeros(bufferSize)  # K线最高价的数组
    lowArray = np.zeros(bufferSize)  # K线最低价的数组
    closeArray = np.zeros(bufferSize)  # K线收盘价的数组


    intraTradeHigh = 0  # 移动止损用的持仓期内最高价
    intraTradeLow = 0  # 移动止损用的持仓期内最低价

    orderList = []  # 保存委托代码的列表

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'atrLength',
                 'atrMaLength',
                 'trailingPercent']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'atrValue',
               'atrMa']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(GrstStrategy, self).__init__(ctaEngine, setting)

        # 注意策略类中的可变对象属性（通常是list和dict等），在策略初始化时需要重新创建，
        # 否则会出现多个策略实例之间数据共享的情况，有可能导致潜在的策略逻辑错误风险，
        # 策略类中的这些可变对象属性可以选择不写，全都放在__init__下面，写主要是为了阅读
        # 策略时方便（更多是个编程习惯的选择）
        self.setting = setting
        self.Ostpn_Dic = setting['ostpn_Dic']
        self.Sgnwt_Dic = setting['sgnwt_Dic']
        # ----------------------------------------------------------------------数据列表设置

        DB_Rt_Dir = ctaEngine.DB_Rt_Dir
        Host = ctaEngine.Host
        Datain = ctaEngine.Datain
        trdvar = ctaEngine.Symbol
        self.symbol = trdvar
        self.vtSymbol = trdvar
        Time_Param = ctaEngine.Time_Param
        # -------------------Mida
        self.var = trdvar
        try:
            Period = ctaEngine.MiniT
            ctaEngine.Mida = load_Dombar(trdvar, Period, Time_Param, Datain=Datain, Host=Host, DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)
            plotsdk(ctaEngine.Mida, symbol=trdvar, disfactors=[''], has2wind=False)
        except:
            ctaEngine.Mida = None

        self.vada1 = None
        self.bada1 = None
        self.Marst = None
        self.vada2 = None
        self.bada2 = None
        self.Surst = None
        # -------------------vada1

        try:
            Period = self.setting['msdpset']['ma']
            self.vada1 = load_Dombar(trdvar, Period, Time_Param, Datain=Datain, Host=Host, DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)
            plotsdk(self.vada1, symbol=trdvar, disfactors=[''], has2wind=False)
            self.bada1 = Barda(trdvar, Period)
            self.bada1.dat = self.vada1

            self.Marst = Grst_Factor(trdvar, Period, self.vada1, fid='ma')
            self.Marst.grst_init(setting=setting, btconfig=TS_Config)
        except:
            self.vada1 = None
            self.bada1=None

        # -------------------vada2
        try:
            Period = self.setting['msdpset']['su']
            self.vada2 = load_Dombar(trdvar, Period, Time_Param, Datain=Datain, Host=Host, DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)
            plotsdk(self.vada2, symbol=trdvar, disfactors=[''], has2wind=False)
            self.bada2 = Barda(trdvar, Period)
            self.bada2.dat = self.vada2
            self.Surst = Grst_Factor(trdvar, Period, self.vada2, fid='su')
            self.Surst.grst_init(setting=setting, btconfig=TS_Config)
        except:
            self.vada2 = None
            self.bada2 = None


        print 'strategy init finished'
        #------------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)

        print 'onInit'


    # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略启动' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略停止' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        # 计算K线
        tickMinute = tick.datetime.minute

        if tickMinute != self.barMinute:
            if self.bar:
                self.onBar(self.bar)

            bar = CtaBarData()
            bar.vtSymbol = tick.vtSymbol
            bar.symbol = tick.symbol
            bar.exchange = tick.exchange

            bar.open = tick.lastPrice
            bar.high = tick.lastPrice
            bar.low = tick.lastPrice
            bar.close = tick.lastPrice

            bar.date = tick.date
            bar.time = tick.time
            bar.datetime = tick.datetime  # K线的时间设为第一个Tick的时间

            self.bar = bar  # 这种写法为了减少一层访问，加快速度
            self.barMinute = tickMinute  # 更新当前的分钟
        else:  # 否则继续累加新的K线
            bar = self.bar  # 写法同样为了加快速度

            bar.high = max(bar.high, tick.lastPrice)
            bar.low = min(bar.low, tick.lastPrice)
            bar.close = tick.lastPrice

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        # 撤销之前发出的尚未成交的委托（包括限价单和停止单）
        print 'onBar: ', bar.datetime
        if bar.close < 1:
            return
        if bar.datetime >= '2017-07-05':
            print 'chk'
        self.ctaEngine.clearocoOrder()

        for orderID in self.orderList:
            self.cancelOrder(orderID)

        self.orderList = []

        # --------------------推送其他周期的vada
        if self.vada1:
            self.vada1.newbar(bar)
        if self.vada2:
            self.vada2.newbar(bar)

        # 保存K线数据
        self.closeArray[0:self.bufferSize - 1] = self.closeArray[1:self.bufferSize]
        self.highArray[0:self.bufferSize - 1] = self.highArray[1:self.bufferSize]
        self.lowArray[0:self.bufferSize - 1] = self.lowArray[1:self.bufferSize]

        self.closeArray[-1] = bar.close
        self.highArray[-1] = bar.high
        self.lowArray[-1] = bar.low

        self.bufferCount += 1
        if self.bufferCount < self.bufferSize:
            return

        # 在vada0上计算指标数值
        self.atrValue = talib.ATR(self.highArray, self.lowArray, self.closeArray, self.atrLength)[-1]
        self.atrArray[0:self.bufferSize - 1] = self.atrArray[1:self.bufferSize]
        self.atrArray[-1] = self.atrValue

        self.atrCount += 1
        if self.atrCount < self.bufferSize:
            return

        self.atrMa = talib.MA(self.atrArray, self.atrMaLength)[-1]
        self.rsiValue = talib.RSI(self.closeArray, self.rsiLength)[-1]

        self.cmaValue = talib.MA(self.closeArray, 20)[-1]
        self.cmaArray[0:self.bufferSize - 1] = self.cmaArray[1:self.bufferSize]
        self.cmaArray[-1] = self.cmaValue

        # 在vada1上计算指标数值
        if self.vada1:
            vada = self.vada1
            if vada.crtnum >= self.bufferSize:
                self.usr_closeArr[:-1] = vada.dat['close'][vada.crtnum - self.bufferSize + 1:vada.crtnum]
                self.usr_closeArr[-1] = vada.crtbar['close']

                self.usr_openArr[:-1] = vada.dat['open'][vada.crtnum - self.bufferSize + 1:vada.crtnum]
                self.usr_openArr[-1] = vada.crtbar['open']

                self.usr_highArr[:-1] = vada.dat['high'][vada.crtnum - self.bufferSize + 1:vada.crtnum]
                self.usr_highArr[-1] = vada.crtbar['high']

                self.usr_lowArr[:-1] = vada.dat['low'][vada.crtnum - self.bufferSize + 1:vada.crtnum]
                self.usr_lowArr[-1] = vada.crtbar['low']

                atrval = talib.ATR(self.usr_highArr, self.usr_lowArr, self.usr_closeArr, self.atrLength)[-1]
                tepma1 = talib.MA(self.usr_closeArr, self.usr_ma1cnt)[-1]
                tepma2 = talib.MA(self.usr_closeArr, self.usr_ma2cnt)[-1]
                tepma3 = talib.MA(self.usr_closeArr, self.usr_ma3cnt)[-1]
                tepma4 = talib.MA(self.usr_closeArr, self.usr_ma4cnt)[-1]
                tepma5 = talib.MA(self.usr_closeArr, self.usr_ma5cnt)[-1]

                if vada.newsta:
                    self.usr_ma1Arr[0:self.bufferSize - 1] = self.usr_ma1Arr[1:self.bufferSize]
                    self.usr_ma1Arr[-1] = tepma1
                    # -----
                    self.usr_ma2Arr[0:self.bufferSize - 1] = self.usr_ma2Arr[1:self.bufferSize]
                    self.usr_ma2Arr[-1] = tepma2
                    # -----
                    self.usr_ma3Arr[0:self.bufferSize - 1] = self.usr_ma3Arr[1:self.bufferSize]
                    self.usr_ma3Arr[-1] = tepma3
                    # -----
                    self.usr_ma4Arr[0:self.bufferSize - 1] = self.usr_ma4Arr[1:self.bufferSize]
                    self.usr_ma4Arr[-1] = tepma4
                    # -----
                    self.usr_ma5Arr[0:self.bufferSize - 1] = self.usr_ma5Arr[1:self.bufferSize]
                    self.usr_ma5Arr[-1] = tepma5
                else:
                    self.usr_ma1Arr[-1] = tepma1
                    self.usr_ma2Arr[-1] = tepma2
                    self.usr_ma3Arr[-1] = tepma3
                    self.usr_ma4Arr[-1] = tepma4
                    self.usr_ma5Arr[-1] = tepma5

                usr_k, usr_d = talib.STOCH(self.usr_highArr, self.usr_lowArr, self.usr_closeArr, 9, 3, 0, 3, 0)
                # print 'kd'
        # 在vada2上计算指标数值
        if self.vada2:
            pass
            # vada = self.vada2

        if self.vada1.crtnum < self.bufferSize:
            return

        # 判断是否要进行交易
        lgcont11 = False

        lgcont12 = False

        lgcont13 = False

        lgcont14 = False

        lgcont2 = False

        # ------------------------
        stcont11 = False
        stcont12 = False
        stcont13 = False
        stcont14 = False
        stcont2 = False

        # 当前无仓位
        print bar.datetime
        if self.pos == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low
            sdop = None
            sdsize = 0
            if lgcont11 and lgcont12 and lgcont13 and lgcont14:
                sdop = bar.close
                sdsize = 1
            elif lgcont2:
                sdop = bar.close + 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                sdsize = 1
            if sdsize > 0:
                orderID = self.buy(sdop, sdsize)
                self.orderList.append(orderID)

            sdop = None
            sdsize = 0
            if stcont11 and stcont12 and stcont13 and stcont14:
                sdop = bar.close
                sdsize = 1
            elif stcont2:
                sdop = bar.close - 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                sdsize = 1
            if sdsize > 0:
                orderID = self.short(sdop, sdsize)
                self.orderList.append(orderID)
        # 持有多头仓位
        elif self.pos > 0:
            # 计算多头持有期内的最高价，以及重置最低价
            self.intraTradeHigh = max(self.intraTradeHigh, bar.high)
            self.intraTradeLow = bar.low

            stpgrpid = 'LSTP'
            # 计算多头移动止损
            sdsp = self.intraTradeHigh * (1 - self.trailingPercent / 100)
            # 发出本地止损委托，并且把委托号记录下来，用于后续撤单
            orderID = self.sell(sdsp, abs(self.pos), stop=True)
            self.orderList.append(orderID)
            self.ctaEngine.addocoOrder(stpgrpid, orderID)
            # ---------信号平仓----
            sgn1spcont = False
            if sgn1spcont:
                sdsp = bar.close - 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                orderID = self.sell(sdsp, abs(self.pos))
                self.ctaEngine.addocoOrder(stpgrpid, orderID)
            # -------- 获利止盈 ----
            sgn1tpcont = False
            if sgn1tpcont:
                sdsp = bar.close - 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                orderID = self.sell(sdsp, abs(self.pos))
                self.orderList.append(orderID)
                self.ctaEngine.addocoOrder(stpgrpid, orderID)
        # 持有空头仓位
        elif self.pos < 0:
            self.intraTradeLow = min(self.intraTradeLow, bar.low)
            self.intraTradeHigh = bar.high

            stpgrpid = 'SSTP'
            # 计算空头移动止损
            sdsp = self.intraTradeLow * (1 + self.trailingPercent / 100)
            # 发出本地止损委托，并且把委托号记录下来，用于后续撤单
            orderID = self.cover(sdsp, abs(self.pos), stop=True)
            self.orderList.append(orderID)
            self.ctaEngine.addocoOrder(stpgrpid, orderID)
            # ---------信号平仓----
            sgn1spcont = False
            if sgn1spcont:
                sdsp = bar.close + 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                orderID = self.cover(sdsp, abs(self.pos))
                self.orderList.append(orderID)
                self.ctaEngine.addocoOrder(stpgrpid, orderID)
            # -------- 获利止盈 ----
            sgn1tpcont = False
            if sgn1tpcont:
                sdsp = bar.close + 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                orderID = self.cover(sdsp, abs(self.pos))
                self.orderList.append(orderID)
                self.ctaEngine.addocoOrder(stpgrpid, orderID)

        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        # 发出状态更新事件
        self.putEvent()


if __name__ == '__main__':
    # 提供直接双击回测的功能
    # 导入PyQt4的包是为了保证matplotlib使用PyQt4而不是PySide，防止初始化出错
    # from ctaBacktesting import *
    # from vnpy.trader.app.ctaStrategy.ctaBacktesting import *
    # from PyQt4 import QtCore, QtGui


    # --------------------backtest------------------------
    TS_Config = {}
    TS_Config['Remark'] = ''
    TS_Config['Datain'] = 'mongo'
    TS_Config['DB_Rt_Dir'] = r'D:\ArcticFox\project\hdf5_database'.replace('\\', '/')
    TS_Config['Rt_Dir'] = r'D:\Apollo\vmbt'  # os.getcwd()
    TS_Config['Host'] = 'localhost'
    TS_Config['Init_Capital'] = 10000000
    TS_Config['Time_Param'] = ['2016-06-01', '2017-06-25']
    TS_Config['SlipT'] = 0
    TS_Config['OrdTyp'] = {'open': 'Lmt', 'close': 'Lmt'}  # ['Mkt', 'Lmt', 'Stp']
    TS_Config['MiniT'] = 'M'

    setting = {}
    setting['msdpset'] = {'ma': 'M30', 'su': 'd'}
    # ---子策略ostp设置
    setting['ostpn_Dic'] = {
        'Grst':
            {
                'Grst-qsp1': Ostpn(0, -10, 200, 0, 10, -200),
                'Grst-bsp1': Ostpn(0, -10, -5, 0, 10, 5),
                'Grst-rsp1': Ostpn(0, -8, 50, 0, 8, -50),
                'Grst-zsp1': Ostpn(2, -20, 2, -2, 20, -2),
                'Grst-ovr1': Ostpn(-110, -150, -40, 110, 150, 40),

                'Grst-qsp2': Ostpn(0, -10, 800, 0, 10, -800),
                'Grst-bsp2': Ostpn(0, -10, 160, 0, 10, -160),
                'Grst-rsp2': Ostpn(0, -8, 160, 0, 8, -160),
                'Grst-zsp2': Ostpn(0, -20, 110, 0, 20, -120),
                'Grst-ovr2': Ostpn(-110, -150, -2, 110, 150, 2)
            }
    }
    # ---子策略开仓权重设置
    setting['sgnwt_Dic'] = {
        'Grst':
            {
                'Grst-qsp1': 0,
                'Grst-bsp1': 0,
                'Grst-rsp1': 1,
                'Grst-zsp1': 0,
                'Grst-ovr1': 0,
                'Grst-qsp2': 0,
                'Grst-bsp2': 0,
                'Grst-rsp2': 1,
                'Grst-zsp2': 0,
                'Grst-ovr2': 0
            }
    }

    # --------------------backtest------------------------
    # mfaset = {'sal': True, 'rdl': True, 'mdl': True, 'upl': True, 'dwl': True, 'mir': True}
    setting['mfaset'] = {'sal': True, 'rdl': True, 'mdl': True, 'upl': False, 'dwl': False, 'mir': False}
    setting['tdkopset'] = {
        'sekop': {'sal': 0, 'rdl': 1, 'mdl': 0},
        'etkop': {'sal': 0, 'rdl': 1, 'mdl': 0}
    }

    DB_Rt_Dir = TS_Config['DB_Rt_Dir']
    Host = TS_Config['Host']


    Var_List_all = ['Y', 'FU', 'BB', 'ZN', 'JR', 'BU', 'WR', 'FG', 'JD', 'HC', 'L', 'NI',
                    'PP', 'PB', 'RB', 'TF', 'RM', 'PM', 'A', 'C', 'B', 'AG', 'RU', 'I', 'J',
                    'M', 'AL', 'CF', 'V', 'CS', 'MA', 'OI', 'JM', 'SR', 'TA', 'P']  #

    Var_List_pat = ['RB']  # ['J','IF','IC','IH','TA','RB','I','CU']

    Var_List = Var_List_pat
    datain = 'mongo'
    for var in Var_List:
        print 'Test Grst For ', var
        TS_Config['Symbol'] = var
        engine = TSBacktest(TS_Config)
        engine.initStrategy(GrstStrategy, setting)
        engine.runBacktesting()

        # skdata = load_Dombar(var, Msdpset['ma'], Time_Param=TS_Config['Time_Param'], datain=datain, host=Host, DB_Rt_Dir=DB_Rt_Dir)
        # Mrst = Grst_Factor(var, Msdpset['ma'], skdata, fid='ma')
        # btest = True
        # if 'su' in Msdpset:
        #     sfaset = {'sal': True, 'rdl': True, 'mdl': True, 'upl': False, 'dwl': False, 'mir': False}
        #     stdsk = load_Dombar(var, Msdpset['su'], Time_Param=TS_Config['Time_Param'], datain=datain, host=Host, DB_Rt_Dir=DB_Rt_Dir)
        #     Arst = Grst_Factor(var, Msdpset['su'], stdsk, fid='su')
        #     Arst.grst_init(faset=sfaset, btest=False, btconfig=TS_Config, subrst=None)
        #     Mrst.addsgn(Arst.quotes, ['close'], Tn='d', fillna=False)
        #     Mrst.renamequote('close_d', 'sudc')
        #     Mrst.setmacn()
        # else:
        #     Arst = None
        # Mrst.grst_init(faset=mfaset, btest=btest, btconfig=TS_Config, subrst=Arst, tdkopset=tdkopset)
        # Mrst.cal_next()
        #
        # if Mrst.TSBT:
        #     Mrst.TSBT.Show_SaveResult()
        #
        # disfas = ['disrst', 'sal', 'brdl', 'trdl', 'bmdl',
        #           'tmdl']  # ,'zsh', 'zsl', 'rstsph', 'rstspl','bsh', 'bsl' ,'bbl', 'ttl'  'bbl', 'ttl', 'obbl', 'ottl',
        # # plotsdk(quotes, disfactors=['grst', 'qsh', 'qsl', 'zsh', 'zsl', 'rsl','disrst', 'bbl', 'ttl'])  # ,'ATR'  'qswr',
        # # plotsdk(quotes, disfactors=['disrst','sk_qsrpt','sk_zs1c', 'sk_zs2c', 'sk_qs1c','sk_qs2c','sk_zs1a', 'sk_zs2a', 'sk_qs1a','sk_qs2a'])  #,'sk_zsrpt','sk_qsrpt'  'sk_zs1c','sk_zs2c', 'qsh', 'qsl',
        # # plotsdk(quotes, Symbol=var, disfactors=['disrst', 'bbl', 'ttl', 'alp1', 'alp2', 'alp5', 'dlp1', 'dlp2', 'dlp5', ])
        # # plotsdk(quotes, Symbol=var, disfactors=disfas)
        #
        # # ---------------show sgns
        # if 'su' in Msdpset:
        #     Arst.colfas()
        #     Tn = 'd'
        #     extfas = ['disrst', 'sal', 'brdl', 'trdl']  # ['disrst', 'sal', 'brdl', 'trdl', 'bmdl', 'tmdl']
        #     Mrst.addsgn(Arst.quotes, extfas, Tn=Tn, fillna=False)
        #     disfas = [colna + '_' + Tn for colna in extfas] + ['disrst', 'brdl', 'trdl', 'alp1', 'dlp1',
        #                                                        'sal']  # 'disrst','ma','mid', ['disrst','sal', 'brdl', 'trdl', 'alp1', 'dlp1' ]
        # Mrst.colfas()
        # quotesk = Mrst.quotes
        # plotsdk(quotesk, Symbol=var, disfactors=disfas)
        #
        # print datetime.now(), 'ok'










    #
    #
    #
    # # 创建回测引擎
    # btconfig = {}
    # btconfig['Rt_Dir'] = r'D:\Apollo\vmtb'  # '/home/chenxiubin/projects/PMM'
    # btconfig['DB_Rt_Dir'] = r'D:\ArcticFox\project\hdf5_database'.replace('\\', '/')  # '/data/all/project/hdf5_database'.replace('\\', '/')
    # btconfig['Start_Date'] = '2016-03-01'
    # btconfig['End_Date'] = '2016-03-30'
    # btconfig['InitCapital'] = 1000000
    # btconfig['Slippage'] = 1
    # btconfig['Symbol'] = 'RB'
    # btconfig['MiniT'] = 'M'
    # engine = BacktestingEngine(btconfig)
    # # 设置引擎的回测模式为K线
    # engine.setBacktestingMode(engine.BAR_MODE)
    #
    # # set setting ---该setting信息在交易中可以通过json导入
    # setting = {}
    # #---------数据列表设置
    # setting['vada1'] = {'var': '', 'period': 'M15'} # var='' --> engine.symbol,
    # setting['vada2'] = {'var': '', 'period': 'H'}
    # setting['vada3'] = {'var': '', 'period': 'd'}
    # # ---------各数据所需要计算的因子列表和参数设置
    #
    # setting['fas1'] = {'Boll': [20, 2], 'Atr': [10, 26, 50], 'Ma': [10, 20, 0]}
    # setting['fas2'] = {'Boll': [20, 2], 'Atr': [10, 26, 50], 'Ma': [10, 20, 0]}
    # setting['fas3'] = {'Boll': [20, 2], 'Atr': [10, 26, 50], 'Ma': [10, 20, 0]}
    #
    # engine.initStrategy(HdmatStrategy, setting)
    #
    # # 开始跑回测
    # engine.runBacktesting()
    #
    # # 显示回测结果
    # engine.showBacktestingResult()
    #
    # ## 跑优化
    #
    # # import time
    # # start = time.time()
    #
    # ## 运行单进程优化函数，自动输出结果，耗时：359秒
    #
    #
    # # print u'耗时：%s' %(time.time()-start)