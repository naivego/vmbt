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
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate
from vnpy.trader.app.ctaStrategy.ctaBarda import *


class HdmatStrategy(CtaTemplate):
    """结合ATR和RSI指标的一个分钟线交易策略"""
    className = 'MatStrategy'
    author = u'Lei'

    # 策略参数
    atrLength = 22  # 计算ATR指标的窗口数
    atrMaLength = 10  # 计算ATR均线的窗口数
    rsiLength = 5  # 计算RSI的窗口数
    rsiEntry = 16  # RSI的开仓信号
    trailingPercent = 8.0  # 百分比移动止损
    initDays = 10  # 初始化数据所用的天数
    fixedSize = 1  # 固定每次交易的数量
    s1fundpct = 1.0  # 按资金比例信号1每次开仓市值占比
    s2fundpct = 1.0  # 按资金比例信号2每次开仓市值占比
    # 策略变量
    bar = None  # K线对象
    barMinute = EMPTY_STRING  # K线当前的分钟

    bufferSize = 100  # 需要缓存的数据的大小
    bufferCount = 0  # 目前已经缓存了的数据的计数
    highArray = np.zeros(bufferSize)  # K线最高价的数组
    lowArray = np.zeros(bufferSize)  # K线最低价的数组
    closeArray = np.zeros(bufferSize)  # K线收盘价的数组

    atrCount = 0  # 目前已经缓存了的ATR的计数
    atrArray = np.zeros(bufferSize)  # ATR指标的数组
    atrValue = 0  # 最新的ATR指标数值
    atrMa = 0  # ATR移动平均的数值

    cmaCount = 0  # 目前已经缓存了的ATR的计数
    cmaArray = np.zeros(bufferSize)  # MA指标的数组
    cmaValue = 0  # 最新的cmaMa指标数值
    cmaMa = 0

    usr_ma1cnt = 5
    usr_ma2cnt = 10
    usr_ma3cnt = 20
    usr_ma4cnt = 30
    usr_ma5cnt = 60
    usr_closeArr = np.zeros(bufferSize)
    usr_highArr = np.zeros(bufferSize)
    usr_lowArr = np.zeros(bufferSize)
    usr_openArr = np.zeros(bufferSize)
    usr_ma1Arr = np.zeros(bufferSize)
    usr_ma2Arr = np.zeros(bufferSize)
    usr_ma3Arr = np.zeros(bufferSize)
    usr_ma4Arr = np.zeros(bufferSize)
    usr_ma5Arr = np.zeros(bufferSize)

    rsiValue = 0  # RSI指标的数值
    rsiBuy = 0  # RSI买开阈值
    rsiSell = 0  # RSI卖开阈值
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
                 'rsiLength',
                 'rsiEntry',
                 'trailingPercent']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'atrValue',
               'atrMa',
               'rsiValue',
               'rsiBuy',
               'rsiSell']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(HdmatStrategy, self).__init__(ctaEngine, setting)

        # 注意策略类中的可变对象属性（通常是list和dict等），在策略初始化时需要重新创建，
        # 否则会出现多个策略实例之间数据共享的情况，有可能导致潜在的策略逻辑错误风险，
        # 策略类中的这些可变对象属性可以选择不写，全都放在__init__下面，写主要是为了阅读
        # 策略时方便（更多是个编程习惯的选择）
        self.setting = setting

        # ----------------------------------------------------------------------数据列表设置
        self.vadas = {}
        trdvar = ctaEngine.symbol
        self.symbol = trdvar
        self.vtSymbol = trdvar
        Time_Param = [ctaEngine.startDate.replace('-','_'), ctaEngine.endDate.replace('-','_')]
        # -------------------vada0
        self.var = trdvar
        try:
            Period = setting['vada0']['period']
            self.vada0 = load_Dombar(self.var, Period, Time_Param, ctaEngine.DB_Rt_Dir)
            plotsdk(self.vada0, symbol=self.var, disfactors=[''], has2wind=False)
        except:
            self.vada0 = None
        # -------------------vada1
        try:
            self.var1 = setting['vada1']['var']
            if self.var1 == '':
                self.var1 = trdvar
            Period = setting['vada1']['period']
            self.vada1 = load_Dombar(self.var1, Period, Time_Param, ctaEngine.DB_Rt_Dir)
            # plotsdk(self.vada1, symbol=self.var1, disfactors=[''], has2wind=False)
        except:
            self.vada1 = None

        # -------------------vada2
        try:
            self.var2 = setting['vada2']['var']
            if self.var2 == '':
                self.var2 = trdvar
            Period = setting['vada2']['period']
            self.vada2 = load_Dombar(self.var2, Period, Time_Param, ctaEngine.DB_Rt_Dir)
            # plotsdk(self.vada2, symbol=self.var2, disfactors=[''], has2wind=False)
        except:
            self.vada2 = None

        # -------------------vada3
        try:
            self.var3 = setting['vada3']['var']
            if self.var3 == '':
                self.var3 = trdvar
            Period = setting['vada3']['period']
            self.vada3 = load_Dombar(self.var3, Period, Time_Param, ctaEngine.DB_Rt_Dir)
            plotsdk(self.vada3, symbol=self.var3, disfactors=[''], has2wind=False)
        except:
            self.vada3 = None

        self.vadas['vada0'] = self.vada0
        self.vadas['vada1'] = self.vada1
        self.vadas['vada2'] = self.vada2
        self.vadas['vada3'] = self.vada3
        #------------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)



        print 'onInit'
        # self.ctaEngine.loadDataFromMongo(self.vadas)

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
        lgcont11 = bar.close > self.usr_ma4Arr[-1] and bar.close > self.usr_ma5Arr[-1] and \
                   self.usr_ma4Arr[-1] > self.usr_ma4Arr[-2] and self.usr_ma5Arr[-1] > self.usr_ma5Arr[-2] and \
                   (self.usr_ma1Arr[-1] > self.usr_ma1Arr[-2] or self.usr_ma2Arr[-1] > self.usr_ma2Arr[-2])

        lgcont12 = bar.close > self.usr_ma3Arr[-1] and bar.close > self.usr_ma4Arr[-1] and \
                   (self.usr_ma4Arr[-1] > self.usr_ma4Arr[-2] or self.usr_ma3Arr[-1] > self.usr_ma3Arr[-2]) and \
                   (self.usr_ma1Arr[-1] > self.usr_ma1Arr[-2] and self.usr_ma2Arr[-1] > self.usr_ma2Arr[-2])

        lgcont13 = bar.close > self.usr_ma3Arr[-1] and bar.close > self.usr_ma4Arr[-1] and \
                   (self.usr_ma3Arr[-1] > self.usr_ma3Arr[-2] and self.usr_ma4Arr[-1] > self.usr_ma4Arr[-2]) and \
                   (self.usr_ma1Arr[-1] > self.usr_ma1Arr[-2] or self.usr_ma2Arr[-1] > self.usr_ma2Arr[-2])

        lgcont14 = usr_d[-1] < 85 and usr_k[-1] - usr_d[-1] < 10 and self.usr_highArr[-2] < bar.close < self.usr_highArr[-2] * 1.002

        lgcont2 = 0 and usr_d[-1] < 15 and usr_k[-1] - usr_d[-1] >= 0.2 and usr_k[-2] - usr_d[-2] <= -0.2 and bar.close > self.usr_lowArr[-2]

        # ------------------------
        stcont11 = bar.close < self.usr_ma4Arr[-1] and bar.close < self.usr_ma5Arr[-1] and \
                   self.usr_ma4Arr[-1] < self.usr_ma4Arr[-2] and self.usr_ma5Arr[-1] < self.usr_ma5Arr[-2] and \
                   (self.usr_ma1Arr[-1] < self.usr_ma1Arr[-2] or self.usr_ma2Arr[-1] < self.usr_ma2Arr[-2])

        stcont12 = bar.close < self.usr_ma3Arr[-1] and bar.close < self.usr_ma4Arr[-1] and \
                   (self.usr_ma4Arr[-1] < self.usr_ma4Arr[-2] or self.usr_ma3Arr[-1] < self.usr_ma3Arr[-2]) and \
                   (self.usr_ma1Arr[-1] < self.usr_ma1Arr[-2] and self.usr_ma2Arr[-1] < self.usr_ma2Arr[-2])

        stcont13 = bar.close < self.usr_ma3Arr[-1] and bar.close < self.usr_ma4Arr[-1] and \
                   (self.usr_ma3Arr[-1] < self.usr_ma3Arr[-2] and self.usr_ma4Arr[-1] < self.usr_ma4Arr[-2]) and \
                   (self.usr_ma1Arr[-1] < self.usr_ma1Arr[-2] or self.usr_ma2Arr[-1] < self.usr_ma2Arr[-2])

        stcont14 = usr_d[-1] > 15 and -usr_k[-1] + usr_d[-1] < 10 and self.usr_lowArr[-2] * 0.998 < bar.close < self.usr_lowArr[-2]

        stcont2 = 0 and usr_d[-1] > 85 and usr_k[-1] - usr_d[-1] <= -0.2 and usr_k[-2] - usr_d[-2] >= 0.2 and bar.close < self.usr_highArr[-2]

        # 当前无仓位
        print bar.datetime
        if self.pos == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low

            sdop = None
            sdsize = 0
            if lgcont11 and lgcont12 and lgcont13 and lgcont14:
                sdop = (self.usr_highArr[-2] - self.usr_lowArr[-2]) * 0.8 + self.usr_lowArr[-2]
                sdsize = round(self.ctaEngine.capital * self.s1fundpct / self.ctaEngine.Multiplier[self.vtSymbol] / sdop)
            elif lgcont2:
                sdop = bar.close + 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                sdsize = round(self.ctaEngine.capital * self.s2fundpct / self.ctaEngine.Multiplier[self.vtSymbol] / sdop)
            if sdsize > 0:
                orderID = self.buy(sdop, sdsize)
                self.orderList.append(orderID)

            sdop = None
            sdsize = 0
            if stcont11 and stcont12 and stcont13 and stcont14:
                sdop = (self.usr_highArr[-2] - self.usr_lowArr[-2]) * 0.2 + self.usr_lowArr[-2]
                sdsize = round(self.ctaEngine.capital * self.s1fundpct / self.ctaEngine.Multiplier[self.vtSymbol] / sdop)
            elif stcont2:
                sdop = bar.close - 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                sdsize = round(self.ctaEngine.capital * self.s2fundpct / self.ctaEngine.Multiplier[self.vtSymbol] / sdop)
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
            sgn1spcont = bar.close < self.usr_lowArr[-2] - atrval * 0.6
            if sgn1spcont:
                sdsp = bar.close - 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                orderID = self.sell(sdsp, abs(self.pos))
                self.ctaEngine.addocoOrder(stpgrpid, orderID)
            # -------- 获利止盈 ----
            sgn1tpcont = bar.close > self.usr_ma1Arr[-1] * 1.03
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
            sgn1spcont = bar.close > self.usr_highArr[-2] + atrval * 0.6
            if sgn1spcont:
                sdsp = bar.close + 2 * self.ctaEngine.Tick_Size[self.vtSymbol]
                orderID = self.cover(sdsp, abs(self.pos))
                self.orderList.append(orderID)
                self.ctaEngine.addocoOrder(stpgrpid, orderID)
            # -------- 获利止盈 ----
            sgn1tpcont = bar.close < self.usr_ma1Arr[-1] * 0.97
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
    from vnpy.trader.app.ctaStrategy.ctaBacktesting import *
    from PyQt4 import QtCore, QtGui

    # 创建回测引擎
    btconfig = {}
    btconfig['Rt_Dir'] = r'D:\Apollo\vmtb'  # '/home/chenxiubin/projects/PMM'
    btconfig['DB_Rt_Dir'] = r'D:\ArcticFox\project\hdf5_database'.replace('\\', '/')  # '/data/all/project/hdf5_database'.replace('\\', '/')
    btconfig['Start_Date'] = '2016-03-01'
    btconfig['End_Date'] = '2016-03-30'
    btconfig['InitCapital'] = 1000000
    btconfig['Slippage'] = 1
    btconfig['Symbol'] = 'RB'
    btconfig['MiniT'] = 'M'
    engine = BacktestingEngine(btconfig)
    # 设置引擎的回测模式为K线
    engine.setBacktestingMode(engine.BAR_MODE)

    # set setting ---该setting信息在交易中可以通过json导入
    setting = {}
    #---------数据列表设置
    setting['vada1'] = {'var': '', 'period': 'M15'} # var='' --> engine.symbol,
    setting['vada2'] = {'var': '', 'period': 'H'}
    setting['vada3'] = {'var': '', 'period': 'd'}
    # ---------各数据所需要计算的因子列表和参数设置

    setting['fas1'] = {'Boll': [20, 2], 'Atr': [10, 26, 50], 'Ma': [10, 20, 0]}
    setting['fas2'] = {'Boll': [20, 2], 'Atr': [10, 26, 50], 'Ma': [10, 20, 0]}
    setting['fas3'] = {'Boll': [20, 2], 'Atr': [10, 26, 50], 'Ma': [10, 20, 0]}

    engine.initStrategy(HdmatStrategy, setting)

    # 开始跑回测
    engine.runBacktesting()

    # 显示回测结果
    engine.showBacktestingResult()

    ## 跑优化

    # import time
    # start = time.time()

    ## 运行单进程优化函数，自动输出结果，耗时：359秒


    # print u'耗时：%s' %(time.time()-start)