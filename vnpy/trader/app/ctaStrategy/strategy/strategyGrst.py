# encoding: UTF-8

"""
Grst
"""

import os
import sys

import numpy as np
import pandas as pd

from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate
from vnpy.trader.app.ctaStrategy.afactor import *
from vnpy.trader.app.ctaStrategy.factosdp import *
from vnpy.trader.app.ctaStrategy.abtest import *
# from vnpy.trader.app.ctaStrategy.barLoader import *
import talib

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
            # plotsdk(ctaEngine.Mida, symbol=trdvar, disfactors=[''], has2wind=False)
        except:
            ctaEngine.Mida = None

        self.Teda = None
        self.vada1 = None
        self.Marst = None
        self.vada2 = None
        self.Surst = None

        # -------------------vada0
        try:
            Period = self.setting['msdpset']['te']
            self.vada0 = load_Dombar(trdvar, Period, Time_Param, Datain=Datain, Host=Host, DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)
            plotsdk(self.vada0, symbol=trdvar, disfactors=[''], has2wind=False)
            self.Teda = Barda(trdvar, Period, self.tedaOnbar)
            self.Teda.dat = self.vada0
            # plotsdk(self.vada0, symbol=trdvar, disfactors=[''], has2wind=False)

        except:
            self.vada0 = None

        # -------------------vada1
        try:
            Period = self.setting['msdpset']['ma']
            self.vada1 = load_Dombar(trdvar, Period, Time_Param, Datain=Datain, Host=Host, DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)
            plotsdk(self.vada1, symbol=trdvar, disfactors=[''], has2wind=False)
            self.Marst = Grst_Factor(trdvar, Period, self.vada1, fid='ma')
            self.Marst.grst_init(setting=setting, btconfig=TS_Config)
        except:
            self.vada1 = None

        # -------------------vada2
        try:
            Period = self.setting['msdpset']['su']
            self.vada2 = load_Dombar(trdvar, Period, Time_Param, Datain=Datain, Host=Host, DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)
            plotsdk(self.vada2, symbol=trdvar, disfactors=[''], has2wind=False)
            self.Surst = Grst_Factor(trdvar, Period, self.vada2, fid='su')
            self.Surst.grst_init(setting=setting, btconfig=TS_Config)
        except:
            self.vada2 = None


        print 'strategy init finished'
    #------------------------------------------------------------------------------------------
    # 将 Marst和Surst中的信号映射到Teda中
    def tedaInit(self, i):
        print 'tedaInit'
        if type(self.Teda) is type(None):
            return
        skdata = self.Teda.dat
        self.sk_open = skdata['open'].values
        self.sk_high = skdata['high'].values
        self.sk_low = skdata['low'].values
        self.sk_close = skdata['close'].values
        self.sk_volume = skdata['volume'].values
        self.sk_time = skdata.index

        self.sk_ma = skdata['close'].rolling(10).mean()
        self.sk_mid = skdata['close'].rolling(20).mean()
        self.sk_std = skdata['close'].rolling(20).std()
        self.sk_upl = self.sk_mid + 2 * self.sk_std
        self.sk_dwl = self.sk_mid - 2 * self.sk_std

        Dat_bar = pd.Dataframe(index=skdata.index)
        Dat_bar['TR1'] = skdata['high'] - skdata['low']
        Dat_bar['TR2'] = abs(skdata['high'] - skdata['close'].shift(1))
        Dat_bar['TR3'] = abs(skdata['low'] - skdata['close'].shift(1))
        TR = Dat_bar.loc[:, ['TR1', 'TR2', 'TR3']].max(axis=1)
        ATR = TR.rolling(14).mean() / skdata['close'].shift(1)
        self.sk_atr = ATR
        self.sk_ckl = []
        self.dkcn = []
        self.teatmal = Skatline(self.sk_open, self.sk_high, self.sk_low, self.sk_close, self.sk_atr, self.sk_ckl, self.dkcn)
        self.teatsul = Skatline(self.sk_open, self.sk_high, self.sk_low, self.sk_close, self.sk_atr, self.sk_ckl, self.dkcn)

        skbgi = 20
        if self.sk_close.size <= skbgi:
            self.crtski = 0
            return
        for i in range(0, skbgi):
            self.sk_ckl.append(None)
        if self.sk_close[skbgi] >= self.sk_open[skbgi]:
            ckli = 1
            cksdh = self.sk_close[skbgi]
            cksdl = self.sk_open[skbgi]
            cklhp = self.sk_high[skbgi]
            ckllp = self.sk_low[skbgi]
        else:
            ckli = -1
            cksdl = self.sk_close[skbgi]
            cksdh = self.sk_open[skbgi]
            cklhp = self.sk_high[skbgi]
            ckllp = self.sk_low[skbgi]
        self.sk_ckl.append((ckli, cksdh, cksdl, cklhp, ckllp, skbgi))
        self.crtski = skbgi

    # ------------------------------------------------------------------------------------------
    # 将 Marst和Surst中的信号映射到Teda中
    def tedaOnbar(self, i):
        print 'tedaOnbar'
        self.crtski = i
        sk_ckdtpst = 0.05  # 0.05倍平均涨幅作为涨跌柱子的公差
        avgski = self.sk_atr[i - 1] * self.sk_close[i - 1]
        if self.sk_ckl[i - 1][0] > 0:
            if self.sk_close[i] >= self.sk_close[i - 1] - sk_ckdtpst * self.sk_atr[i - 1] * self.sk_close[
                        i - 1]:  # self.sk_open[i] - sk_ckdtpst * self.sk_atr[i]:  #
                ckli = self.sk_ckl[i - 1][0] + 1
                cksdh = max(self.sk_ckl[i - 1][1], self.sk_close[i])
                cksdl = self.sk_ckl[i - 1][2]
                cklhp = max(self.sk_ckl[i - 1][3], self.sk_high[i])
                ckllp = min(self.sk_ckl[i - 1][4], self.sk_low[i])
                cklbi = self.sk_ckl[i - 1][5]
                self.sk_ckl.append((ckli, cksdh, cksdl, cklhp, ckllp, cklbi))
            else:
                ckli = -1
                cksdh = self.sk_ckl[i - 1][1]
                cksdl = self.sk_close[i]
                cklhp = max(self.sk_high[i], cksdh)
                ckllp = self.sk_low[i]
                self.sk_ckl.append((ckli, cksdh, cksdl, cklhp, ckllp, i))

        else:
            if self.sk_close[i] <= self.sk_close[i - 1] + sk_ckdtpst * self.sk_atr[i - 1] * self.sk_close[
                        i - 1]:  # self.sk_open[i] + sk_ckdtpst * self.sk_atr[i]:  #
                ckli = self.sk_ckl[i - 1][0] - 1
                cksdl = min(self.sk_ckl[i - 1][2], self.sk_close[i])
                cksdh = self.sk_ckl[i - 1][1]
                cklhp = max(self.sk_ckl[i - 1][3], self.sk_high[i])
                ckllp = min(self.sk_ckl[i - 1][4], self.sk_low[i])
                cklbi = self.sk_ckl[i - 1][5]
                self.sk_ckl.append((ckli, cksdh, cksdl, cklhp, ckllp, cklbi))

            else:
                ckli = 1
                cksdl = self.sk_ckl[i - 1][2]
                cksdh = self.sk_close[i]
                cklhp = self.sk_high[i]
                ckllp = min(self.sk_low[i], cksdl)
                self.sk_ckl.append((ckli, cksdh, cksdl, cklhp, ckllp, i))






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

        self.Marst.bada.newbar(bar)
        self.Surst.bada.newbar(bar)
        if self.Teda:
            self.Teda.newbar(bar)



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
    TS_Config['Time_Param'] = ['2017-01-05', '2017-06-25']
    TS_Config['SlipT'] = 0
    TS_Config['OrdTyp'] = {'open': 'Lmt', 'close': 'Lmt'}  # ['Mkt', 'Lmt', 'Stp']
    TS_Config['MiniT'] = 'M'


    setting = {}
    setting['msdpset'] = {'te': 'M15','ma': 'M30', 'su': 'd'}


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
    setting['sfaset'] = {'sal': True, 'rdl': True, 'mdl': True, 'upl': False, 'dwl': False, 'mir': False}
    setting['makopset'] = {
        'sekop': {'sal': 0, 'rdl': 1, 'mdl': 0},
        'etkop': {'sal': 0, 'rdl': 1, 'mdl': 0}
    }
    setting['sukopset'] = {
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