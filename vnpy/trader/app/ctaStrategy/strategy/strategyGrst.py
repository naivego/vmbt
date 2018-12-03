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
        self.ctaEngine.intedsgn.tdkopset = setting['tdkopset']
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
            # plotsdk(ctaEngine.Mida, symbol=trdvar, disfactors=[''], has2wind=False, period=Period)
        except:
            ctaEngine.Mida = None
            print 'ctaEngine.Mida load_Dombar error'
        self.Teda = None
        self.vada1 = None
        self.Marst = None
        self.vada2 = None
        self.Surst = None
        self.tedaet = setting['tedaet']

        self.skatetl = None

        # -------------------vada0
        if len(self.tedaet)>0:
            try:
                Period = self.setting['msdpset']['te']
                self.vada0 = load_Dombar(trdvar, Period, Time_Param, Datain=Datain, Host=Host, DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)
                plotsdk(self.vada0, symbol=trdvar, disfactors=[''], has2wind=False, period=Period)
                self.Teda = Barda(trdvar, Period, self.tedaOnbar)
                self.Teda.dat = self.vada0
                self.tedaInit()
            except:
                self.vada0 = None

        # -------------------vada1
        try:
            Period = self.setting['msdpset']['ma']
            self.vada1 = load_Dombar(trdvar, Period, Time_Param, Datain=Datain, Host=Host, DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)
            plotsdk(self.vada1, symbol=trdvar, disfactors=[''], has2wind=False, period=Period)
            self.Marst = Grst_Factor(self.ctaEngine, trdvar, Period, self.vada1, fid='ma', teda = self.Teda)
            self.Marst.grst_init(setting=setting, btconfig=TS_Config)
        except:
            self.vada1 = None

        # -------------------vada2
        try:
            Period = self.setting['msdpset']['su']
            self.vada2 = load_Dombar(trdvar, Period, Time_Param, Datain=Datain, Host=Host, DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)
            plotsdk(self.vada2, symbol=trdvar, disfactors=[''], has2wind=False, period=Period)
            self.Surst = Grst_Factor(self.ctaEngine, trdvar, Period, self.vada2, fid='su', teda = self.Teda)
            self.Surst.grst_init(setting=setting, btconfig=TS_Config)
        except:
            self.vada2 = None


        print 'strategy init finished'
    #------------------------------------------------------------------------------------------
    # 将 Marst和Surst中的信号映射到Teda中
    def tedaInit(self):
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

        Dat_bar = pd.DataFrame(index=skdata.index)
        Dat_bar['TR1'] = skdata['high'] - skdata['low']
        Dat_bar['TR2'] = abs(skdata['high'] - skdata['close'].shift(1))
        Dat_bar['TR3'] = abs(skdata['low'] - skdata['close'].shift(1))
        TR = Dat_bar.loc[:, ['TR1', 'TR2', 'TR3']].max(axis=1)
        ATR = TR.rolling(14).mean() / skdata['close'].shift(1)
        self.sk_atr = ATR
        self.sk_ckl = []


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
        self.crtidtm = self.sk_time[skbgi]

        self.Teda.crtnum = self.crtski + 1
        self.Teda.crtidx = self.sk_time[self.Teda.crtnum]
        if len(self.tedaet):
            self.ctaEngine.intedsgn.skatl['te'] = Skatline(self.sk_time, self.sk_open, self.sk_high, self.sk_low, self.sk_close, self.sk_volume, self.sk_atr, self.sk_ckl)
            self.skatetl = self.ctaEngine.intedsgn.skatl['te']

    # ----------------------------------------------------------------------
    # 将 Marst和Surst中的信号映射到Teda上
    def tedasgn(self, sgndat, sgnids, fid='ma', fillna = False):
        xsgns = [isgn + '_' + fid for isgn in sgnids]
        extqts = []
        for isgn in xsgns:
            if isgn in sgndat.columns:
                extqts.append(isgn)
                if 'ak_' + isgn in sgndat.columns:
                    extqts.append('ak_' + isgn)
        if len(extqts) == 0:
            return
        toaddf = sgndat.loc[:, extqts]
        sindex = self.Teda.dat.index
        reindex = []
        for dtm in toaddf.index:
            redtm = sindex[sindex <= dtm][-1]
            reindex.append(redtm)
        toaddf.index = reindex
        self.Teda.dat = pd.concat([self.Teda.dat, toaddf], axis=1, join_axes=[self.Teda.dat.index])
        if fillna:
            self.Teda.dat.fillna(method='pad', inplace=True)
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

        #----------------------------------------------------------------

        #----------------------------------------------------------------
        for fa in self.tedaet:
            if fa == 'ma':
                farst = self.Marst

            elif fa == 'su':
                farst = self.Surst

            else:
                farst = None

            if farst:
                farst.teofi += 1
                if len(farst.sadlines) > 0:
                    subcrtsal = farst.sadlines.values()[-1]
                    self.skatetl.uptsta(subcrtsal, i, farst.crtski, farst.teofi, farst.teofn)
                if len(farst.sadlines) > 1:
                    subpresal = farst.sadlines.values()[-2]
                    self.skatetl.uptsta(subpresal, i, farst.crtski, farst.teofi, farst.teofn)

                supls = farst.suplines.keys()
                for tdlna in supls[-3:]:
                    bbls = farst.suplines[tdlna]
                    rdl = bbls['rdl']
                    mdl = bbls['mdl']
                    upls = bbls['upl']
                    mirs = bbls['mir']
                    if rdl:
                        self.skatetl.uptsta(rdl, i, farst.crtski, farst.teofi, farst.teofn)
                    if mdl:
                        self.skatetl.uptsta(mdl, i, farst.crtski, farst.teofi, farst.teofn)

                resls = farst.reslines.keys()
                for tdlna in resls[-3:]:
                    ttls = farst.reslines[tdlna]
                    rdl = ttls['rdl']
                    mdl = ttls['mdl']
                    dwls = ttls['dwl']
                    mirs = ttls['mir']
                    if rdl:
                        self.skatetl.uptsta(rdl, i, farst.crtski, farst.teofi, farst.teofn)
                    if mdl:
                        self.skatetl.uptsta(mdl, i, farst.crtski, farst.teofi, farst.teofn)

                self.ctaEngine.intedsgn.etsgnbs(fa, i, farst.crtski, farst.teofi, farst.teofn)
                self.ctaEngine.sgntotrd(fa, 'et', i, farst.crtski, farst.teofi, farst.teofn)
    # ----------------------------------------------------------------------
    def showfas(self, showset = {}):

        self.Marst.colfas()
        self.Surst.colfas()

        self.tedasgn(self.Surst.quotes, ['tekn'], fid='su', fillna = True)
        self.tedasgn(self.Marst.quotes, ['tekn'], fid='ma', fillna = True)

        extfas = ['disrst', 'sal', 'brdl', 'trdl', 'bmdl', 'tmdl']     # ['disrst', 'sal', 'brdl', 'trdl', 'bmdl', 'tmdl']
        self.tedasgn(self.Surst.quotes, extfas, fid='su')
        self.tedasgn(self.Marst.quotes, extfas, fid='ma')

        afc = []
        for col in self.Teda.dat.columns:
            if 'ak_' + col in self.Teda.dat.columns:
                afc.append(col)

        for col in afc:
            self.Teda.dat.loc[:, 'ak_' + col].fillna(method='pad', inplace=True)
        if len(afc) > 0:
            fidtm = self.Teda.dat.index[0]
            for idtm in self.Teda.dat.index[1:]:
                for col in afc:
                    fid = col.split('_')[-1]
                    if np.isnan(self.Teda.dat.loc[idtm, col]):
                        self.Teda.dat.loc[idtm, col] = self.Teda.dat.loc[fidtm, col] + self.Teda.dat.loc[
                            fidtm, 'ak_' + col] * 1.0 / self.Teda.dat.loc[fidtm, 'tekn_'+fid]
                fidtm = idtm

        self.Teda.dat.fillna(method='pad', inplace=True)
        quotesk = self.Teda.dat

        shwmafas = [fas + '_ma' for fas in extfas]

        shwsufas = [fas + '_su' for fas in extfas]

        plotsdk(quotesk, symbol=self.Teda.var, disfactors=shwmafas+shwsufas)

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
    def onBar(self, bar, ski= 0, islastbar = False):
        """收到Bar推送（必须由用户继承实现）"""
        # 撤销之前发出的尚未成交的委托（包括限价单和停止单）
        print 'onBar: ', bar.datetime
        if bar.close < 1:
            return
        if bar.datetime >= '2017-07-05':
            print 'chk'

        self.Marst.bada.newbar(bar, islastbar)
        self.Surst.bada.newbar(bar, islastbar)
        if self.Teda:
            self.Teda.newbar(bar, islastbar)

        #----------------------------------
        # 撮合前，若有信号更新需要相应的将原来的信号清除， 涉及4种信号 maetl, masel, suetl, susel



        self.ctaEngine.crossords(bar, ski)

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
    TS_Config['Time_Param'] = ['2014-03-05', '2017-04-15']
    TS_Config['SlipT'] = 0
    TS_Config['OrdTyp'] = {'open': 'Lmt', 'close': 'Lmt'}  # ['Mkt', 'Lmt', 'Stp']
    TS_Config['MiniT'] = 'M'


    setting = {}
    setting['msdpset'] = {'te': 'M15', 'ma': 'M30', 'su': 'd'}


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

    setting['tedaet'] = ['ma', 'su']
    setting['tdkopset'] = {
        'ma': {
            'sekop': {'sal': 1, 'rdl': 1, 'mdl': 1},
            'etkop': {'sal': 1, 'rdl': 1, 'mdl': 1}
        },
        'su': {
            'sekop': {'sal': 0, 'rdl': 1, 'mdl': 1},
            'etkop': {'sal': 0, 'rdl': 1, 'mdl': 1}
        }
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
        engine.Show_SaveResult()
