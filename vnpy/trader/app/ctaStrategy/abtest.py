# -*- coding: utf-8 -*-

# 2018-05-18 单品种TS测试模块
# ------------------------------------------------------------------------------
from __future__ import division
import pandas as pd
import numpy as np
from copy import deepcopy
import os
from datetime import datetime, timedelta
from time import localtime, strftime
import csv
import json
import logging
import collections

from barLoader import *
# import shutil
# from scipy.interpolate import griddata
# import matplotlib
# import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
# from matplotlib import cm
# from matplotlib.ticker import LinearLocator, FormatStrFormatter
# from matplotlib.pylab import date2num

from afactor import *
from factosdp import *
from ctaBase import *

# ----------------------------------------------------------------------
class Order(object):
    # ----------------------------------------------------------------------
    def __init__(self, Symbol, OrdSize, OrdAmount, OrdPrice, OrdTyp, OrdTime, Offset, OrdSki=None, OrdSp = None, OrdTp = None, Psn=None, Msn = None, Ordid=None, Mso=None, OrdFlg='NB'):
        """Constructor"""
        self.Symbol = Symbol
        self.OrdSize = OrdSize
        self.OrdAmount = OrdAmount
        self.OrdPrice = OrdPrice
        self.OrdTyp = OrdTyp
        self.OrdTime = OrdTime
        self.Offset = Offset
        self.OrdSki = OrdSki
        self.OrdSp = OrdSp
        self.OrdTp = OrdTp
        self.Psn = Psn
        self.Msn = Msn
        self.Ordid = Ordid
        self.Mso = Mso
        self.OrdFlg = OrdFlg

# ----------------------------------------------------------------------
class Trdord(object):
    # ----------------------------------------------------------------------
    def __init__(self, Symbol, TrdSize, TrdPrice, TrdTime, Offset, Trdski=None, TrdSp = None, TrdTp = None, Psn=None, Msn = None, Trdid=None, TrdFlg='NB'):
        """Constructor"""
        self.Symbol = Symbol
        self.TrdSize = TrdSize
        self.TrdPrice = TrdPrice
        self.TrdTime = TrdTime
        self.Offset = Offset
        self.Trdski = Trdski
        self.TrdSp = TrdSp
        self.TrdTp = TrdTp
        self.Psn = Psn
        self.Msn = Msn
        self.Trdid = Trdid
        self.TrdFlg = TrdFlg

class Position(object):
    # ----------------------------------------------------------------------
    def __init__(self, OpenTrd):
        """Constructor"""
        self.Symbol = OpenTrd.Symbol
        self.EntSize = OpenTrd.TrdSize
        self.EntPrice = OpenTrd.TrdPrice
        self.EntTime = OpenTrd.TrdTime
        self.EntSki = OpenTrd.Trdski
        self.EntSp = OpenTrd.TrdSp
        self.EntTp = OpenTrd.TrdTp
        self.Psn = OpenTrd.Psn
        self.Msn = OpenTrd.Msn
        self.MktValue = 0
        self.Margin = 0
        self.PAL = 0
        self.CldPrice = 0
        self.CldTime = 0
        self.CldFlg = None
        self.EntFlg = OpenTrd.TrdFlg
        self.Posid = OpenTrd.Trdid
        self.SpFlg = None
        self.TpFlg = None

class Sgnposinf(object):
    # ----------------------------------------------------------------------
    def __init__(self, SgnFlg, Symbol):
        self.SgnFlg = SgnFlg
        self.Symbol = Symbol
        self.EntNum = 0

        self.Lg_Vol = 0
        self.Lg_EntNum = 0
        self.Lg_Avg = 0
        self.Lg_MinEntp = None
        self.Lg_MaxEntp = None
        self.Lg_Sum = 0.0


        self.St_Vol = 0
        self.St_EntNum = 0
        self.St_Avg = 0
        self.St_MinEntp = None
        self.St_MaxEntp = None
        self.St_Sum = 0.0

    def addpos(self, pos):
        if pos.Symbol != self.Symbol or pos.EntFlg.split('_')[0] != self.SgnFlg:
            return
        self.EntNum += 1
        if pos.EntSize > 0:
            self.Lg_Sum += pos.EntSize * pos.EntPrice
            self.Lg_Vol += pos.EntSize
            self.Lg_Avg = self.Lg_Sum/self.Lg_Vol
            self.Lg_EntNum += 1
            self.Lg_MinEntp = min(self.Lg_MinEntp, pos.EntPrice) if self.Lg_MinEntp else pos.EntPrice
            self.Lg_MaxEntp = max(self.Lg_MaxEntp, pos.EntPrice) if self.Lg_MaxEntp else pos.EntPrice
        elif pos.EntSize < 0:
            self.St_Sum += pos.EntSize * pos.EntPrice
            self.St_Vol += pos.EntSize
            self.St_Avg = self.St_Sum / self.St_Vol
            self.St_EntNum += 1
            self.St_MinEntp = min(self.St_MinEntp, pos.EntPrice) if self.St_MinEntp else pos.EntPrice
            self.St_MaxEntp = max(self.St_MaxEntp, pos.EntPrice) if self.St_MaxEntp else pos.EntPrice




class Lspsd(object):
    # ----------------------------------------------------------------------
    def __init__(self, Lgvar, Lgsize, Lgid, Stvar, Stsize, Stid, Lscorr, Lsflg, Sddtm, Offset='close'):
        """Constructor"""
        self.Lgvar = Lgvar
        self.Lgsize = Lgsize
        self.Lgid = Lgid
        self.Stvar = Stvar
        self.Stsize = Stsize
        self.Stid = Stid
        self.Lscorr = Lscorr
        self.Lsflg = Lsflg
        self.Sddtm = Sddtm
        self.Offset = Offset


class Sglsd(object):
    # ----------------------------------------------------------------------
    def __init__(self, Sdvar, Sdsize, Sdid, Sdflg, Sddtm, Offset='close'):
        """Constructor"""
        self.Sdvar = Sdvar
        self.Sdsize = Sdsize
        self.Sdid = Sdid
        self.Sdflg = Sdflg
        self.Sddtm = Sddtm
        self.Offset = Offset


# ----------------------------------------------------------------------
def max_drawdown(Networth_Series):
    Series = Networth_Series[:-1]
    Size = Series.size
    Max = np.zeros(Size)
    Drawdown = np.zeros(Size)
    Drawdown_Duration = np.zeros(Size)
    Recovery_Duration = None
    Max_Drawdown = 0
    Ind = 0

    for t in range(Size):
        Max[t] = max(Max[t - 1], Series[t])
        Drawdown[t] = float(Series[t] - Max[t]) / Max[t]
        if Drawdown[t] < Max_Drawdown:
            Max_Drawdown = Drawdown[t]
            Ind = t
        Drawdown_Duration[t] = (0 if Drawdown[t] >= 0 else Drawdown_Duration[t - 1] + 1)

    if Ind is not 0:
        Temp_Max = map(lambda x: x - Max[Ind], Max[Ind:])
        for i in range(len(Max[Ind:])):
            if Temp_Max[i] > 0:
                Recovery_Duration = i
                break

        redic = {'Max': Max, 'Max_Drawdown': Drawdown[Ind], 'Drawdown_Duration': Drawdown_Duration[Ind],
            'Recovery_Duration': Recovery_Duration}
    else:
        redic = {'Max': Max, 'Max_Drawdown': np.nan, 'Drawdown_Duration': np.nan, 'Recovery_Duration': Recovery_Duration}

    return redic


# ----------------------------------------------------------------------
def return_metrics(Networth_Series, Trading_Days_Per_Year=242):
    Num_Days = Networth_Series.size
    if Num_Days<=1:
        return {'Daily_Return': np.nan, 'Annualized_Return': np.nan, 'Annualized_Std': np.nan,
            'Sharpe_Ratio': np.nan, 'Sortino_Ratio': np.nan, 'Winning_Rate': np.nan,
            'Num_Days': Num_Days}

    Daily_Return = (Networth_Series / Networth_Series.shift() - 1)[1:]
    Winning_Rate = Daily_Return[Daily_Return > 0].size / Daily_Return.dropna().size
    Mean_Daily_Return = Daily_Return.mean()
    Annualized_Return = np.power((Mean_Daily_Return + 1), Trading_Days_Per_Year) - 1
    Annualized_Std = Daily_Return.std() * np.sqrt(Trading_Days_Per_Year)
    Sharpe_Ratio = Annualized_Return / Annualized_Std
    Sortino_Ratio = Annualized_Return / (Daily_Return[Daily_Return < 0].std() * np.sqrt(Trading_Days_Per_Year))

    return {'Daily_Return': Daily_Return, 'Annualized_Return': Annualized_Return, 'Annualized_Std': Annualized_Std,
            'Sharpe_Ratio': Sharpe_Ratio, 'Sortino_Ratio': Sortino_Ratio, 'Winning_Rate': Winning_Rate,
            'Num_Days': Num_Days}


# ----------------------------------------------------------------------
def calc_metrics(Networth_Series, Trading_Days_Per_Year=242):
    Drawdown_Metrics_Dict = max_drawdown(Networth_Series)
    Return_Metrics_Dict = return_metrics(Networth_Series, Trading_Days_Per_Year=Trading_Days_Per_Year)
    Drawdown_Metrics_Dict.update(Return_Metrics_Dict)
    Drawdown_Metrics_Dict['Calmar_Ratio'] = Drawdown_Metrics_Dict['Annualized_Return'] / abs(
        Drawdown_Metrics_Dict['Max_Drawdown'])
    return Drawdown_Metrics_Dict


class TSBacktest(object):
    # ----------------------------------------------------------------------
    # def __init__(self, Var, sk_open, sk_high, sk_low, sk_close, sk_volume, sk_time, sk_atr, sk_ckl, TS_Config):
    def __init__(self,  TS_Config):  #quotes, sk_ckl,
        self.Datain = TS_Config['Datain']
        self.Rt_Dir = TS_Config['Rt_Dir']
        self.DB_Rt_Dir = TS_Config['DB_Rt_Dir']
        self.Meta_Csv_Dir = self.Rt_Dir + '/vnpy/trader/' + 'CommodityMetaData.csv'

        self.Trade_Date_DT = pd.read_csv(self.Rt_Dir + '/vnpy/trader/' + 'CHN_Date.csv').iloc[:, 0]

        self.Host = TS_Config['Host']
        self.Result_Dir = self.Rt_Dir + '/TSBT_results/'
        self.Symbol = TS_Config['Symbol']
        self.Var = self.Symbol
        self.Variety_List = [self.Symbol]
        self.MiniT = TS_Config['MiniT']
        self.Mida = None

        Time_Param = TS_Config['Time_Param']
        self.Trade_Date_DT = self.Trade_Date_DT[self.Trade_Date_DT >= Time_Param[0]]
        self.Trade_Date_DT = self.Trade_Date_DT[self.Trade_Date_DT < Time_Param[1]].tolist()

        self.Start_Date = self.Trade_Date_DT[0]
        self.End_Date = self.Trade_Date_DT[-1]
        self.Time_Param = [self.Start_Date, self.End_Date]

        self.Return_Panel = None
        self.Return_Metric = {}

        self.intedsgn = Intsgnbs()
        self.OrdMkt_Dic = {}
        self.OrdLmt_Dic = {}
        self.OrdStp_Dic = {}


        self.SdOpen_dic = {}
        self.SdCld_dic = {}

        self.Remids = []  # 已经记录入备用队列的订单号


        # 回测相关
        Init_Capital = TS_Config['Init_Capital']
        self.Init_Capital = TS_Config['Init_Capital']
        self.SlipT = TS_Config['SlipT']
        self.Free_Money = self.Init_Capital
        self.Networth = self.Init_Capital
        self.Record_Frame = pd.DataFrame([[Init_Capital, 0, Init_Capital, 0, 0]], index=['Init'],
                                         columns=['Free', 'Margin', 'Networth', 'Max_Holding_Ratio','Net_Holding_Ratio'])
        self.Record_FrameD = self.Record_Frame.copy()
        self.Transaction_Rate = pd.Series([0.0] * len(self.Variety_List), index=self.Variety_List)
        self.Transaction_Rate_Same_Day = pd.Series([0.0] * len(self.Variety_List), index=self.Variety_List)
        self.Transaction_Fee = pd.Series([0.0] * len(self.Variety_List), index=self.Variety_List)
        self.Multiplier = pd.Series([1] * len(self.Variety_List), index=self.Variety_List)
        self.Margin_Rate = pd.Series([0.1] * len(self.Variety_List), index=self.Variety_List)
        self.Tick_Size = pd.Series([0.0] * len(self.Variety_List), index=self.Variety_List)
        self.Transaction_Cost = 0
        self.Slippage_Cost = 0
        self.OrdTypSet = TS_Config['OrdTyp']
        self.set_param(self.Meta_Csv_Dir)

        self.LogOrds = pd.DataFrame(
            columns=['OrdID', 'Symbol', 'Size', 'OrdAmount', 'EntPrice', 'Ordtyp', 'EntTime', 'Offset', 'OrdSp', 'OrdTp', 'OrdFlg'])
        self.LogOrdsnum = 0
        self.LogTrds = pd.DataFrame(
            columns=['TrdID', 'Symbol', 'Size', 'TrdAmount', 'TrdPrice', 'TrdTime', 'Offset', 'TrdSp', 'TrdTp', 'TrdFlg'])
        self.LogTrdsnum = 0
        self.LogCldPos = pd.DataFrame(
            columns=['PosID', 'Symbol', 'Size', 'EntAmount', 'EntPrice', 'EntTime', 'EntSp', 'EntTp', 'CldPrice', 'CldTime', 'PAL', 'EntFlg', 'CldFlg'])

        self.Position_List = []
        self.SgnVarPos_Dic = {}  # 仓位更新时统计各个策略的各个品种的持仓
        self.SgnVarLstSdop_Dic = {}  # 各个策略的各个品种最近的开仓信号
        self.SocPos_Dic = {} # 各信号源持仓汇总



        self.Opid = 0
        self.MktValue = 0
        self.Long_MktValue = 0
        self.Short_MktValue = 0
        self.Margin = 0
        self.PAL = 0
        self.CldPosition_List = []
        self.CldPAL = 0
        self.LongEnt_Df = None
        self.ShortEnt_Df = None
        self.testname = ''
        self.Ordslogfile = ''
        self.Trdslogfile = ''
        self.CldPoslogfile = ''
        self.NetValuefile = ''
        self.DNetValuefile = ''
        self.Remark = TS_Config['Remark']

    # ----------------------------------------------------------------------
    def write_to_csv_file(self, csvdir, Item_List):
        with open(csvdir, 'ab+') as Csvfile:
            Csv_Writer = csv.writer(Csvfile, delimiter=',')
            Csv_Writer.writerow(Item_List)
# ----------------------------------------------------------------------
    def set_param(self, Csv_Dir=None):
        self.set_transaction_rate(Csv_Dir)
        self.set_transaction_rate_same_day(Csv_Dir)
        self.set_transaction_fee(Csv_Dir)
        self.set_multiplier(Csv_Dir)
        self.set_margin_rate(Csv_Dir)
        self.set_tick_size(Csv_Dir)

    def set_transaction_rate(self, Csv_Dir, Col_Nam='PercentageFee', Index_Col=0):
        Transaction_Rate = pd.read_csv(Csv_Dir, index_col=Index_Col).loc[:, Col_Nam]
        for x in self.Variety_List:
            self.Transaction_Rate[x] = Transaction_Rate[x.lower()]

    def set_transaction_rate_same_day(self, Csv_Dir, Col_Nam='PercentageFeeSameDay', Index_Col=0):
        Transaction_Rate_Same_Day = pd.read_csv(Csv_Dir, index_col=Index_Col).loc[:, Col_Nam]
        for x in self.Variety_List:
            self.Transaction_Rate_Same_Day[x] = Transaction_Rate_Same_Day[x.lower()]

    def set_transaction_fee(self, Csv_Dir, Col_Nam='FixFee', Index_Col=0):
        Transaction_Fee = pd.read_csv(Csv_Dir, index_col=Index_Col).loc[:, Col_Nam]
        for x in self.Variety_List:
            self.Transaction_Fee[x] = Transaction_Fee[x.lower()]

    def set_multiplier(self, Csv_Dir, Col_Nam='Multiplier', Index_Col=0):
        Multiplier = pd.read_csv(Csv_Dir, index_col=Index_Col).loc[:, Col_Nam]
        for x in self.Variety_List:
            self.Multiplier[x] = Multiplier[x.lower()]

    def set_margin_rate(self, Csv_Dir, Col_Nam='LongMarginRate(%)', Index_Col=0):
        Margin_Rate = pd.read_csv(Csv_Dir, index_col=Index_Col).loc[:, Col_Nam]
        for x in self.Variety_List:
            self.Margin_Rate[x] = Margin_Rate[x.lower()]

    def set_tick_size(self, Csv_Dir, Col_Nam='TickSize', Index_Col=0):
        Tick_Size = pd.read_csv(Csv_Dir, index_col=Index_Col).loc[:, Col_Nam]
        for x in self.Variety_List:
            self.Tick_Size[x] = Tick_Size[x.lower()]
    # ----------------------------------------------------------------------

    def putStrategyEvent(self, name):
        """发送策略更新事件，回测中忽略"""
        pass

    # ----------------------------------------------------------------------
    def writeCtaLog(self, content):
        """记录日志"""
        # log = str(self.dt) + ' ' + content
        # self.logList.append(log)

        # 写入本地log日志
        logging.info(content)

    def writeCtaError(self, content):
        """记录异常"""
        self.output(content)
        self.writeCtaLog(content)

    def newBar(self, bar):
        """新的K线"""
        self.bar = bar
        self.dt = datetime.strptime(bar.datetime, '%Y-%m-%d %H:%M:%S')
        # self.crossLimitOrder()  # 先撮合限价单
        # self.crossStopOrder()  # 再撮合停止单
        self.strategy.onBar(bar)  # 推送K线到策略中
        # self.__sendOnBarEvent(bar)  # 推送K线到事件

    # ----------------------------------------------------------------------
    def newTick(self, tick):
        """新的Tick"""
        self.tick = tick
        self.dt = tick.datetime
        # self.crossLimitOrder()
        # self.crossStopOrder()
        self.strategy.onTick(tick)

    # ----------------------------------------------------------------------
    def initStrategy(self, strategyClass, setting=None):
        """
        初始化策略
        setting是策略的参数设置，如果使用类中写好的默认设置则可以不传该参数
        """
        self.strategy = strategyClass(self, setting)
        if not self.strategy.name:
            self.strategy.name = self.strategy.className

        self.strategy.onInit()
        self.strategy.onStart()

    # ----------------------------------------------------------------------
    def endOfDay(self, bar):
        self.update_posinfo(bar, dayendcal =True)
        pass
        # self.posmkv = 0
        # self.unsetpnl = 0
        # if len(self.longPosition) > 0:
        #     for t in self.longPosition:
        #         self.posmkv += t.price * abs(t.volume) * self.size
        #         self.unsetpnl += (setp - t.price) * abs(t.volume) * self.size
        # if len(self.shortPosition) > 0:
        #     for t in self.shortPosition:
        #         self.posmkv -= t.price * abs(t.volume) * self.size
        #         self.unsetpnl += (-setp + t.price) * abs(t.volume) * self.size
        # if self.capital > 0:
        #     self.leverage = self.posmkv / self.capital
        # else:
        #     self.leverage = 0
        #
        # dse = pd.Series()
        # dse['Posmkv'] = self.posmkv
        # dse['Networth'] = (self.capital + self.unsetpnl) / self.initCapital
        # dse['Margin'] = self.capital - self.avaliable
        # dse['Margpct'] = (self.capital - self.avaliable) / (self.capital + self.unsetpnl)
        # dse['Leverage'] = self.posmkv / (self.capital + self.unsetpnl)
        # dse.name = dtm
        # self.dailyinf = self.dailyinf.append(dse)

        # ----------------------------------------------------------------------


    def sendord(self, Soda, Symbol, Size, Amount, Price, OrdTyp='Mkt', EntTime=None, Offset='open', EntSki=None, OrdSp = None, OrdTp = None, Psn=None, Msn = None, Ordid=None, Mso=None, OrdFlg='NB'):
        # print 'sendord:', Symbol, Size, Amount, Price, OrdTyp, EntTime, Offset, OrdSp, OrdTp, OrdFlg
        self.LogOrdsnum += 1
        # self.LogOrds.loc[self.LogOrdsnum, :] = [self.LogOrdsnum, Symbol, Size, Amount, Price, OrdTyp, EntTime, Offset, OrdSp, OrdTp, OrdFlg]
        self.write_to_csv_file(self.Ordslogfile, [self.LogOrdsnum, Symbol, Size, Amount, Price, OrdTyp, EntTime, Offset, OrdSp, OrdTp, OrdFlg])
        if OrdTyp == 'Mkt':
            neword = Order(Symbol, Size, Amount, Price, OrdTyp, EntTime, Offset, OrdSki=EntSki, OrdSp = OrdSp, OrdTp = OrdTp, Psn=Psn, Msn = Msn, Ordid=Ordid, Mso=Mso, OrdFlg=OrdFlg)
            if Soda in self.OrdMkt_Dic:
                self.OrdMkt_Dic[Soda].append(neword)
            else:
                self.OrdMkt_Dic[Soda] = [neword]

        elif OrdTyp == 'Lmt':
            neword = Order(Symbol, Size, Amount, Price, OrdTyp, EntTime, Offset, OrdSki=EntSki, OrdSp = OrdSp, OrdTp = OrdTp, Psn=Psn, Msn = Msn, Ordid=Ordid, Mso=Mso, OrdFlg=OrdFlg)
            if Soda in self.OrdLmt_Dic:
                self.OrdLmt_Dic[Soda].append(neword)
            else:
                self.OrdLmt_Dic[Soda] = [neword]

        elif OrdTyp == 'Stp':
            neword = Order(Symbol, Size, Amount, Price, OrdTyp, EntTime, Offset, OrdSki=EntSki, OrdSp = OrdSp, OrdTp = OrdTp, Psn=Psn, Msn = Msn, Ordid=Ordid, Mso=Mso, OrdFlg=OrdFlg)
            if Soda in self.OrdStp_Dic:
                self.OrdStp_Dic[Soda].append(neword)
            else:
                self.OrdStp_Dic[Soda] = [neword]

    def newposition(self, OpenTrd):
        newpos = Position(OpenTrd)
        newpos.MktValue = OpenTrd.TrdSize * OpenTrd.TrdPrice * self.Multiplier[OpenTrd.Symbol]

        # newpos.Margin = abs(newpos.MktValue) * self.Margin_Rate[OpenTrd.Symbol]
        # self.MktValue += newpos.MktValue
        # self.Long_MktValue += max(0, newpos.MktValue)
        # self.Short_MktValue += min(0, newpos.MktValue)
        # self.Margin += newpos.Margin
        # self.PAL += newpos.PAL

        self.Position_List.append(newpos)
        newfee_cost = abs(OpenTrd.TrdSize) * self.Transaction_Fee[OpenTrd.Symbol] + abs(newpos.MktValue) * self.Transaction_Rate[OpenTrd.Symbol]
        # newfee_cost = 0
        self.Transaction_Cost += newfee_cost
        self.CldPAL -= newfee_cost

        if newpos.MktValue > 0:
            self.LongEnt_Df.at[OpenTrd.Symbol, 'entnum'] += 1
            self.LongEnt_Df.at[OpenTrd.Symbol, 'size'] += OpenTrd.TrdSize
            self.LongEnt_Df.at[OpenTrd.Symbol, 'mktvalue'] += newpos.MktValue
        elif newpos.MktValue < 0:
            self.ShortEnt_Df.at[OpenTrd.Symbol, 'entnum'] += 1
            self.ShortEnt_Df.at[OpenTrd.Symbol, 'size'] += OpenTrd.TrdSize
            self.ShortEnt_Df.at[OpenTrd.Symbol, 'mktvalue'] += newpos.MktValue

        pass

    def closeposition(self, CldTrd):  # 根据开仓单号来平仓持仓
        Tocldposlist = []
        for pos in self.Position_List:
            if pos.Posid == CldTrd.Trdid:
                if CldTrd.TrdSize == 0:
                    break
                if pos.EntSize * CldTrd.TrdSize < 0:
                    if abs(pos.EntSize) <= abs(CldTrd.TrdSize):
                        pos.CldPrice = CldTrd.TrdPrice
                        pos.CldTime = CldTrd.TrdTime
                        pos.PAL = pos.EntSize * (pos.CldPrice - pos.EntPrice) * self.Multiplier[pos.Symbol]
                        pos.CldFlg = CldTrd.TrdFlg
                        Tocldposlist.append(pos)
                        CldTrd.TrdSize += pos.EntSize
                        newfee_cost = abs(pos.EntSize) * self.Transaction_Fee[pos.Symbol] + abs(
                            pos.EntSize * pos.CldPrice * self.Multiplier[pos.Symbol]) * self.Transaction_Rate[pos.Symbol]
                        # newfee_cost = 0
                        self.Transaction_Cost += newfee_cost
                        self.CldPAL -= newfee_cost

                    elif abs(pos.EntSize) > abs(CldTrd.TrdSize):
                        pos.EntSize += CldTrd.TrdSize
                        newcldtrd = Trdord(pos.Symbol, - CldTrd.TrdSize, pos.EntPrice, pos.EntTime, Offset='open', Trdski=pos.EntSki,
                                           TrdSp=pos.EntSp, TrdTp=pos.EntTp, Psn=pos.Psn, Msn=pos.Msn, Trdid=pos.Posid, TrdFlg=pos.EntFlg)
                        newcldpos = Position(newcldtrd)
                        newcldpos.CldPrice = CldTrd.TrdPrice
                        newcldpos.CldTime = CldTrd.TrdTime
                        newcldpos.PAL = newcldpos.EntSize * (newcldpos.CldPrice - pos.EntPrice) * self.Multiplier[newcldpos.Symbol]
                        newcldpos.CldFlg = CldTrd.TrdFlg
                        self.CldPosition_List.append(newcldpos)
                        self.CldPAL += newcldpos.PAL
                        newfee_cost = abs(newcldtrd.TrdSize) * self.Transaction_Fee[newcldtrd.Symbol] + abs(
                            newcldtrd.TrdSize * newcldpos.CldPrice * self.Multiplier[newcldtrd.Symbol]) * self.Transaction_Rate[newcldtrd.Symbol]
                        # newfee_cost = 0
                        self.Transaction_Cost += newfee_cost
                        self.CldPAL -= newfee_cost
                break
        for pos in Tocldposlist:
            self.CldPosition_List.append(pos)
            self.CldPAL += pos.PAL
            self.Position_List.remove(pos)

            if pos.EntSize > 0:
                self.LongEnt_Df.at[pos.Symbol, 'entnum'] -= 1
                self.LongEnt_Df.at[pos.Symbol, 'size'] -= pos.EntSize
                self.LongEnt_Df.at[pos.Symbol, 'mktvalue'] -= pos.MktValue
            if pos.EntSize < 0:
                self.ShortEnt_Df.at[pos.Symbol, 'entnum'] -= 1
                self.ShortEnt_Df.at[pos.Symbol, 'size'] -= pos.EntSize
                self.ShortEnt_Df.at[pos.Symbol, 'mktvalue'] -= pos.MktValue

    def update_posinfo(self, bar, dayendcal = False):  # 更新统计持仓、盈亏、资产、保证金、杠杆水平
        sk_time  = bar.datetime
        idtime = sk_time[:10]

        self.MktValue = 0
        self.Long_MktValue = 0
        self.Short_MktValue = 0
        self.Margin = 0
        self.PAL = 0
        self.LongEnt_Df['mktvalue'] = 0
        self.ShortEnt_Df['mktvalue'] = 0
        self.SgnVarPos_Dic = {}
        self.SocPos_Dic = {}
        nullpvarlist = []  # 记录空值合约
        for pos in self.Position_List:
            Symbol = pos.Symbol
            Flag = pos.EntFlg
            Lastprice = bar.close
            pos.MktValue = pos.EntSize * Lastprice * self.Multiplier[Symbol]
            pos.Margin = abs(pos.MktValue) * self.Margin_Rate[Symbol]
            pos.PAL = pos.EntSize * (Lastprice - pos.EntPrice) * self.Multiplier[Symbol]
            if pos.PAL != pos.PAL:
                nullpvarlist.append(Symbol)
            self.MktValue += pos.MktValue
            self.Long_MktValue += max(0, pos.MktValue)
            self.Short_MktValue += min(0, pos.MktValue)
            self.Margin += pos.Margin
            self.PAL += pos.PAL
            if pos.MktValue > 0:
                self.LongEnt_Df['mktvalue'] += pos.MktValue
                self.ShortEnt_Df['mktvalue'] += pos.MktValue

            #------------------------------------------------------------统计各子策略中各信号类别持仓信息
            sgnid = '_'.join(Flag.split('_')[0:2])          # sgnid = bek3_se | bek3_et
            if sgnid in self.SgnVarPos_Dic:                 # SgnVarPos_Dic 基于信号类别来分组
                self.SgnVarPos_Dic[sgnid].addpos(pos)
            else:
                self.SgnVarPos_Dic[sgnid] = Sgnposinf(sgnid, Symbol)
                self.SgnVarPos_Dic[sgnid].addpos(pos)

            socna = Flag.split('-')[1]                      # socna = ma_sa_73_0 | ma_rdl_73
            if socna in self.SocPos_Dic:                    # SocPos_Dic 基于信号源来分组， {socna： {sgnid：[poslist]}
                if sgnid in self.SocPos_Dic[socna]:
                    self.SocPos_Dic[socna][sgnid].append(pos)
                else:
                    self.SocPos_Dic[socna][sgnid]=[pos]
            else:
                self.SocPos_Dic[socna]= {sgnid: [pos]}
            #------------------------------------------------------------------------------
        self.Free_Money = self.Init_Capital + self.PAL + self.CldPAL - self.Margin
        if self.CldPAL !=self.CldPAL:
            print 'mkv nan'
        self.Networth = self.Init_Capital + self.PAL + self.CldPAL
        maxRatio = (self.Long_MktValue - self.Short_MktValue) / self.Networth
        netRatio = (self.MktValue) / self.Networth
        if not dayendcal:
            self.Record_Frame.loc[idtime, :] = [self.Free_Money, self.Margin, self.Networth, maxRatio, netRatio]
        else:
            self.Record_FrameD.loc[idtime, :] = [self.Free_Money, self.Margin, self.Networth, maxRatio, netRatio]
        if len(nullpvarlist)>0:
            nullpvarlist.insert(0, idtime)
            self.write_to_csv_file(r'D:\temp\nullvar.csv', nullpvarlist)
        # print Date, 'Networth: ', self.Networth, 'maxRatio: ',maxRatio

    def newtrd(self, Ord, trdsize, trdp, trdtime, trdski=None):
        if Ord.Offset == 'open':
            self.Opid += 1
            Trd = Trdord(Ord.Symbol, trdsize, trdp, trdtime, Ord.Offset, trdski, Ord.OrdSp, Ord.OrdTp, Ord.Psn, Ord.Msn, self.Opid, Ord.OrdFlg)
            self.LogTrdsnum += 1
            # self.LogTrds.loc[self.LogTrdsnum, :] = [self.LogTrdsnum, Ord.Symbol, trdsize, Ord.OrdAmount, trdp, trdtime, Ord.Offset, Ord.OrdSp, Ord.OrdTp, Ord.OrdFlg]
            self.write_to_csv_file(self.Trdslogfile, [self.LogTrdsnum, Ord.Symbol, trdsize, Ord.OrdAmount, trdp, trdtime, Ord.Offset, Ord.OrdSp, Ord.OrdTp, Ord.OrdFlg])
            self.newposition(Trd)
        elif Ord.Offset == 'close':
            Trd = Trdord(Ord.Symbol, trdsize, trdp, trdtime, Ord.Offset, trdski, TrdSp = None, TrdTp = None, Psn = None, Msn = None, Trdid = Ord.Ordid, TrdFlg = Ord.OrdFlg)
            self.LogTrdsnum += 1
            # self.LogTrds.loc[self.LogTrdsnum, :] = [self.LogTrdsnum, Ord.Symbol, trdsize, Ord.OrdAmount, trdp, trdtime, Ord.Offset, None, None, Ord.OrdFlg]
            self.write_to_csv_file(self.Trdslogfile, [self.LogTrdsnum, Ord.Symbol, trdsize, Ord.OrdAmount, trdp, trdtime, Ord.Offset, None, None, Ord.OrdFlg])
            self.closeposition(Trd)

    # -------------------------------------------------------------------------------------------------------
    def save_result(self, Result_Dir=None):
        if not Result_Dir:
            Result_Dir = self.Result_Dir
        self.Return_Panel.Return.plot()
        plt.savefig(Result_Dir + '/Return.png')
        plt.close('all')
        #self.to_h5(Result=True)

        with open(Result_Dir + '/Return_Metric.csv', 'wb') as Csvfile:
            Row_Writer = csv.writer(Csvfile, delimiter=',')
            Header_Row = ['Num_Days', 'Annualized_Return', 'Annualized_Std', 'Max_Drawdown', 'Sharpe_Ratio']
            Row_Writer.writerow(Header_Row)
            Row_Writer.writerow([self.Return_Metric[x] for x in Header_Row])

    def Show_SaveResult(self):
        self.Record_Frame.to_csv(self.NetValuefile)
        self.Record_FrameD.to_csv(self.DNetValuefile)
        PosID = 0
        with open(self.CldPoslogfile, 'ab+') as Csvfile:
            Csv_Writer = csv.writer(Csvfile, delimiter=',')
            # Csv_Writer.writerow(['PosID','Symbol','Size','EntAmount','EntPrice','EntTime','EntSp','EntTp','CldPrice','CldTime','PAL','EntFlg'])
            for pos in self.CldPosition_List:
                PosID += 1
                Csv_Writer.writerow(
                    [pos.Posid, pos.Symbol, pos.EntSize, pos.EntSize * pos.EntPrice, pos.EntPrice, pos.EntTime, pos.EntSp, pos.EntTp, pos.CldPrice, pos.CldTime, pos.PAL, pos.EntFlg, pos.CldFlg])

        Networth_Series = self.Record_FrameD['Networth']
        Asmreport = calc_metrics(Networth_Series)

        Fact_cfg_str = 'grst'
        Rep_Dic = collections.OrderedDict()

        Rep_Dic['Time_Param'] = self.Time_Param
        Rep_Dic['SlipT'] = self.SlipT
        Rep_Dic['OrdTypSet'] = self.OrdTypSet

        Rep_Dic['Init_Capital'] = self.Init_Capital,
        Rep_Dic['Final_Networth'] = self.Networth
        Rep_Dic['Final_PAL'] = self.Networth - self.Init_Capital
        Rep_Dic['Final_PAL%'] = self.Networth / self.Init_Capital
        Rep_Dic['Trdsnum'] = self.LogTrdsnum
        Rep_Dic['Fee_Cost'] = self.Transaction_Cost

        Rep_Dic['Annualized_Return'] = Asmreport['Annualized_Return']
        Rep_Dic['Annualized_Std'] = Asmreport['Annualized_Std']
        Rep_Dic['Max_Drawdown'] = Asmreport['Max_Drawdown']
        Rep_Dic['Num_Days'] = Asmreport['Num_Days']
        Rep_Dic['Drawdown_Duration'] = Asmreport['Drawdown_Duration']
        Rep_Dic['Recovery_Duration'] = Asmreport['Recovery_Duration']
        Rep_Dic['Winning_Rate'] = Asmreport['Winning_Rate']
        Rep_Dic['Sharpe_Ratio'] = Asmreport['Sharpe_Ratio']
        Rep_Dic['Sortino_Ratio'] = Asmreport['Sortino_Ratio']
        Rep_Dic['Calmar_Ratio'] = Asmreport['Calmar_Ratio']
        Rep_Dic['Remark'] = self.Remark
        # 写入策略的特征参数
        try:
            Rep_Dic['Feargs'] = self.strategy.feargs_Dic
        except:
            Rep_Dic['Feargs'] = None

        with open(self.Result_Dir + '/' + self.testname + '_Report' + '.json', 'a') as Reportfile:
            json.dump(Rep_Dic, Reportfile, ensure_ascii=False, indent=4)

        matplotlib.style.use('ggplot')
        fig, (ax1, ax2) = plt.subplots(2, sharex=True)
        Res_Networth = self.Record_FrameD.loc[:, ['Networth']]
        Res_MkvRatio = self.Record_FrameD.loc[:, ['Max_Holding_Ratio', 'Net_Holding_Ratio']]

        Res_Networth.plot(ax=ax1, title='Total Networth')
        Res_MkvRatio.plot(ax=ax2, title='MkvRatio')
        xdate = [itime for itime in Res_Networth.index]
        def mydate(x, pos):
            try:
                return xdate[int(x)]
            except IndexError:
                return ''
        ax1.xaxis.set_major_formatter( ticker.FuncFormatter(mydate))
        plt.grid()
        plt.savefig(self.Result_Dir + '/' + self.testname + '_' + Fact_cfg_str + '.png')
        # plt.show()

    def initbacktest(self, ):
        if not (os.path.exists(self.Result_Dir)):
            os.makedirs(self.Result_Dir)

        self.testname = 'TS_' + strftime("%Y-%m-%d_%H_%M_%S", localtime())+ '_' + self.Var
        self.Ordslogfile = self.Result_Dir + '/' + self.testname + '_Ords' + '.csv'
        self.Trdslogfile = self.Result_Dir + '/' + self.testname + '_Trds' + '.csv'
        self.CldPoslogfile = self.Result_Dir + '/' + self.testname + '_CldPos' + '.csv'
        self.NetValuefile = self.Result_Dir + '/' + self.testname + '_NetValue' + '.csv'
        self.DNetValuefile = self.Result_Dir + '/' + self.testname + '_DNetValue' + '.csv'

        self.write_to_csv_file(self.Ordslogfile, self.LogOrds.columns)
        self.write_to_csv_file(self.Trdslogfile, self.LogTrds.columns)
        self.write_to_csv_file(self.CldPoslogfile, self.LogCldPos.columns)

        self.OrdMkt_Dic = {}
        self.OrdLmt_Dic = {}
        self.OrdStp_Dic = {}

        self.LongEnt_Df = pd.DataFrame([[0, 0, 0]], index=self.Variety_List, columns=['entnum', 'size', 'mktvalue'])
        self.ShortEnt_Df = pd.DataFrame([[0, 0, 0]], index=self.Variety_List, columns=['entnum', 'size', 'mktvalue'])

    # -------------------------------------------
    def runBacktesting(self):
        if type(self.Mida) == type(None):
            print 'Mida is None'
            return

        for i in range(0, self.Mida.index.size):
            if i==1085:
                print i

            idtm = self.Mida.index[i]
            self.bar = CtaBarData()
            ds = self.Mida.ix[i, :]
            self.bar.datetime = self.Mida.index[i]
            for k in ds.index:
                if k in self.bar.__dict__:
                    self.bar.__dict__[k] = ds[k]

            # 更新各级数据周期下的信号并生成订单
            islastbar = (i == self.Mida.index.size-1)
            self.strategy.onBar(self.bar, ski= i, islastbar=islastbar)

            #----------------------------------------------逐日盯市
            try:
                nxtdtmhh = self.Mida.index[i+1][11:13]
            except:
                nxtdtmhh = '09'
            if idtm[11:13] =='15' and (nxtdtmhh =='09' or nxtdtmhh =='21'):
                self.endOfDay(self.bar)

        # self.strategy.showfas()

    # -------------------------------------------
    def crossords(self, bar, ski=None):
        # ----------------------------------------------------------------------------------------------------撮合订单
        newtrdcnt = 0
        if True:
            for soda, Ords in self.OrdMkt_Dic.iteritems():
                # 过滤行情缺失
                if bar.open > 0 and bar.close > 0 and bar.low > 0 and bar.high > 0:
                    pass
                else:
                    print 'cross CurOrdMkt error: ', soda, bar.datetime
                    continue
                for Ord in Ords[:]:
                    try:
                        if Ord.Mso and Ord.Mso.trdsta >0:
                            Ords.remove(Ord)
                            continue
                        Var = Ord.Symbol
                        if Ord.OrdSize != 0:
                            trdsize = Ord.OrdSize
                        else:
                            trdsize = int(Ord.OrdAmount / (bar.open * self.Multiplier[Var]))
                        if trdsize > 0:
                            trdp = bar.open + self.SlipT * self.Tick_Size[Var]
                        elif trdsize < 0:
                            trdp = bar.open - self.SlipT * self.Tick_Size[Var]
                        else:
                            print 'cross CurOrdMkt error: ', Var, bar.datetime, ' trdp =', trdp
                            continue
                        # -----------成交后将ord.Mso.trdsta置为1(已经成交)，并删除委托单
                        self.newtrd(Ord, trdsize, trdp, bar.datetime, ski)
                        if Ord.Mso:
                            Ord.Mso.trdsta = 1
                        Ords.remove(Ord)
                        newtrdcnt += 1
                    except Exception, e:
                        print 'cross CurOrdMkt', e.message
                        pass
            # ------------------------------------------------------------
            for soda, Ords in self.OrdStp_Dic.iteritems():
                # 过滤行情缺失
                if bar.open > 0 and bar.close > 0 and bar.low > 0 and bar.high > 0:
                    pass
                else:
                    print 'cross CurOrdStp error: ', soda, bar.datetime
                    continue
                for Ord in Ords[:]:
                    try:
                        if Ord.Mso and Ord.Mso.trdsta >0:
                            Ords.remove(Ord)
                            continue
                        Var = Ord.Symbol
                        if Ord.OrdSize != 0:
                            trdsize = Ord.OrdSize
                        else:
                            trdsize = Ord.OrdAmount / (Ord.OrdPrice * self.Multiplier[Var])

                        if trdsize > 0 and bar.high >= Ord.OrdPrice:
                            trdp = max(Ord.OrdPrice, bar.open)
                        elif trdsize < 0 and bar.low <= Ord.OrdPrice:
                            trdp = min(Ord.OrdPrice, bar.open)
                        else:
                            continue
                        # -----------成交后将ord.Mso.trdsta置为1(已经成交)，并删除委托单
                        self.newtrd(Ord, trdsize, trdp, bar.datetime, ski)
                        if Ord.Mso:
                            Ord.Mso.trdsta = 1
                        Ords.remove(Ord)
                        newtrdcnt += 1
                    except Exception, e:
                        print 'cross CurOrdStp', e.message
                        pass

            # ------------------------------------------------------------
            for soda, Ords in self.OrdLmt_Dic.iteritems():
                # 过滤行情缺失
                if bar.open > 0 and bar.close > 0 and bar.low > 0 and bar.high > 0:
                    pass
                else:
                    print 'cross CurOrdLmt error: ', soda, bar.datetime
                    continue
                for Ord in Ords[:]:
                    try:
                        if Ord.Mso and Ord.Mso.trdsta >0:
                            Ords.remove(Ord)
                            continue
                        Var = Ord.Symbol
                        if Ord.OrdSize != 0:
                            trdsize = Ord.OrdSize
                        else:
                            trdsize = Ord.OrdAmount / (Ord.OrdPrice * self.Multiplier[Var])

                        if trdsize > 0 and Ord.OrdPrice >= bar.low:
                            trdp = min(Ord.OrdPrice, bar.open)
                        elif trdsize < 0 and Ord.OrdPrice <= bar.high:
                            trdp = max(Ord.OrdPrice, bar.open)
                        else:
                            continue
                        # -----------成交后将ord.Mso.trdsta置为1(已经成交)，并删除委托单
                        self.newtrd(Ord, trdsize, trdp, bar.datetime, ski)
                        if Ord.Mso:
                            Ord.Mso.trdsta = 1
                        Ords.remove(Ord)
                        newtrdcnt += 1
                    except Exception, e:
                        print 'cross CurOrdLmt', e.message
                        pass
        if newtrdcnt > 0:
            self.update_posinfo(bar)
    # ------------------------------------------------------------

    def sgntotrd(self, fid, seet, ski, eti=0, mosi = 0, mosn=1):  # fid = ma|su  seet = se|et
        soda = fid+seet
        if fid not in self.intedsgn.fas:
            return
        xfas = self.intedsgn.fas[fid]
        if seet=='et':
            xskl = self.intedsgn.skatl['te']
        else:
            xskl = self.intedsgn.skatl[fid]

        sk_open = xskl.sk_open
        sk_high = xskl.sk_high
        sk_low = xskl.sk_low
        sk_close = xskl.sk_close
        sk_volume = xskl.sk_volume
        sk_time = xskl.sk_time
        sk_atr = xskl.sk_atr
        atr = sk_atr[ski] * sk_close[ski]
        sk_ckl = xskl.sk_ckl
        kopsdic = xskl.trpkops

        etskl = self.intedsgn.skatl['te']
        etfid = etskl.fid
        etfas = self.intedsgn.fas[etfid]
        et_close = etskl.sk_close
        et_atr = xskl.sk_atr
        if eti>0:
            eatr = et_atr[eti] * et_close[eti]
        else:
            eatr = atr
        #-----------------------------------------------------------------
        idtime = sk_time[ski]
        print 'backtest: trade on ', idtime
        iski  = ski
        ihigh = sk_high[ski]
        if '2015-01-10 00:30:00' in idtime:
            print ' iski:', iski, 'crtdate:', idtime[:10], 'crttime:', idtime

        self.SdCld_dic = {}
        self.SdOpen_dic = {}

        #====================================================
        # sads--------------------------外部信号et
        etrstdir = etfas['rstdir']
        if len(etfas['upsas']) > 0:
            etuprs = etfas['upsas'].values()[-1]
        else:
            etuprs = None
        if len(etfas['dwsas']) > 0:
            etdwrs = etfas['dwsas'].values()[-1]
        else:
            etdwrs = None
        # sads-------------------------本部信号se
        serstdir = xfas['rstdir']
        if len(xfas['upsas']) > 0:
            seuprs = xfas['upsas'].values()[-1]
        else:
            seuprs = None
        if len(xfas['dwsas']) > 0:
            sedwrs = xfas['dwsas'].values()[-1]
        else:
            sedwrs = None

        # ====================================================
        # phds--------------------------外部信号et
        if len(etfas['phds']) > 0:
            etphd = etfas['phds'].values()[-1]
        else:
            etphd = None

        # phds-------------------------内部信号se
        if len(xfas['phds']) > 0:
            sephd = xfas['phds'].values()[-1]
        else:
            sephd = None


        self.intedsgn.cmbsgn(fid, seet, self.SocPos_Dic)  # 此处整合信号，并调整持仓的止损和止盈设置
        # 更新相应的发单信号

        self.OrdMkt_Dic[soda] = []
        self.OrdLmt_Dic[soda] = []
        self.OrdStp_Dic[soda] = []
        # --------------------------------------------------------平仓逻辑 止损 止盈
        for socna, sgnpos in self.SocPos_Dic.iteritems():
            socfid = socna.split('_')[0]
            if socfid != fid:
                continue
            for sgnid, poslist in sgnpos.iteritems():
                sgntyp = sgnid.split('_')[0]
                sgnseet = sgnid.split('_')[-1]
                if seet != sgnseet:
                    continue
                for pos in poslist:
                    entski = pos.EntSki
                    entsize = pos.EntSize
                    entprice = pos.EntPrice
                    entflg = pos.EntFlg
                    var = pos.Symbol
                    psn = pos.Psn
                    msn = pos.Msn
                    sgnna = entflg
                    # ===========================================================================止损单
                    sdsp = pos.EntSp
                    sdflg = pos.SpFlg
                    # --------------------------平保止损
                    pbsp = None
                    if psn:
                        if entsize > 0 and sk_close[ski] >= entprice + psn * atr:
                            pbsp = entprice + 0.5 * atr
                        elif entsize < 0 and sk_close[ski] <= entprice - psn * atr:
                            pbsp = entprice - 0.5 * atr

                    # --------------------------移动止损
                    msp = None
                    if msn:
                        if msn==1:
                            # --------------------------简单移动止损
                            if entsize > 0 and sdsp and sdsp < sk_close[ski] - msn * atr and entprice < sk_close[ski] - msn * atr:
                                msp = sk_close[ski] - msn * atr
                            elif entsize < 0 and sdsp and sdsp > sk_close[ski] + msn * atr and entprice > sk_close[ski] + msn * atr:
                                msp = sk_close[ski] + msn * atr
                        elif msn==2:
                            # --------------------------基于sads的移动止损
                            # 1、rss开仓信号止损采用本身周期的Rstsa移动止损
                            # 2、se|et开仓信号止损采用外部周期的Rstsa移动止损
                            mspsetyps = ['rsk1']
                            mspettyps = ['rek2', 'rek1', 'rek3', 'bek1', 'bek2', 'bek3', 'bek4']
                            if sgntyp in mspsetyps:
                                rstdir  = serstdir
                                crtuprs = seuprs
                                crtdwrs = sedwrs
                                catr = atr
                            else:
                                rstdir  = etrstdir
                                crtuprs = etuprs
                                crtdwrs = etdwrs
                                catr = eatr

                            if entsize > 0 and rstdir > 0 and crtuprs and len(crtuprs)>0:
                                upssd = crtuprs.values()[-1]
                                if upssd.mexsta >= 2:
                                    msp = upssd.mexp - 0.4 * catr
                                else:
                                    msp = upssd.cexp - 0.4 * catr
                            elif entsize < 0 and rstdir < 0 and crtdwrs and len(crtdwrs) > 0:
                                dwssd = crtdwrs.values()[-1]
                                if dwssd.mexsta >= 2:
                                    msp = dwssd.mexp + 0.4 * catr
                                else:
                                    msp = dwssd.cexp + 0.4 * catr

                        elif msn == 3:
                            # --------------------------基于phds的移动止损
                            # 1、phd开仓信号止损采用本身周期的crtphd移动止损
                            # 2、se|et开仓信号止损采用外部周期的crtphd移动止损
                            mspsetyps = ['psk2', 'bsk1', 'bsk3', 'bsk5']
                            mspettyps = ['rek2', 'rek1', 'rek3', 'bek1', 'bek2', 'bek3', 'bek4']
                            phddir = 0
                            if sgntyp in mspsetyps and sephd:
                                phddir = sephd.dirn/abs(sephd.dirn)
                                catr = atr
                                phdmsp = sephd.fbsp - phddir * catr * 1.0
                            elif sgntyp in mspettyps and etphd:
                                phddir = etphd.dirn/abs(etphd.dirn)
                                catr = eatr
                                phdmsp = etphd.fbsp - phddir * catr * 1.0

                            if entsize > 0 and phddir > 0:
                                msp = phdmsp
                            elif entsize < 0 and phddir < 0:
                                msp = phdmsp

                    # --------------------------信号止损
                    sgnbs = None
                    #---------------------------------
                    sgnsp = None
                    spflg = ''
                    if sgnbs:
                        if sgnbs.mark and entski > sgnbs.mark:
                            sgnsp = sgnbs.sdsp
                            spflg = sgnbs.sgnna

                    # --------------------------------------
                    if entsize > 0:
                        if not sdsp or (pbsp and sdsp < pbsp):
                            sdsp = pbsp
                            sdflg = 'pbsp'
                        if not sdsp or (msp and sdsp < msp):
                            sdsp = msp
                            sdflg = 'msp'
                        if not sdsp or (sgnsp and sdsp < sgnsp):
                            sdsp = sgnsp
                            sdflg = spflg

                    elif entsize < 0:
                        if not sdsp or (pbsp and sdsp > pbsp):
                            sdsp = pbsp
                            sdflg = 'pbsp'
                        if not sdsp or (msp and sdsp > msp):
                            sdsp = msp
                            sdflg = 'msp'
                        if not sdsp or (sgnsp and sdsp > sgnsp):
                            sdsp = sgnsp
                            sdflg = spflg

                    if not sdflg:
                        sdflg = 'sp0'
                    if sdsp and abs(sdsp - sk_close[ski]) < atr * 6:
                        spfid = 'pos_' + str(pos.Posid) + '_sp_' + sdflg
                        self.sendord(soda, var, -entsize, 0, sdsp, 'Stp', idtime, Offset='close', EntSki=iski, Ordid=pos.Posid, OrdFlg=spfid)
                        pos.EntSp = sdsp
                        pos.SpFlg = sdflg

                    # =========================================================================限价平仓止盈单
                    sdtp = pos.EntTp
                    sdflg = pos.TpFlg
                    # 移动止盈 (需要移动止盈的信号)
                    mtpsetyps = ['psk2', 'bsk1', 'bsk3', 'bsk5']
                    mtpettyps = []
                    mtp = None
                    phddir = 0
                    if sgntyp in mtpsetyps and sephd:
                        phddir = sephd.dirn / abs(sephd.dirn)
                        catr = atr
                        phdmtp = sephd.dbsp - phddir * catr * 0.0
                    elif sgntyp in mtpettyps and etphd:
                        phddir = etphd.dirn / abs(etphd.dirn)
                        catr = eatr
                        phdmtp = etphd.dbsp - phddir * catr * 0.0

                    if entsize > 0 and phddir < 0:
                        mtp = phdmtp
                    elif entsize < 0 and phddir > 0:
                        mtp = phdmtp

                    # --------------------------------------
                    if entsize > 0:
                        if mtp:
                            if not sdtp or sdtp > mtp:
                                sdtp = mtp
                                sdflg = 'mtp'
                    elif entsize < 0:
                        if mtp:
                            if not sdtp or sdtp < mtp:
                                sdtp = mtp
                                sdflg = 'mtp'

                    if not sdflg:
                        sdflg = 'tp0'
                    if sdtp and abs(sdtp - sk_close[ski]) < atr * 10:
                        tpfid = 'pos_' + str(pos.Posid) + '_tp_' + sdflg
                        self.sendord(soda, var, -entsize, 0, sdtp, 'Lmt', idtime, Offset='close', EntSki=iski, Ordid=pos.Posid, OrdFlg=tpfid)
                        pos.EntTp = sdtp
                        pos.TpFlg = sdflg
        # --------------------------------------------------------开仓逻辑
        for socna, sgnkops in self.intedsgn.cmbkops.iteritems():
            for sgnid, koplist in sgnkops.iteritems():
                for kop in koplist:
                    var = self.Symbol
                    sgnna= kop.sgnna
                    sdop = kop.sdop
                    sdsp = kop.sdsp
                    sdtp = kop.sdtp
                    psn = kop.psn
                    msn = kop.msn
                    oco = kop.oco
                    if kop.bsdir > 0:
                        stp = sdop - sdsp if sdsp else None
                        tpl = sdtp - sdop if sdtp else None
                    else:
                        stp = -sdop + sdsp if sdsp else None
                        tpl = -sdtp + sdop if sdtp else None

                    if abs(sdop - sk_close[ski]) > sk_atr[ski] * sk_close[ski] * 6:
                        continue

                    etcn = 1
                    iMsp = self.Networth * 0.1 / etcn
                    if iMsp != iMsp:
                        continue
                    imsize = round(iMsp * 1.0 / sdop / self.Multiplier[var])  # 单次开仓最大用1倍杠杆
                    # 通过风险比例分配策略下单量
                    iRsp = iMsp * 0.01   # 单次风险最大 1%
                    perstp = stp * self.Multiplier[var] if stp else None
                    ifsize = round(iRsp / perstp) if perstp else None

                    entsize = min(ifsize, imsize) if ifsize else imsize
                    # entsize = imsize
                    if entsize < 1:
                        continue
                    entsize = entsize * kop.bsdir
                    ordtyp = kop.ordtyp

                    self.sendord(soda, var, entsize, 0, sdop, ordtyp, idtime, Offset='open', EntSki=iski, OrdSp=sdsp, OrdTp=sdtp, Psn=psn, Msn=msn,
                                 Mso=oco, OrdFlg=sgnna)











