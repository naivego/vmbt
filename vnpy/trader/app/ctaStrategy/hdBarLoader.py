# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 14:42:03 2017

@author: naievgo
"""

from __future__ import division
import pandas as pd
import numpy as np
import imp
import sys
import os

from datetime import datetime, timedelta


class Load_BarData(object):
    # ----------------------------------------------------------------------
    def __init__(self, ABD_Config):
        self.DB_Rt_Dir = ABD_Config['DB_Rt_Dir']
        self.Time_Param = ABD_Config['Time_Param']
        self.Start_Date = self.Time_Param[0]
        self.End_Date = self.Time_Param[1]
        self.Period_List = ABD_Config['Period_List']
        self.First_Freq_Col_Param = ABD_Config['First_Freq_Col_Param']
        self.Other_Freq_Col_Param = ABD_Config['Other_Freq_Col_Param']
        sys.path.insert(0, self.DB_Rt_Dir + '/code/db_app.py')
        sys.path.insert(0, self.DB_Rt_Dir + '/code/')

        self.DB_App = getattr(imp.load_source('DB_App', self.DB_Rt_Dir + '/code/db_app.py'), 'DB_App')(self.DB_Rt_Dir)
        self.Panel_Data = None
        self.Variety_Index = None
        self.Date_Index = None
        self.Column_Index = None
        self.Sup_Signal_Col_List = None
        self.Filtered = False
        self.Filtered_Panel_Data = None
        self.Filtered_Dict_Data = {}
        self.Return_Dict = {}
        self.Data_Folder_List = []
        self.DF_Data = None

    def load_bar(self, Var_List=None, Dom = 'DomContract'):
        Start_Time = datetime.now()
        self.Panel_Data = self.DB_App.load_domContract_data_in_time(Var_List, self.Time_Param,
                                                                    self.First_Freq_Col_Param,
                                                                    self.Other_Freq_Col_Param, self.Period_List, Fctr_Name = Dom )
        print 'Loading panel data from database took:', (datetime.now() - Start_Time).total_seconds(), 's.'
        for x in self.Panel_Data.items:
            if self.Panel_Data[x].empty:
                self.Panel_Data.drop(x, inplace=True)
        self.Variety_Index = self.Panel_Data.items
        self.Date_Index = self.Panel_Data.major_axis
        self.Column_Index = self.Panel_Data.minor_axis
        # self.mark_date_on_panel()

    # ----------------------------------------------------------------------
    def clean_data(self, Col_List):
        for Col in Col_List:
            self.Panel_Data.loc[:, :, Col].replace(0, np.nan, inplace=True)

    # ----------------------------------------------------------------------
    def apply_adj_factor(self):
        Mult_List = [Minor for Minor in self.Column_Index if Minor.split('_')[0] in ['Open', 'High', 'Low', 'Close']]
        Join_List = list(set(self.Column_Index) - set(Mult_List))
        # self.Panel_Data.loc[:, :, 'AdjFactor'] = 1
        self.Panel_Data.update(self.Panel_Data.loc[:, :, Mult_List].mul(self.Panel_Data.loc[:, :, 'AdjFactor'], axis=2))

    # ----------------------------------------------------------------------
    def add_column(self, Column_Name, Func, Input_Col_List=[], **kwargs):
        self.Panel_Data.loc[:, :, Column_Name] = pd.DataFrame()
        for Variety in self.Variety_Index:
            Curr_Data = Func(self.Panel_Data.loc[Variety, :, Input_Col_List], **kwargs)
            if type(Curr_Data) == pd.DataFrame:
                self.Panel_Data.loc[Variety, :, Column_Name] = Curr_Data.iloc[:, 0]
            else:
                self.Panel_Data.loc[Variety, :, Column_Name] = Curr_Data

    # ----------------------------------------------------------------------
    def add_columns(self, Column_Name_List, Func, Input_Col_List=[], **kwargs):
        for Column_Name in Column_Name_List:
            self.Panel_Data.loc[:, :, Column_Name] = pd.DataFrame()
        for Variety in self.Variety_Index:
            Curr_Data = Func(self.Panel_Data.loc[Variety, :, Input_Col_List], Column_Name_List, **kwargs)
            for Column_Name in Column_Name_List:
                self.Panel_Data.loc[Variety, :, Column_Name] = Curr_Data[Column_Name]

    # ----------------------------------------------------------------------
    def add_df(self, Column_Name, Func, Input_Col_List=[], **kwargs):
        self.Panel_Data.loc[:, :, Column_Name] = pd.DataFrame()
        self.Panel_Data.loc[:, :, Column_Name] = Func(self.Panel_Data.loc[:, :, Input_Col_List], **kwargs)

    # ----------------------------------------------------------------------
    def set_sup_signal_col_list(self, Sup_Signal_Col_List):
        self.Sup_Signal_Col_List = Sup_Signal_Col_List
        for Variety in self.Variety_Index:
            self.Panel_Data[Variety].loc[:, self.Sup_Signal_Col_List] = self.Panel_Data[Variety].loc[:,
                                                                        self.Sup_Signal_Col_List].shift(1)

    # ----------------------------------------------------------------------
    def filter_by_column_value(self, Func, Separate=False, **kwargs):
        if not self.Filtered:
            if Separate:
                for Variety in self.Variety_Index:
                    self.Filtered_Dict_Data[Variety] = Func(self.Panel_Data[Variety], **kwargs)
            else:
                for Variety in self.Variety_Index:
                    self.Filtered_Panel_Data.loc[Variety, :, :] = Func(self.Panel_Data[Variety], **kwargs)
            self.Filtered = True
        else:
            pass

    # ----------------------------------------------------------------------

 # Dom = 'DomContract' | 'DomContractOi' | 'CrossContract'  | 'CrossContractOi' | 'DomContract2' | 'DomContractOi2' | 'CrossContract2'  | 'CrossContractOi2'
def load_Dombar( Var, Period, Time_Param, DB_Rt_Dir, Dom = 'DomContract', Adj = True):
    ABD_Config = {}
    ABD_Config['DB_Rt_Dir'] = DB_Rt_Dir
    ABD_Config['Time_Param'] = Time_Param
    ABD_Config['Period_List'] = [Period]
    ABD_Config['First_Freq_Col_Param'] = ['Open', 'High', 'Low', 'Close', 'Volume','Oi','AdjFactor']
    ABD_Config['Other_Freq_Col_Param'] = ['Close']

    ABD = Load_BarData(ABD_Config)
    ABD.load_bar([Var], Dom = Dom)
    period = Period
    # ABD.clean_data(['Close_M'])
    if Adj and period != 'd':
        ABD.apply_adj_factor()

    skdata = ABD.Panel_Data[Var]
    skdata = skdata.dropna(axis=0, how='any').copy()

    if 1:  # 标准化处理
        # 修改索引名称
        if period == 'd':
            reindex = [date.replace('_', '-') for date in skdata.index]
            skdata.index = reindex

        # 修改列名 去掉 '_d' or _M, bar数据列名与 catbardata保持一致
        recol = {}
        for col in skdata.columns:
            ncol = col.replace('_' + period, '').lower()
            if ncol == 'oi':
                ncol = 'openInterest'
            elif ncol == 'adjfactor':
                ncol = 'adjFactor'
            recol[col] = ncol
        skdata.rename(columns=recol, inplace=True)

    #----剔除集合竞价bar
    if 1 and period != 'd':
        skdata['hms'] = [x[11:] for x in skdata.index]
        skdata['crthh'] = [x[11:13] for x in skdata.index]
        skdata['prehh'] = skdata['crthh'].shift(1)
        skdata = skdata[~((skdata['prehh'] == u'15') & (skdata['high'] - skdata['low'] < 0.01) \
                         & ((skdata['hms'] == u'21:00:00') | (skdata['hms'] == u'09:00:00') | (skdata['hms'] == u'09:30:00')))]
        skdata.drop(['hms', 'crthh','prehh'], axis=1, inplace=True)

    return skdata
