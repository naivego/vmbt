# encoding: UTF-8

"""
Barda
"""
import numpy as np
import pandas as pd
########################################################################
class Barda(object):
    def __init__(self, var, period ='M', onbar = None):
        self.var = var
        self.period = period
        self.dat = pd.DataFrame()
        self.crtidx = 'init'
        self.crtnum = 0
        self.crtbar = None
        self.newsta = 0
        self.indexs = []
        self.onbar = onbar

    # ----------------------------------------------------------------------
    def newbar(self, bar, islastbar = False):
        for idtm in self.dat.index[self.crtnum:self.dat.index.size]:
            if self.newsta>1:
                return
            if bar.datetime > idtm or islastbar:
                if type(self.onbar) != type(None):
                    self.onbar(self.crtnum)
                if self.crtnum >= self.dat.index.size-1:
                    self.newsta = 2
                    return
                else:
                    self.crtnum += 1
                    self.crtidx = self.dat.index[self.crtnum]
                    self.crtbar = pd.Series(index=self.dat.columns)
                    self.newsta = 1
            else:
                if type(self.crtbar) == type(None):
                    return
                if self.crtbar.count() == 0:
                    if bar.vtSymbol == self.var:
                        self.crtbar.name = idtm
                        for k in self.crtbar.index:
                            if k in bar.__dict__:
                                self.crtbar[k] = bar.__dict__[k]
                    break
                else:
                    self.newsta = 0
                    if bar.vtSymbol == self.var:
                        self.crtbar.name = idtm
                        self.crtbar['high'] = max(self.crtbar['high'], bar.high)
                        self.crtbar['low'] = min(self.crtbar['low'], bar.low)
                        self.crtbar['close'] = bar.close
                        self.crtbar['volume'] = self.crtbar['volume'] + bar.volume
                        self.crtbar['openInterest'] = bar.openInterest
                    break

    # ----------------------------------------------------------------------

########################################################################