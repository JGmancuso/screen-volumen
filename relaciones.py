
import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
import matplotlib.pyplot as plt
from datetime import timedelta
from bs4 import BeautifulSoup
import requests
from datetime import datetime
#import mplfinance as mpf
#import ta
import datetime
import os.path
import yfinance as yf
import seaborn as sns

def beta(df,periodo=24):
  '''
  Calculo semanal.
  El calculo se realiza en fracciones de medio año por eso 24 semanas.
  DF es la matriz de retornos
  '''

  act=df.rolling(periodo).cov()
  #pre=df2.rolling(periodo).corr()
  info=pd.DataFrame(index=act.loc[(slice(None),'Indice'),slice(None)].mean().index) # Listado de todos los activos como indice.
  info['CovMedia']=act.loc[(slice(None),'Indice'),slice(None)].mean().values

  info=info.join(act[act.index.isin((slice(None),'Indice'),level=1)].T.droplevel(0,axis=1),how='left')

  info.rename(columns={'Indice':'CovActual'},inplace=True)

  sigmaindice=(df['Indice'].rolling(periodo).std())**2


  beta=pd.DataFrame(np.zeros_like(act.loc[(slice(None),'Indice'),slice(None)]),columns=act.columns, index=sigmaindice.index)

  for i in beta.columns:
    beta.loc[:,i]=act.loc[(slice(None),'Indice'),i].values/sigmaindice.values
  beta

  info['BetaActual']=beta.iloc[-1]
  info['BetaMedia']=beta.mean()

  info['Tipo_Beta']=0

  for i in info.index:
    if info.loc[i]['BetaMedia']>=-1 and info.loc[i]['BetaMedia']<=0:
      info.loc[i,'Tipo_Beta']='Inversa'
    elif info.loc[i]['BetaMedia']>0 and info.loc[i]['BetaMedia']<=0.8:
      info.loc[i,'Tipo_Beta']='Defensiva'
    elif info.loc[i]['BetaMedia']>0.8 and info.loc[i]['BetaMedia']<=1.01:
      info.loc[i,'Tipo_Beta']='Agresiva'
    elif info.loc[i]['BetaMedia']>1.01:
      info.loc[i,'Tipo_Beta']='Muy Agresiva'

  return info.iloc[:,-3:]

def corr_activos(df2,retorno,periodo=24):
  
  act=retorno.rolling(periodo).corr()
  pre=df2.rolling(periodo).corr()

  corr=pd.DataFrame(index=pre.loc[(slice(None),'Indice'),slice(None)].mean().index)
  corr['CorrMedia']=pre.loc[(slice(None),'Indice'),slice(None)].mean().values

  corr=corr.join(pre[pre.index.isin((slice(None),'Indice'),level=1)].T.droplevel(0,axis=1).iloc[:,-1],how='left') # selecciona del MIndx solo la 2~ col el valor Indice
  corr.rename(columns={'Indice':'Corractual'},inplace=True)

  corr['Tipo']=0

  for i in corr.index:
    if corr.loc[i]['CorrMedia']>=-1 and corr.loc[i]['CorrMedia']<=0:
      corr.loc[i,'Tipo']='Inversa'
    elif corr.loc[i]['CorrMedia']>0 and corr.loc[i]['CorrMedia']<=0.4:
      corr.loc[i,'Tipo']='Nula/Poca'
    elif corr.loc[i]['CorrMedia']>0.4 and corr.loc[i]['CorrMedia']<=0.6:
      corr.loc[i,'Tipo']='Poca'
    elif corr.loc[i]['CorrMedia']>0.6 and corr.loc[i]['CorrMedia']<=0.8:
      corr.loc[i,'Tipo']='Alta'
    elif corr.loc[i]['CorrMedia']>0.8 and corr.loc[i]['CorrMedia']<=1:
      corr.loc[i,'Tipo']='Muy Alta'

  return corr


def RS(df,benchmark,media=36):
  '''
  Benchmark: colocar el nombre del activo con el que se compara, ej: merval ^MERV; ser un activo GGAL==> GGAL.BA
  Media: detallar el valor de media que se quiere trabajar para RS, defecto 36
  Posiblemente la ultima fecha no salga como consecuencia de que el indice no siempre tiene hasta la fecha actual cargado el valor.
  Por lo que la formula generalmente otorga resultado con el valor relative strengh del dia anterior.
  '''
  objetivo=benchmark

  #lista_act=activo.columns.unique().to_list()
  #lista_act.remove('^MERV')
  
  #Preparar base para calculos
  indice=df.data[df.data['activo']==objetivo]['Close'] # contra que se compara cada activo
  activo=df.data.groupby(['Date','activo'])['Close'].sum().unstack('activo')
  activo.drop('^MERV',axis=1,inplace=True)
  
  #Calculo
  RS=activo.T/indice*1000
  RS=RS.T
  RS=RS.stack()
  RS.name='RS'

  #DataFrame
  RS=pd.DataFrame(RS)
  RS['RS_medio_%s'%(media)]=RS.groupby(level=-1,group_keys=False).apply(lambda x: x.rolling(media).mean())
  RS['DIF_RS_media']=((RS['RS']/RS['RS_medio_%s'%(media)])-1)*100

  RS['DIF_RS_media']=((RS['RS']/RS['RS_medio_%s'%(media)])-1)*100
  RS['pos_DS']=RS.groupby(level=-1,group_keys=False)['DIF_RS_media'].apply(lambda x: x.rolling(media).std()*2)
  RS['neg_DS']=RS.groupby(level=-1,group_keys=False)['DIF_RS_media'].apply(lambda x: x.rolling(media).std()*-2)
  RS['pos_DS*media']=RS['pos_DS']*RS['DIF_RS_media']
  RS['neg_DS*media']=RS['neg_DS']*RS['DIF_RS_media']
  RS['max_RS_90']=RS.groupby(level=-1,group_keys=False)['RS'].apply(lambda x: x.rolling(90).max())
  # idenficar si hay maximo o no
  RS['max_neto']=RS.groupby(level=-1,group_keys=False)['max_RS_90'].apply(lambda x: x-x.shift(1))
  RS['new_RS_max']=np.where(RS['max_neto']>0,'si','no')

  return (RS)
