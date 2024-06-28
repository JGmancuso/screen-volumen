
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



def matriztrabajo_solucion(tickers,start,end,activos='externos'):
  import yfinance as yf

  sd = start
  ed = end
  tickers= tickers# funcion que determine los ticker automaticamente
  
  
  df4=pd.DataFrame()

  if activos!='externos':

    info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
    del info['Unnamed: 0']

    info.drop(index=['TGLT.BA','IRCP.BA','PGR.BA','BRIO.BA'],inplace=True)
    tickers=info.index


    for i in tickers:

      if i=='^MERV':
        ''
      else:

        df1=yf.download(tickers=i, start=sd, end=ed)
        df1['activo']=i

        try:

          df1['sector']=info.loc[i]['sector']
          df1['industria']=info.loc[i]['industria']

        except KeyError:

          df1['sector']='NN'
          df1['industria']='NN'

        df1=df1.loc[:'2024-05-24']
        df1.index = pd.to_datetime(df1.index)
        df1.index=df1.index.strftime('%Y-%m-%d')
        #df3=df1.copy()# problema dff3 vuelve a 0... funcion que acumule.


        df2=yf.download(tickers=i, start=sd_e, end=ed,interval='1h')
        df2['activo']=i

        try:

          df2['sector']=info.loc[i]['sector']
          df2['industria']=info.loc[i]['industria']

        except KeyError:

          df2['sector']='NN'
          df2['industria']='NN'

        df2=df2.resample('D').agg({'Open':'first','High':'max','Low':'min','Close':'last','Adj Close':'last','Volume':'sum','activo':'last','sector':'last','industria':'last'})
        df2.dropna(inplace=True)
        df2.index=df2.index.strftime('%Y-%m-%d')

        df3=pd.concat([df1,df2])

        df3.reset_index(inplace=True)      

        volumen=df3['Volume']

        df3['Vol_50']=volumen.rolling(window=50).sum()
        #Ultimos 30 dias.
        df3['Vol_30']=volumen.rolling(window=30).sum()
        #Ultimos 10 dias.
        df3['Vol_10']=volumen.rolling(window=10).sum()
        #Calcular media de Vol de 50
        df3['Volmed_50']=volumen.rolling(window=50).mean()
        #,30
        df3['Volmed_30']=volumen.rolling(window=30).mean()
        #y 10 dias.
        df3['Volmed_10']=volumen.rolling(window=10).mean()
        #VAR
        df3['Volvsmed50']=round(((df3['Volume']/df3['Volmed_50'])-1)*100,2)
        df3['Volvsmed30']=round(((df3['Volume']/df3['Volmed_30'])-1)*100,2)
        df3['Volvsmed10']=round(((df3['Volume']/df3['Volmed_10'])-1)*100,2)
        df3['V10vs50']=round(((df3['Volmed_10']/df3['Volmed_50'])-1)*100,2)
        df3['V30vs50']=round(((df3['Volmed_30']/df3['Volmed_50'])-1)*100,2)
        

        df4=pd.concat([df4,df3],ignore_index=True)

  else:
    for i in tickers:  
      df1=yf.download(tickers=i, start=sd, end=ed)
      df1['activo']=i

      try:

        df1['sector']=yf.Ticker(i).info['sector']
        df1['industria']=yf.Ticker(i).info['industry']
      
      except KeyError:

        df1['sector']='NN' 
        df1['industria']='NN'

      df1.reset_index(inplace=True)

      volumen=df1['Volume']
      
      df1['Vol_50']=volumen.rolling(window=50).sum()
      #Ultimos 30 dias.
      df1['Vol_30']=volumen.rolling(window=30).sum()
      #Ultimos 10 dias.
      df1['Vol_10']=volumen.rolling(window=10).sum()
      #Calcular media de Vol de 50
      df1['Volmed_50']=volumen.rolling(window=50).mean()
      #,30 
      df1['Volmed_30']=volumen.rolling(window=30).mean()
      #y 10 dias.
      df1['Volmed_10']=volumen.rolling(window=10).mean()
      #VAR
      df1['Volvsmed50']=round(((df1['Volume']/df1['Volmed_50'])-1)*100,2)
      df1['Volvsmed30']=round(((df1['Volume']/df1['Volmed_30'])-1)*100,2)
      df1['Volvsmed10']=round(((df1['Volume']/df1['Volmed_10'])-1)*100,2)
      df1['V10vs50']=round(((df1['Volmed_10']/df1['Volmed_50'])-1)*100,2)
      df1['V30vs50']=round(((df1['Volmed_30']/df1['Volmed_50'])-1)*100,2)

      df4=pd.concat([df4,df1],ignore_index=True)

  df4= df4.rename(columns={'index': 'Date'})

  return df4

class screener_sol():

  def __init__(self,tickers,inicio,fin,seleccion):

    self.data=matriztrabajo_solucion(tickers,inicio,fin,activos=seleccion)

class Volumen_sol():
  def __init__(self,tickers,inicio,fin,seleccion):
    data=screener_sol(tickers,inicio,fin,seleccion)
    self.data=data.data.set_index(data.data.Date).drop(['Date'],axis=1)
    self.industria=grupo_industria_volumen(data.data)
    self.sectores=grupo_sectores_volumen(data.data)
    self.activos=grupo_activo_volumen(data.data)
