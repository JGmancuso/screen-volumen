# -*- coding: utf-8 -*-
"""
Created on Wed May  5 14:25:45 2021

@author: jgome
"""

import curl_cffi
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

session = curl_cffi.Session(impersonate="chrome", timeout=5)

def getsymbol():
  
  info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
  del info['Unnamed: 0']

  info=info.index.to_numpy()
  info=info.tolist()
  
  Borrar=['BRIO.BA','DOME.BA','DYCA.BA','GARO.BA','TGLT.BA','IRCP.BA','PGR.BA','^MERV']
  info.append('M.BA')

  for i in Borrar:
    info.remove(i)

  return info


def obtenersymb():

  lider='http://www.rava.com/empresas/mapa.php'
  # gral='http://www.rava.com/precios/panel.php?m=GEN'

  reqlid=requests.get(lider)
  #reqgral=requests.get(gral)
  bsObjLID=BeautifulSoup(reqlid.text, "html.parser")
  #bsObjGRAL=BeautifulSoup(reqgral.text,"html.parser")

  TablaPanelLID=bsObjLID.find('div',id='cartelpanellider') #busca tabla de valores
  TablaPanelGRAL=bsObjLID.find('div',id='cartelpanelgeneral')

  FiltroespeciesLID=TablaPanelLID.find_all('div',class_='celdad tick') #encontrar las especies
  FiltroespeciesGRAL=TablaPanelGRAL.find_all('div',class_='celdad tick')
  tickers=FiltroespeciesGRAL+FiltroespeciesLID

  BA='.BA'
  symbolo=[i.text+BA for i in tickers]
  symbolo.append('^MERV')

  a=symbolo.index('RICH.BA')
  symbolo.pop(a) #13/03/2021 Rich no esta en la lista.

  a=symbolo.index('CARC.BA')
  symbolo.pop(a)

  return symbolo

def ticker():
    """
    Obtiene todos los activos locales que cotizan en bolsa

    """
    lider='http://www.rava.com/precios/panel.php?m=LID'
    gral='http://www.rava.com/precios/panel.php?m=GEN'

    reqlid=requests.get(lider)
    reqgral=requests.get(gral)
    bsObjLID=BeautifulSoup(reqlid.text, "html.parser")
    bsObjGRAL=BeautifulSoup(reqgral.text,"html.parser")

    TablaPanelLID=bsObjLID.find('table',class_='tablapanel') #busca tabla de valores
    TablaPanelGRAL=bsObjGRAL.find('table',class_='tablapanel2')
  
    FiltroespeciesLID=TablaPanelLID.find_all('td',class_='b') #encontrar las especies
    FiltroespeciesGRAL=TablaPanelGRAL.find_all('td',class_='b')
    tickers=FiltroespeciesGRAL+FiltroespeciesLID

    BA='.BA'
    symbolo=[i.text+BA for i in tickers]
    indx=symbolo.index('MERVAL.BA')
    symbolo[indx]='^MERV'
    eliminar=['CARC.BA','RICH.BA']
    for i in eliminar:
      if i in symbolo:
        a=symbolo.index(i)
        symbolo.pop(a) 
    #a=symbolo.index('RICH.BA')
    #symbolo.pop(a) #13/03/2021 Rich no esta en la lista.

    #a=symbolo.index('CARC.BA')
    #symbolo.pop(a)

    #if symbolo.index('MERVAL.BA')!=None:
      #  b=symbolo.index('MERVAL.BA')
       # del (symbolo[b])
    return symbolo

#Volumen

def medidavol(tickers,df):

  for i in tickers:

    #Calcular acumulado ultimos 50 dias.
    df[('Vol_50',i)]=df['Volume'][i].rolling(window=50).sum()
    #Ultimos 30 dias.
    df[('Vol_30',i)]=df['Volume'][i].rolling(window=30).sum()
    #Ultimos 10 dias.
    df[('Vol_10',i)]=df['Volume'][i].rolling(window=10).sum()
    #Calcular media de Vol de 50
    df[('Volmed_50',i)]=df['Volume'][i].rolling(window=50).mean()
    #,30 
    df[('Volmed_30',i)]=df['Volume'][i].rolling(window=30).mean()
    #y 10 dias.
    df[('Volmed_10',i)]=df['Volume'][i].rolling(window=10).mean()
    #VAR
    df[('Volvsmed50',i)]=((df['Volume'][i]/df['Volmed_50'][i])-1)*100
    df[('Volvsmed30',i)]=((df['Volume'][i]/df['Volmed_30'][i])-1)*100
    df[('Volvsmed10',i)]=((df['Volume'][i]/df['Volmed_10'][i])-1)*100
    df[('V10vs50',i)]=((df['Volmed_10'][i]/df['Volmed_50'][i])-1)*100
    df[('V30vs50',i)]=((df['Volmed_30'][i]/df['Volmed_50'][i])-1)*100

  return df


def volacumrango(inicio,fin,activo):
  '''ej:a=volacumrango('2020-06-03','2020-07-20','GGAL.BA')'''

  df2=df.xs(activo, level=1, axis=1).copy()
  df2['Volume'] = np.where((df2['Close']<df2['Close'].shift(1)), df2['Volume']*-1, df2['Volume'])

  volumen=df2.loc[inicio:fin]['Volume'].cumsum()

  return volumen 

def matriztrabajo(tickers,start,end,activos='externos'):
  
  import yfinance as yf
  
  '''
  Activos='externos' cuando la base es sobre activos especificos.
  Si no dejar en blanco o colocar no se actovos= 'locales'

  '''

  sd = start
  ed = end
  tickers= tickers# funcion que determine los ticker automaticamente
 
  df=pd.DataFrame()
  
  if activos!='externos':

    info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
    del info['Unnamed: 0']

    if len(tickers)>len(info.index):
      ''
    else:
      tickers=info.index
    

    for i in tickers:
    
      df1=yf.download(tickers=i, start=sd, end=ed,session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      
      if i=='^MERV':
        ''
      else:
        try:
      
          df1['sector']=info.loc[i]['sector']  
          df1['industria']=info.loc[i]['industria']
        
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

      df=pd.concat([df,df1],ignore_index=True)
    
  else:
    
    for i in tickers:

      df1=yf.download(tickers=i, start=sd, end=ed,session=session)
      df1=df1.droplevel(1,axis=1)
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

      df=pd.concat([df,df1],ignore_index=True)

  return df
  
def grupo_sectores_volumen(df):
  '''
  SCREENER POR SECTOR POR VOLUMEN

  Funcion trae ultima fila de cada sector con los datos de variacion de volumen.

  '''

  df1=df.groupby(['Date','sector'])[['Volume','Vol_50','Vol_30','Vol_10','Volmed_50','Volmed_30','Volmed_10','Volvsmed50','Volvsmed30','Volvsmed10','V10vs50','V30vs50']].sum()
  df1=df1.iloc[-len(df1.index.get_level_values(1).unique()):]
  df1=df1.droplevel(0,0)
 
  return df1

def grupo_industria_volumen(df):
  '''
  SCREENER POR INDSUTRIA POR VOLUMEN

  Funcion trae ultima fila de cada industria con los datos de variacion de volumen.

  '''

  df1=df.groupby(['Date','industria'])[['Volume','Vol_50','Vol_30','Vol_10','Volmed_50','Volmed_30','Volmed_10','Volvsmed50','Volvsmed30','Volvsmed10','V10vs50','V30vs50']].sum()
  df1=df1.iloc[-len(df1.index.get_level_values(1).unique()):]
  df1=df1.droplevel(0,0)
 
  return df1

def grupo_activo_volumen(df):
  '''
  SCREENER POR ACTIVO POR VOLUMEN

  Funcion trae ultima fila de cada activo con los datos de variacion de volumen.

  '''

  df1=df.groupby(['Date','activo'])[['Volume','Vol_50','Vol_30','Vol_10','Volmed_50','Volmed_30','Volmed_10','Volvsmed50','Volvsmed30','Volvsmed10','V10vs50','V30vs50']].sum()
  df1=df1.iloc[-len(df1.index.get_level_values(1).unique()):]
  df1=df1.droplevel(0,0)
 
  return df1

#Vol Profile, para realizar clasificacion no grafica.

#Gestion de riesgo
def agregar_hs(df):

  qh=[]

  for i in df.index.unique():
    
    q=len(df.loc[i])
    for i in range(1,q+1):
      qh.append(i)
  
  return qh

def plotactividad(df,sd,ed,activo,panel1='V10vs50',panel2='inusual',alto=14,ancho=14):
  fig, axs = plt.subplots(3, 1, sharex=True)
  fig.set_size_inches(alto,ancho)
  df.loc[(slice(sd,ed),activo),['Close']].plot(ax=axs[0])
  df.loc[(slice(sd,ed),activo),[panel1]].plot(ax=axs[1])#'Volvsmed30','Volvsmed10','V10vs50','V30vs50'
  df.loc[(slice(sd,ed),activo),[panel2]].plot(ax=axs[2])

def actividad_inusual(df,umbralmed=50,umbralprev=100):
  '''
  Para ser contabilizado como actividad inusual tiene que haber 2 requisitos.
  1- Umbral sobre el volumen medio de 10 ruedas tiene que ser superior a xxx porcentaje, de forma standar tiene que superar en un 50%.
  2- Umbral al volumen previo tiene que superar en xxx porcentaje, de forma standar tiene que superar el 100%.

  para contar movimiento institucional.
  '''

  # parametros para inusual.
  df['inusual30']=np.where(df['Volvsmed10']>umbralmed,1,0)

  # para definir si es mayor en un 40% al dia anterior, hay que sacar los porcentajes.

  df['inusualprev']=np.where(((df['Volume']/df['Volume'].shift((len(df.index.get_level_values(1).unique()))))-1)*100>umbralprev,1,0)

  #unificando concepto de inusual

  df['inusual']=np.where((df['inusual30']==1)&(df['inusualprev']==1),1,0)

  return df

def agrupar_volumen(df,objetivo='activo'):
  # standar la columna objetivo es activo.

  df=df.data.groupby(['Date',objetivo])[['Close','Volume','Vol_50','Vol_30','Vol_10','Volmed_50','Volmed_30','Volmed_10','Volvsmed50','Volvsmed30','Volvsmed10','V10vs50','V30vs50']].sum()

  return df

def conteo(df,lookback=[90,60,30,15]):
  df=df.copy()

  df2=pd.DataFrame(index=df.index.get_level_values(1).unique())

  for i in range(0,len(lookback)):
    
    df3=df.iloc[range(-len(df.index.get_level_values(1).unique())*lookback[i]+1, 0), :].copy() #90 es la cantidad de dias no activos.

    for x in df.index.get_level_values(1).unique(): 

      df2['conteo'+ str(lookback[i]) +'d']=df3.groupby([str(df.index.levels[1].name),])['inusual'].sum()

  for i in range(0,len(lookback)):

    # incremental de ultimos periodos con actividad inusual.
    if i==len(lookback)-1:
    
      df2['intervalo'+str(lookback[i])+'-'+str(0)]=df2['conteo'+ str(lookback[i]) +'d']
    else:
      
      df2['intervalo'+str(lookback[i])+'-'+str(lookback[i+1])]=df2['conteo'+ str(lookback[i]) +'d']-df2['conteo'+ str(lookback[i+1]) +'d']
      

  return df2

def comportamiento(df,periodo=90,industria='no'):
  
  if industria=='no':
    df1=df.data.groupby(['Date','activo'])['Close'].sum()
    df1=df1.unstack('activo')
    df1.fillna(method='ffill', inplace=True)
    
    retorno=pd.DataFrame()
    
    for i in df1.columns:
    
      retorno[i]=(df1[i]/df1[i].shift(1))-1

    act=retorno.rolling(periodo).corr()['^MERV']
    pre=df1.rolling(periodo).corr()['^MERV']

    pre.name='corrP'
    act.name='corrR'
    
    union=pd.concat([pre,act],axis=1)


    return df1,union

  elif industria=='si':

        #correlacion sectores/industria.

    df1=df.data.groupby(['Date','activo','industria'])['Close'].sum()
    df1=df1.unstack(level=(1,2)).swaplevel(1,0,axis=1)
    df1.fillna(method='ffill', inplace=True)

    df2 = pd.DataFrame()

    #normaliza entre 0 ~ 1 y suma los sectores/idustria para poder calcular la correlacion.

    for sector in df1.columns.levels[0]:
        data = (df1[sector] - df1[sector].min()) / (df1[sector].max() - df1[sector].min())
        data = data.sum(axis=1)
        df2[sector] = data

    df3=df.data[df.data.activo=='^MERV']['Close']
    df3=(df3-df3.min())/(df3.max()-df3.min())
    df3.name='Indice'

    df2=pd.concat([df2,df3],axis=1)

    #RETORNOS

    retorno=pd.DataFrame(index=df1.index,columns=df1.columns)

    df1.fillna(method='ffill', inplace=True)

    for i in df1.columns.get_level_values(1):#df1.columns.levels[1]
      
      retorno.loc[slice(None),(slice(None),i)]=(df1.loc[slice(None),(slice(None),i)]/df1.loc[slice(None),(slice(None),i)].shift(1))-1


    df4 = pd.DataFrame()
    for sector in retorno.columns.levels[0]:
        data = ((retorno[sector]*1000) - (retorno[sector].min()*1000)) / ((retorno[sector].max()*1000) - (retorno[sector].min()*1000))
        data = data.sum(axis=1)
        df4[sector] = data

    df3=df.data[df.data.activo=='^MERV']['Close']
    df3=(df3/df3.shift(1))-1
    df3=(df3-df3.min())/(df3.max()-df3.min())
    df3.name='Indice'
    df3.fillna(method='ffill', inplace=True)

    retorno=pd.concat([df4,df3],axis=1)
    
    retorno['Indice'].fillna(method='ffill', inplace=True)
    df2['Indice'].fillna(method='ffill', inplace=True)

    act=retorno.rolling(periodo).corr()['Indice']
    pre=df2.rolling(periodo).corr()['Indice']


    pre.name='corrP'
    act.name='corrR'
    
    union=pd.concat([pre,act],axis=1)


    return df2,union

def grafcomp(data,intervalo=90,subind='',activo='',sector=''):


  fig, axs = plt.subplots(4, 1, sharex=True)
  fig.set_size_inches(14,14)
   
  
  if subind=='no':
    precio,corr=comportamiento(data,periodo=intervalo,industria='no')
    mostrar=precio[[activo,'^MERV']].copy()
    mostrar['corrP']=corr.loc[(slice(None),activo),'corrP'].values
    mostrar['corrR']=corr.loc[(slice(None),activo),'corrR'].values  
    mostrar[activo].plot(ax=axs[0]).title.set_text('Activo')#'Volvsmed30','Volvsmed10','V10vs50','V30vs50'
    mostrar['^MERV'].plot(ax=axs[1]).title.set_text('Indice')
    mostrar['corrP'].plot(ax=axs[2]).title.set_text('Correlacion Precio')
    mostrar['corrR'].plot(ax=axs[3]).title.set_text('Correlacion Retorno')
  
  
  else:
  
    precio,corr=comportamiento(data,periodo=intervalo,industria='si')
    mostrar=precio[[sector,'Indice']].copy()
    mostrar['corrP']=corr.loc[(slice(None),sector),'corrP'].values
    mostrar['corrR']=corr.loc[(slice(None),sector),'corrR'].values  
    mostrar[sector].plot(ax=axs[0]).title.set_text('Sector')#'Volvsmed30','Volvsmed10','V10vs50','V30vs50'
    mostrar['Indice'].plot(ax=axs[1]).title.set_text('Indice')
    mostrar['corrP'].plot(ax=axs[2]).title.set_text('Correlacion Precio')
    mostrar['corrR'].plot(ax=axs[3]).title.set_text('Correlacion Retorno')


def actualizacion(nombre,intervalo,activos='no'):
  import yfinance as yf
  
  
  archivo="/content/drive/MyDrive/%s.csv" %str(nombre)

  if os.path.isfile(archivo)==True:

    df=pd.read_csv(archivo)#cargarlo.
    del df['Unnamed: 0']

    if df.columns is 'Date'==True:
      del df['Date']
    
    if activos=='no':  

      if intervalo=='1m':
        base=base1m(guardar='no')#generar DataFrame actual
      elif intervalo=='5m':
        base=base5m(guardar='no')
      elif intervalo=='15m':
        base=base15m(guardar='no')
      elif intervalo=='30m':
        base=base30m(guardar='no')
      elif intervalo=='1h':
        base=base1h(guardar='no')
    
    elif activos!='no':  

      if intervalo=='1m':
        base=base1m(lista=activos,guardar='no')#generar DataFrame actual
      elif intervalo=='5m':
        base=base5m(lista=activos,guardar='no')
      elif intervalo=='15m':
        base=base15m(lista=activos,guardar='no')
      elif intervalo=='30m':
        base=base30m(lista=activos,guardar='no')
      elif intervalo=='1h':
        base=base1h(lista=activos,guardar='no')

    if base.columns is 'Date'==True:
      del base['Date']
    #Unir ambos
    df=pd.concat([df,base],ignore_index=True)
    df=pd.concat([df,base],ignore_index=True)

    date=pd.to_datetime(df['Datetime'],utc=True)
    del df['Datetime']
    df['Date']=date
    df=df.groupby(['Date','activo']).sum()
    df.reset_index(inplace=True)
    #Guardar sobreescribiendo.
    df.to_csv(archivo)


  #NO
  elif os.path.isfile(archivo)==False:
   
    print("El fichero no existe")
    
    df=pd.DataFrame()

    if activos=='no':  

      if intervalo=='1m':
        base=base1m(nombre=nombre)#generar DataFrame actual
      elif intervalo=='5m':
        base=base5m(nombre=nombre)
      elif intervalo=='15m':
        base=base15m(nombre=nombre)
      elif intervalo=='30m':
        base=base30m(nombre=nombre)
      elif intervalo=='1h':
        base=base1h(nombre=nombre)
    
    elif activos!='no':  

      if intervalo=='1m':
        base=base1m(lista=activos,nombre=nombre)#generar DataFrame actual
      elif intervalo=='5m':
        base=base5m(lista=activos,nombre=nombre)
      elif intervalo=='15m':
        base=base15m(lista=activos,nombre=nombre)
      elif intervalo=='30m':
        base=base30m(lista=activos,nombre=nombre)
      elif intervalo=='1h':
        base=base1h(lista=activos,nombre=nombre)

    df=base.copy()

  return df

def base1m(lista='',nombre='',guardar='si'):
  
  import yfinance as yf

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=7)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed,session=session)


  df=pd.DataFrame()
  
  if lista=='':

    for i in tickers:
      
      import yfinance as yf
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1m',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)
  
  else:

    for i in lista:
      import yfinance as yf
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1m',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  #base de datos 1 m
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

def base5m(lista='',nombre='',guardar='si'):
  import yfinance as yf

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=59)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed,session=session)

 
  df=pd.DataFrame()
  if lista=='':

    for i in tickers:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='5m',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)
  
  else:

    for i in lista:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='5m',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  #base de datos 5 m
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

def base15m(lista='',nombre='',guardar='si'):
  import yfinance as yf

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=59)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed,session=session)


  df=pd.DataFrame()
  if lista=='':

    for i in tickers:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='15m',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  else:

    for i in lista:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='15m',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  #base de datos 5 m
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

def base30m(lista='',nombre='',guardar='si'):
  import yfinance as yf

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=59)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed,session=session)



  df=pd.DataFrame()
  if lista=='':

    for i in tickers:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='30m',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)
  
  else:

    for i in lista:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='30m',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  #base de datos 5 m
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

def base1h(lista='',nombre='',guardar='si'):
  import yfinance as yf

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=729)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed,session=session)

  df=pd.DataFrame()
  if lista=='':

    for i in tickers:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1h',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      df1.rename(columns={'index':'Datetime'},inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)
  
  else:

    for i in lista:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1h',session=session)
      df1=df1.droplevel(1,axis=1)
      df1['activo']=i
      df1.reset_index(inplace=True)
      df1.rename(columns={'index':'Datetime'},inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

def screen_industrias(df):

  #PRIMERA PARTE
  base=agrupar_volumen(df,'industria')
  #normalizar?? No por que no tiene nada que ver que tenga mas activos y por ende no va a tener mas actividad inusual.
  inusual=actividad_inusual(base)
  cuenta=conteo(inusual)

  #SEGUNDA PARTE

  noventad=cuenta['intervalo90-60'].sort_values(ascending=False).head(10)
  sesentad=cuenta['intervalo60-30'].sort_values(ascending=False).head(10)
  treintad=cuenta['intervalo30-15'].sort_values(ascending=False).head(10)
  quinced=cuenta['intervalo15-0'].sort_values(ascending=False).head(10)

  movmaduro=noventad.loc[noventad.index.isin(treintad.index)].index
  movenmad=sesentad.loc[sesentad.index.isin(treintad.index)].index
  movinic=treintad.loc[treintad.index.isin(quinced.index)].index

  #TERCERA PARTE
  base2=inusual['V10vs50'].unstack(1).resample('W-FRI').sum()

  #crecimientto
  crecimiento=((base2/base2.shift(1))-1).replace([np.nan,np.inf,-np.inf],0).cumsum()

  acummej=crecimiento.iloc[-1].sort_values(ascending=False).head(10)

  #SECTORES QUE MAS CRECIO en promedio ACUMULADO EN PROMEDIO

  acumprommej=crecimiento[str(datetime.datetime.utcnow().year)].mean()
  acumprommej=acumprommej.sort_values(ascending=False).head(10)

  #SECTORES QUE MAS % entro en 60-30 dias
  data60=base2.iloc[-40:].copy()
  data30=base2.iloc[-20:].copy()


  crec60=((data60/data60.shift(1))-1).replace([np.nan,np.inf,-np.inf],0).cumsum()
  mascrec60=crec60.iloc[-1].sort_values(ascending=False).head(10)

  crec30=((data30/data30.shift(1))-1).replace([np.nan,np.inf,-np.inf],0).cumsum()
  mascrec30=crec30.iloc[-1].sort_values(ascending=False).head(10)

  crecacum=acummej.index
  crecacumprom=acumprommej.index
  crecconstante=mascrec60.loc[mascrec60.index.isin(mascrec30.index)].index

  resumen=pd.unique(movmaduro.tolist()+movenmad.tolist()+movinic.tolist()+crecacum.tolist()+crecacumprom.tolist()+crecconstante.tolist())
  nombres=['Maduro','En_Maduracion','Inic_Mov','Inic_Fuerte','Crec_Acum','Crec_A_Prom','Crec_Const']

  resumen=pd.DataFrame(np.zeros((len(resumen),len(nombres))),columns=nombres,index=resumen)

  resumen= resumen.astype('object')

  for i in resumen.index:
    if i in movmaduro:
      resumen.loc[i,'Maduro']='X'

  for i in resumen.index:
    if i in movenmad:
      resumen.loc[i,'En_Maduracion']='X'

  for i in resumen.index:
    if i in movinic:
      resumen.loc[i,'Inic_Mov']='X'

  for i in resumen.index:
    if i in movinic and i in mascrec30:
      resumen.loc[i,'Inic_Fuerte']='X'

  for i in resumen.index:
    if i in crecacum:
      resumen.loc[i,'Crec_Acum']='X'

  for i in resumen.index:
    if i in crecacumprom:
      resumen.loc[i,'Crec_A_Prom']='X'

  for i in resumen.index:
    if i in crecconstante:
      resumen.loc[i,'Crec_Const']='X'

  return resumen
def screen_activos(df,industrias='',Activos='todos'):

  '''
  Activos: "mas activos" los que salen del los sectores mas activos
  Activos: "lideres" merval lideres
  Activos: "general" panel genral
  Activos: "todos" todos los activos
  '''

  if Activos=='mas activos':

    listadoact=df.data[df.data['industria'].isin(industrias.index)].groupby(['Date','industria','activo']).sum()

  elif Activos=='lideres':

    lideres=['ALUA','BBAR','BMA','BYMA','CEPU','COME','CRES','CVH','EDN','GGAL','HARG','LOMA','MIRG','PAMP','SUPV','TECO2','TGNO4','TGSU','TRAN','TXAR','VALO','YPF']
    for i in range(len(lideres)):
      lideres[i]+=".BA"

    listadoact=df.data.groupby(['Date','industria','activo']).sum(numeric_only=True)
    listadoact=listadoact[listadoact.index.isin(lideres,level='activo')]# solo lideres  


  elif Activos=='general':

    listadoact=df.data.groupby(['Date','industria','activo']).sum(numeric_only=True)

    tickets=listadoact.index.get_level_values(2).unique().tolist()
    lideres=['ALUA','BBAR','BMA','BYMA','CEPU','COME','CRES','CVH','EDN','GGAL','HARG','LOMA','MIRG','PAMP','SUPV','TECO2','TGNO4','TRAN','TXAR','VALO','YPFD']
    for i in range(len(lideres)):
      lideres[i]+=".BA"
    for i in lideres:
      tickets.pop(tickets.index(i))

    #listadoact=df.data.groupby(['Date','industria','activo']).sum()
    listadoact=listadoact[listadoact.index.isin(tickets,level='activo')]# solo generales  

  elif Activos=='todos':

    listadoact=df.data.groupby(['Date','industria','activo']).sum(numeric_only=True) 


  #PRIMERA PARTE
  base=actividad_inusual(listadoact)
  base=base.reindex().groupby(['Date','activo']).sum()
  #normalizar?? No por que no tiene nada que ver que tenga mas activos y por ende no va a tener mas actividad inusual.
  inusual=actividad_inusual(base)
  cuenta=conteo(inusual)

  #SEGUNDA PARTE

  noventad=cuenta['intervalo90-60'].sort_values(ascending=False).head(10)
  sesentad=cuenta['intervalo60-30'].sort_values(ascending=False).head(10)
  treintad=cuenta['intervalo30-15'].sort_values(ascending=False).head(10)
  quinced=cuenta['intervalo15-0'].sort_values(ascending=False).head(10)

  movmaduro=noventad.loc[noventad.index.isin(treintad.index)].index
  movenmad=sesentad.loc[sesentad.index.isin(treintad.index)].index
  movinic=treintad.loc[treintad.index.isin(quinced.index)].index

  #TERCERA PARTE
  #tengo que igualar que quede 2 en indice y uno en columna.

  #base2=inusual['V10vs50'].unstack(1).resample('W-FRI').sum()
  base2=inusual.reindex().groupby(['Date','activo']).sum()['V10vs50'].unstack(1).resample('W-FRI').sum()

  #crecimientto
  crecimiento=((base2/base2.shift(1))-1).replace([np.nan,np.inf,-np.inf],0).cumsum()

  acummej=crecimiento.iloc[-1].sort_values(ascending=False).head(10)

  #SECTORES QUE MAS CRECIO en promedio ACUMULADO EN PROMEDIO

  acumprommej=crecimiento.loc[str(datetime.datetime.utcnow().year)].mean() # VER
  acumprommej=acumprommej.sort_values(ascending=False).head(10)

  #SECTORES QUE MAS % entro en 60-30 dias
  data60=base2.iloc[-40:].copy()
  data30=base2.iloc[-20:].copy()


  crec60=((data60/data60.shift(1))-1).replace([np.nan,np.inf,-np.inf],0).cumsum()
  mascrec60=crec60.iloc[-1].sort_values(ascending=False).head(10)

  crec30=((data30/data30.shift(1))-1).replace([np.nan,np.inf,-np.inf],0).cumsum()
  mascrec30=crec30.iloc[-1].sort_values(ascending=False).head(10)

  crecacum=acummej.index
  crecacumprom=acumprommej.index
  crecconstante=mascrec60.loc[mascrec60.index.isin(mascrec30.index)].index
  
  totallist=list(set(movmaduro.tolist()+movenmad.tolist()+movinic.tolist()+crecacum.tolist()+crecacumprom.tolist()+crecconstante.tolist()))
  nombres=['Maduro','En_Maduracion','Inic_Mov','Inic_Fuerte','Crec_Acum','Crec_A_Prom','Crec_Const']

  resumen=pd.DataFrame(np.zeros((len(totallist),len(nombres))),columns=nombres,index=totallist)

  resumen= resumen.astype('object')

  for i in resumen.index:
    if i in movmaduro:
      resumen.loc[i,'Maduro']='X'

  for i in resumen.index:
    if i in movenmad:
      resumen.loc[i,'En_Maduracion']='X'

  for i in resumen.index:
    if i in movinic:
      resumen.loc[i,'Inic_Mov']='X'

  for i in resumen.index:
    if i in movinic and i in mascrec30:
      resumen.loc[i,'Inic_Fuerte']='X'

  for i in resumen.index:
    if i in crecacum:
      resumen.loc[i,'Crec_Acum']='X'

  for i in resumen.index:
    if i in crecacumprom:
      resumen.loc[i,'Crec_A_Prom']='X'

  for i in resumen.index:
    if i in crecconstante:
      resumen.loc[i,'Crec_Const']='X'

  return resumen

def preparar_corrind(df):

  df1=df.data.copy()
  df1.reset_index(inplace=True)
  #Agrupacion por semana INDUSTRIA / ACTIVO
  df1=df1.groupby([pd.Grouper(key='Date', freq='W-FRI'),'activo','industria'])['Close'].last()
  # columnas LEVEL=0 son las industrias/LEVEL=1 son los activos
  df1=df1.unstack(level=(1,2)).swaplevel(1,0,axis=1)
  df1.fillna(method='ffill', inplace=True)
  df2 = pd.DataFrame()
  #normaliza entre 0 ~ 1 y suma los sectores/idustria para poder calcular la correlacion.

  for sector in df1.columns.levels[0]:
      data = (df1[sector] - df1[sector].min()) / (df1[sector].max() - df1[sector].min())
      data = data.sum(axis=1)
      df2[sector] = data

  #Datos normalizados de cada industria/sector CACULA LA CORRELACION EN BASE A PRECIO

  #Saco datos del MERVAL para poder calcular la correlacion.
  df3=df.data[df.data.activo=='^MERV']['Close'].copy()
  df3=df3.reset_index()
  df3=df3.groupby(pd.Grouper(key="Date", freq="W-FRI")).last()


  # NORMALIZO DATOS DEL MERVAL

  df3=(df3-df3.min())/(df3.max()-df3.min())
  df3=df3.rename(columns={'Close':'Indice'})

  df2=pd.concat([df2,df3],axis=1)# UNIFICO

  #RETORNOS IDEM ANTERIOR PERO CALCULADO EN RETORNOS

  retorno=pd.DataFrame(index=df1.index,columns=df1.columns)

  df1.fillna(method='ffill', inplace=True)

  for i in df1.columns.get_level_values(1):#df1.columns.levels[1]
    
    retorno.loc[slice(None),(slice(None),i)]=(df1.loc[slice(None),(slice(None),i)]/df1.loc[slice(None),(slice(None),i)].shift(1))-1


  df4 = pd.DataFrame()
  for sector in retorno.columns.levels[0]:
      data = ((retorno[sector]*1000) - (retorno[sector].min()*1000)) / ((retorno[sector].max()*1000) - (retorno[sector].min()*1000))
      data = data.sum(axis=1)
      df4[sector] = data

  df3=df.data[df.data.activo=='^MERV']['Close']
  df3=df3.reset_index()
  df3=df3.groupby(pd.Grouper(key="Date", freq="W-FRI")).last()
  df3=(df3/df3.shift(1))-1
  df3=(df3-df3.min())/(df3.max()-df3.min())
  #df3.name='Indice'
  df3=df3.rename(columns={'Close':'Indice'})
  df3.fillna(method='ffill', inplace=True)

  retorno=pd.concat([df4,df3],axis=1)
  retorno['Indice'].fillna(method='ffill', inplace=True)
  df2['Indice'].fillna(method='ffill', inplace=True)



  return df2,retorno
def prep_corract(df):
  df1=df.data.copy()
  df1.reset_index(inplace=True)
  df1['Date'] = pd.to_datetime(df1['Date'])  
  
  #Agrupacion por semana INDUSTRIA / ACTIVO
  df1=df1.groupby([pd.Grouper(key='Date', freq='W-FRI'),'activo'])['Close'].last()
  
  # columnas LEVEL=0 son las industrias/LEVEL=1 son los activos
  df1=df1.unstack(level=(1))
  df1.fillna(method='ffill', inplace=True)
  df2 = pd.DataFrame()
  #normaliza entre 0 ~ 1 y suma los sectores/idustria para poder calcular la correlacion.

  for activo in df1.columns:
      data = (df1[activo] - df1[activo].min()) / (df1[activo].max() - df1[activo].min())
      df2[activo] = data

  #Datos normalizados de cada industria/sector CACULA LA CORRELACION EN BASE A PRECIO
  #Saco datos del MERVAL para poder calcular la correlacion.

  df3=df.data[df.data.activo=='^MERV']['Close'].copy()
  df3=df3.reset_index()
  df3=df3.groupby(pd.Grouper(key="Date", freq="W-FRI")).last()


  # NORMALIZO DATOS DEL MERVAL

  df3=(df3-df3.min())/(df3.max()-df3.min())
  df3=df3.rename(columns={'Close':'Indice'})

  df2=pd.concat([df2,df3],axis=1)# UNIFICO


  #RETORNOS IDEM ANTERIOR PERO CALCULADO EN RETORNOS

  retorno=pd.DataFrame(index=df1.index,columns=df1.columns)

  df1.fillna(method='ffill', inplace=True)

  #for i in df1.columns:#df1.columns.levels[1]
    
  retorno=(df1/df1.shift(1))-1


  df4 = pd.DataFrame()
  for activo in retorno.columns:
      data = ((retorno[activo]*1000) - (retorno[activo].min()*1000)) / ((retorno[activo].max()*1000) - (retorno[activo].min()*1000))
      df4[activo] = data

  df3=df.data[df.data.activo=='^MERV']['Close']
  df3=df3.reset_index()
  df3=df3.groupby(pd.Grouper(key="Date", freq="W-FRI")).last()
  df3=(df3/df3.shift(1))-1
  df3=(df3-df3.min())/(df3.max()-df3.min())
  #df3.name='Indice'
  df3=df3.rename(columns={'Close':'Indice'})
  df3.fillna(method='ffill', inplace=True)

  retorno=pd.concat([df4,df3],axis=1)


  retorno['Indice'].fillna(method='ffill', inplace=True)
  df2['Indice'].fillna(method='ffill', inplace=True)

  return df2,retorno

def corr_industrias(df2,retorno,periodo=24):
  #calculos y expresion de valores

  act=retorno.rolling(periodo).corr()
  pre=df2.rolling(periodo).corr()

  corr=pd.DataFrame(index=pre.loc[(slice(None),'Indice'),slice(None)].mean().index)
  corr['Corrmedia']=pre.loc[(slice(None),'Indice'),slice(None)].mean().values
  corr['Tipo']=0

  for i in corr.index:
    if corr.loc[i]['Corrmedia']>=-1 and corr.loc[i]['Corrmedia']<=0:
      corr.loc[i,'Tipo']='Inversa'
    elif corr.loc[i]['Corrmedia']>0 and corr.loc[i]['Corrmedia']<=0.4:
      corr.loc[i,'Tipo']='Nula/Poca'
    elif corr.loc[i]['Corrmedia']>0.4 and corr.loc[i]['Corrmedia']<=0.6:
      corr.loc[i,'Tipo']='Poca'
    elif corr.loc[i]['Corrmedia']>0.6 and corr.loc[i]['Corrmedia']<=0.8:
      corr.loc[i,'Tipo']='Alta'
    elif corr.loc[i]['Corrmedia']>0.8 and corr.loc[i]['Corrmedia']<=1:
      corr.loc[i,'Tipo']='Muy Alta'

  corr=corr.join(pre.loc[(pre.index[-1],'Indice'),slice(None)].T.droplevel(0,axis=1),how='left')

  corr.rename(columns={'Indice':'Corractual'},inplace=True)

  return corr


class screener():

  def __init__(self,tickers,inicio,fin,seleccion):

    self.data=matriztrabajo(tickers,inicio,fin,activos=seleccion)

class Volumen():
  def __init__(self,tickers,inicio,fin,seleccion):
    data=screener(tickers,inicio,fin,seleccion)
    self.data=data.data.set_index(data.data.Date).drop(['Date'],axis=1)
    self.industria=grupo_industria_volumen(data.data)
    self.sectores=grupo_sectores_volumen(data.data)
    self.activos=grupo_activo_volumen(data.data)
    
    

def comportamiento_activos(df,corr,Beta,RS,Activos='todos'):
  '''
  Funcion que trae como resultado aquellos activos que tuvieron volumen en XXX periodo y aquellos que vienen con tasa de crecimiento.
  Ademas realiza resumen de sus correlaciones y betas.

  Activos pude tener el valor:
    'todos'= trae como resultado los principales activos
    'mas activos'=trae como resultados los activos de las industrias con mas actvidad de volumen
    'general'= realiza el filtro para los activos del panel general.
    'lideres'=realiza el filtro para los activos del panel general.
  
  Cargar matriz de analisis(df), matriz de correlaciones y de beta (corr y Beta).
  '''
  #hasta aca el Corte
  resumen=screen_activos(df,Activos=Activos) # esta funcion es la que aplica el filtro al tipo de activos.
  resumen=pd.concat([resumen,corr],axis=1,join='inner')
  resumen=pd.concat([resumen,Beta],axis=1,join='inner')
  resumen=pd.concat([resumen,RS],axis=1,join='inner')
  resumen=resumen.sort_values('DIF_RS_media',ascending=False)

  return resumen

def comportamiento_industrias(df,periodo=24):


    df2,retorno=preparar_corrind(df)
    corr=corr_industrias(df2,retorno,periodo=periodo)
    filtro=screen_industrias(df)
    filtro=pd.concat([filtro,corr],axis=1,join='inner')
    resumen=filtro.iloc[:,:-1]
    last=filtro.iloc[:,-1:]
    resumen.insert(8,'CorrActaul',last)

    return resumen
  
def plot_movinichist(df,sd,ed,activo,alto=10,ancho=10):

  df1=df.copy()
  df2=df1.loc[(slice(sd,ed),activo),:].copy()


  mask = df2['movinicfuert']> 0 #filtro para multiindex dataframe.
  idx = pd.IndexSlice
  # Puntos para indicar en el grafico cuando hubo mov indi
  ejex=df2.loc[idx[mask,activo],'Close'].index.get_level_values(0) # puntos eje de las X
  ejey=df2.loc[idx[mask,activo],'Close'] # eje de las Y

  fig, (ax1, ax2, ax3) = plt.subplots(3, 1, gridspec_kw={'height_ratios': [3, 1, 1]}) # height nos muestra la proporcion de la grafica 3.1.1 relacion de 3 en el primer grafico y uno en los sig.
  fig.set_size_inches(20,10)

  ax1.plot(df1.loc[(slice(sd,ed),activo),['Close']].index.get_level_values(0),
           df1.loc[(slice(sd,ed),activo),['Close']])

  ax1.fill_between(df2.index.get_level_values(0),df1.loc[(slice(sd,ed),activo),:]['Close'].values,0,where=(df2.cumhist>0),color='lightsteelblue', label=['Crecimiento Acum'])# relleno para identificar crecimiento acumulado.

  ax1.plot( ejex,ejey,'v', color='orange')

  ax1.legend( ['Precio','Crecimento Volumen Acumulado','Movimiento inicial fuerte'],loc=0)

  ax2.plot(df1.loc[(slice(sd,ed),activo),['movinic']].index.get_level_values(0),
             df1.loc[(slice(sd,ed),activo),['movinic']])
  ax2.title.set_text('Moviminto inicial')

  ax3.plot(df1.loc[(slice(sd,ed),activo),['movinicfuert']].index.get_level_values(0),
                          df1.loc[(slice(sd,ed),activo),['movinicfuert']])
  ax3.title.set_text('Moviminto inicial Fuerte')


def screen_activos_historico(df,vlargo=30,vcorto=15):

  listadoact=df.data.groupby(['Date','industria','activo']).sum(numeric_only=True)

  #PRIMERA PARTE
  base=actividad_inusual(listadoact)
  base=base.reindex().groupby(['Date','activo']).sum()
  #normalizar?? No por que no tiene nada que ver que tenga mas activos y por ende no va a tener mas actividad inusual.
  inusual=actividad_inusual(base.copy())
  cuenta=conteo(inusual)

  lookback=[90,60,30,15] # ventas tempoarales 90 dias, 60...etc.
  df5=inusual.copy()
  
  # matriz base con conteos inusuales por ventana temporal.


  for i in lookback:

    df5['cuminu{}'.format(i)]=inusual.groupby(level=-1,group_keys=False)['inusual'].apply(lambda x: x.rolling(window=i).sum()) # suma acumulada en groupby, se identifica el nivel con el rolleo de cada periodo.
  

  # sacar conteos de 30-15
  df5['intervalo 30-15']=df5['cuminu30']-df5['cuminu15']


  # Movimiento inusual activos que tuvieron actividad inusual ultimos 15 dias y que tuvieron en los 30-15 dias.

  df5['movinic']=np.where((df5['intervalo 30-15']>0)&(df5['cuminu15']>0),1,0)

  datalong=inusual['V10vs50'].iloc[vlargo:].copy()
  datashort=inusual['V10vs50'].iloc[vcorto:].copy()

  #Porcentaje

  df5['%long']=datalong.groupby(level=-1,group_keys=False).apply(lambda x: x.diff()/x.shift().abs())
  df5['%short']=datashort.groupby(level=-1,group_keys=False).apply(lambda x: x.diff()/x.shift().abs())

  #cumsum historico

  cumhist=inusual.groupby(level=-1)['V10vs50'].pct_change()
  cumhist.replace([np.inf, -np.inf], np.nan,inplace=True) 
  df5['cumhist']=cumhist.groupby(level=-1,group_keys=False).apply(lambda x: round((x+1).cumprod(),2))

  #CUM
  #LARGO
  df5['cumlargo']=df5.groupby(level=-1,group_keys=False)['cumhist'].apply(lambda x: x.rolling(window=vlargo).sum())
  #CORTO
  df5['cumcorto']=df5.groupby(level=-1,group_keys=False)['cumhist'].apply(lambda x: x.rolling(window=vcorto).sum())



  df5['movinicfuert']=np.where((df5['movinic']>0)&(df5['cumcorto']>0),1,0)
  

  return df5
  
def datosdrawdown(activo):
  
  import yfinance as yf

  sd = '2016-06-10'
  ed = datetime.datetime.utcnow()
  Tickets=[activo]
  df=yf.download(tickers=Tickets,start=sd,end=ed,session=session)
  df['Ret']=(df.Close/df.Close.shift())-1
  df['Max']=df.Close.cummax()
  df['drawdownabs']=df.Close-df.Close.cummax()
  df['drawdown']=(df.drawdownabs/df.Close.cummax())
  df['drawdown']=df['drawdown']*(100)
  recoveryfact=df.Close.iloc[-1]/-df.drawdownabs.min()# esta bien?

  df['dd_start']=pd.NaT
  df['_date']=df.index
  df.dd_start=df.dd_start.mask(df.drawdown==0,0) #completa en la comlumna dd-start con 0 cuando drawdown es igual a 0
  df.dd_start=df.dd_start.mask((df.drawdown<0)&(df.drawdown.shift()==0),df._date) #pone fecha de inicio, cuando drawdown es negativo y el anterior fue positivo.
  df.dd_start=pd.to_datetime(df.dd_start, errors='coerce') # pone en formato fecha.
  df.dd_start.ffill(inplace=True)# rellena los valores NaT con el de la fecha de incio del drawdown.
  df.dd_start=df.dd_start.mask(df.drawdown==0,np.nan) # cuando drawdown es = a 0 compleca con nan
  df['dd_duration']=df.groupby('dd_start').dd_start.cumcount()+1 # crea colmna de duracion grupa por fecha y suma el acumulado de conteo. 
  df.dd_duration=df.dd_duration.mask(df.drawdown==0,0)#  en la columan dd_duration cuando drawdown es 0 se pide que cargue 0, de manera que no acumule conteo.
  df.drop('_date',axis=1,inplace=True) # eliminamos fecha.
  df['dd_total_dur']=df.groupby('dd_start').dd_duration.transform('max').fillna(0).astype(int) # nose que hace.
  df['dd-until_end']=df.dd_total_dur-df.dd_duration+1 #calcula cuantos dias falta para finalizar el drawdown

  return df

def analisisdraw(df,umbral=15,grafico='no'):

  '''
  Umbral: es la cantidad de dias para atras y para adelante que se considera para correcion y caida.

  caida se considera correcion de menor duracion.
  '''
  
  dd_groups=df.groupby('dd_start')# no se que hace es un filtro.
  drawdowns=df.loc[dd_groups.drawdown.idxmin()]
  #resumen de interpretacion.
  #Ej ATK- Así comprobamos que su mayor drawdown fue de 31.26% (drawdownabs maximo valor)el día 18-03-2020, 
  #que ese drawdown se inició el 14-02-2020, en su peor momento duraba 23 días(23 dias para llegar a su mini valor-dd_duration para el maximo valor abs),
  #que tuvo una duración total de 208 días(total duraion drawdowns-dd_total_duration para el mayor drwadown), y que por tanto desde su mínimo necesito todavía 186 días (dd-until_end de maximodrawdown) para recuperar el nivel previo.

  datos=drawdowns.mask(drawdowns.dd_total_dur<umbral)['drawdown'].describe().to_frame()
  datos.rename(columns={'drawdown':'Correcion_porc'},inplace=True)
  datos['Caidas_porc']=drawdowns.mask(drawdowns.dd_total_dur>umbral)['drawdown'].describe().to_frame()
  datos['Dias_min_corr']=drawdowns.mask(drawdowns.dd_total_dur<umbral)['dd_duration'].describe().to_frame()
  datos['Dias_min_caida']=drawdowns.mask(drawdowns.dd_total_dur>umbral)['dd_duration'].describe().to_frame()
  datos['Dias_tot_corr']=drawdowns.mask(drawdowns.dd_total_dur<umbral)['dd_total_dur'].describe().to_frame()
  datos['Dias_tot_caida']=drawdowns.mask(drawdowns.dd_total_dur>umbral)['dd_total_dur'].describe().to_frame()
  datos['Dias_rec_corr']=drawdowns.mask(drawdowns.dd_total_dur<umbral)['dd-until_end'].describe().to_frame()
  datos['Dias_rec_caida']=drawdowns.mask(drawdowns.dd_total_dur>umbral)['dd-until_end'].describe().to_frame()
  
  maximacaida=datos.loc['min','Correcion_porc']
  fechamaxcaida=drawdowns[drawdowns['drawdown']==maximacaida]['dd_start'].values[0]
  picodura=drawdowns[drawdowns['drawdown']==maximacaida]['dd_duration'].values[0]
  maxdura=drawdowns[drawdowns['drawdown']==maximacaida]['dd_total_dur'].values[0]
  recup=drawdowns[drawdowns['drawdown']==maximacaida]['dd-until_end'].values[0]

  if grafico=='si':
    
    sns.set_theme(style="darkgrid")

    plt.figure(figsize=(10,10))
    ax=sns.lineplot(data=df, x=df.index, y='Close')

    ax.fill_between(df.index,df.Close, df.Max,where=(df.dd_total_dur>15),color='r')
    labels = [f"{stat}: {val:.2f}" for stat, val in datos.loc['mean'].items()]
    handles = [plt.Line2D([], [], visible=False) for _ in labels]
    

    ax.legend(
      handles,
      labels,
      loc="best",
      handlelength=0,
      title='Valores Medios'
      )



  return display(datos),print('\n',      
              'La mayor perdida porcentaul en un ciclo de caida (drawdown) fue de {} %.'.format(round(maximacaida,2)),'Esa caida se inició el {}, hasta su peor momento duro {} días,'.format(pd.to_datetime(str(fechamaxcaida)).strftime('%d-%m-%Y'),picodura),
              'de un total de {} días,\n'.format(maxdura),
              '\n',
              'Desde su mínimo necesito {} días para recuperar el nivel previo.'.format(recup),
              '\n','\n',
              'ESTADISTICAS (para correciones totales de mas de 15 dias)\n',
              '--------------------------------------------------------\n',
              'Media porcentual de caidas en una correccion de largo plazo:','{} %'.format(round(datos.loc['mean','Correcion_porc'],2)),'\n',
              'En promedio tardo {} dias para llegar al minimo en un correcion de largo plazo '.format(round(datos.loc['mean','Dias_min_corr'],0)),'\n',
              'En promedio las correciones de Largo Plazo duraron un total de {} días'.format(round(datos.loc['mean','Dias_tot_corr'],0)),'\n',
              )


def analisisretorno(df,detalle='no'):

  media=df['Ret'].mean()
  sigma=df['Ret'].std()
  limsup=media+sigma
  liminf=media-sigma

  statd=df[df.Ret>=limsup]['Ret'].describe().to_frame()
  statd['Ret inf']=df[df.Ret<=liminf]['Ret'].describe().to_frame()
  statd['Ret_norm']=df['Ret'].describe().to_frame()

  semanal=(df['Ret']+1).resample('W').prod()-1
  mediasem=semanal.mean()
  sigmasem=semanal.std()
  limsupsem=mediasem+sigmasem
  liminfsem=mediasem-sigmasem

  statsem=semanal[semanal>=limsupsem].describe().to_frame()
  statsem['Ret inf']=semanal[semanal<=liminfsem].describe().to_frame()
  statsem['Ret_norm']=semanal.describe().to_frame()

  if detalle=='si':

    print('\n','Diariamente la firma tuvo un retorno medio de {} % con una desviacion de +/- {} %'.format(round(statd.loc['mean','Ret_norm']*100,2),round(statd.loc['std','Ret_norm']*100,2)),'\n',
          'En su limite superior(media + una desviacion) la normalidad de los retornos fue de {} % con una desviacion de +/- {} % '.format(round(statd.loc['mean','Ret']*100,2),round(statd.loc['std','Ret']*100,2)),'\n',
          'En su limite inferior(media - una desviacion) la normalidad de los retornos fue de {} % con una desviacion de +/- {} % '.format(round(statd.loc['mean','Ret inf']*100,2),round(statd.loc['std','Ret inf']*100,2)),'\n','\n',
          'Semanalmente la firma tuvo un retorno medio de {} % con una desviacion de +/- {} % '.format(round(statsem.loc['mean','Ret_norm']*100,2),round(statsem.loc['std','Ret_norm']*100,2)),'\n',
          'En su limite superior(media + una desviacion) la normalidad de los retornos fue de {} % con una desviacion de +/- {} % '.format(round(statsem.loc['mean','Ret']*100,2),round(statsem.loc['std','Ret']*100,2)),'\n',
          'En su limite inferior(media - una desviacion) la normalidad de los retornos fue de {} % con una desviacion de +/- {} % '.format(round(statsem.loc['mean','Ret inf']*100,2),round(statsem.loc['std','Ret inf']*100,2)),'\n',
    )
  
  return statd,statsem



