# -*- coding: utf-8 -*-
"""
Created on Wed May  5 14:25:45 2021

@author: jgome
"""
!pip install yfinance

import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
import matplotlib.pyplot as plt
from datetime import timedelta
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import mplfinance as mpf
import yfinance as yf
import ta
import datetime
import os.path


def getsymbol():
  
  import requests

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

    a=symbolo.index('RICH.BA')
    symbolo.pop(a) #13/03/2021 Rich no esta en la lista.

    a=symbolo.index('CARC.BA')
    symbolo.pop(a)

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

def matriztrabajo(tickers,start,end):
  
  import yfinance as yf
  
  sd = start
  ed = end
  tickers= tickers# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed)
  df=pd.DataFrame()
  info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
  del info['Unnamed: 0']

  if len(tickers)>len(info.index):
    ''
  else:
    tickers=info.index
  

  for i in tickers:
   
    df1=yf.download(tickers=i, start=sd, end=ed)
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

def actividad_inusual(df,umbralmed=100,umbralprev=40):

  # parametros para inusual.
  df['inusual30']=np.where(df['Volvsmed30']>100,1,0)

  # para definir si es mayor en un 40% al dia anterior, hay que sacar los porcentajes.

  df['inusualprev']=np.where(((df['Volume']/df['Volume'].shift((len(df.index.get_level_values(1).unique()))))-1)*100>40,1,0)

  #unificando concepto de inusual

  df['inusual']=np.where((df['inusual30']==1)&(df['inusualprev']==1),1,0)

  return df

def agrupar_volumen(df,objetivo='activo'):
  # standar la columna objetivo es activo.

  df=df.data.groupby(['Date',objetivo])[['Close','Volume','Vol_50','Vol_30','Vol_10','Volmed_50','Volmed_30','Volmed_10','Volvsmed50','Volvsmed30','Volvsmed10','V10vs50','V30vs50']].sum()

  return df

def conteo(df,lookback=[90,60,30,15]):

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
    mostrar[activo].plot(ax=axs[0])#'Volvsmed30','Volvsmed10','V10vs50','V30vs50'
    mostrar['^MERV'].plot(ax=axs[1])
    mostrar['corrP'].plot(ax=axs[2])
    mostrar['corrR'].plot(ax=axs[3])


  else:

    precio,corr=comportamiento(data,periodo=intervalo,industria='si')
    mostrar=precio[[sector,'Indice']].copy()
    mostrar['corrP']=corr.loc[(slice(None),sector),'corrP'].values
    mostrar['corrR']=corr.loc[(slice(None),sector),'corrR'].values  
    mostrar[sector].plot(ax=axs[0])#'Volvsmed30','Volvsmed10','V10vs50','V30vs50'
    mostrar['Indice'].plot(ax=axs[1])
    mostrar['corrP'].plot(ax=axs[2])
    mostrar['corrR'].plot(ax=axs[3])


def actualizacion(nombre,intervalo,activos='no'):
  
  
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

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=7)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed)

  info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
  del info['Unnamed: 0']

  if len(tickers)>len(info.index):
    ''
  else:
    tickers=info.index


  df=pd.DataFrame()
  
  if lista=='':

    for i in tickers:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1m')
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)
  
  else:

    for i in lista:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1m')
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  #base de datos 1 m
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

def base5m(lista='',nombre='',guardar='si'):

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=59)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed)

  info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
  del info['Unnamed: 0']

  if len(tickers)>len(info.index):
    ''
  else:
    tickers=info.index


  df=pd.DataFrame()
  if lista=='':

    for i in tickers:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='5m')
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)
  
  else:

    for i in lista:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='5m')
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  #base de datos 5 m
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

def base15m(lista='',nombre='',guardar='si'):

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=59)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed)

  info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
  del info['Unnamed: 0']

  if len(tickers)>len(info.index):
    ''
  else:
    tickers=info.index


  df=pd.DataFrame()
  if lista=='':

    for i in tickers:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='15m')
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  else:

    for i in lista:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='15m')
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  #base de datos 5 m
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

def base30m(lista='',nombre='',guardar='si'):

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=59)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed)

  info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
  del info['Unnamed: 0']

  if len(tickers)>len(info.index):
    ''
  else:
    tickers=info.index


  df=pd.DataFrame()
  if lista=='':

    for i in tickers:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='30m')
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)
  
  else:

    for i in lista:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='30m')
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  #base de datos 5 m
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

def base1h(lista='',nombre='',guardar='si'):

  '''
  La base se guarda por defecto.
  '''

  ed=datetime.datetime.utcnow()
  sd=ed - datetime.timedelta(days=729)

  tickers= getsymbol()# funcion que determine los ticker automaticamente
  #df = yf.download(tickers=tickers, start=sd, end=ed)

  info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
  del info['Unnamed: 0']

  if len(tickers)>len(info.index):
    ''
  else:
    tickers=info.index


  df=pd.DataFrame()
  if lista=='':

    for i in tickers:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1h')
      df1['activo']=i
      df1.reset_index(inplace=True)
      df1.rename(columns={'index':'Datetime'},inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)
  
  else:

    for i in lista:
      
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1h')
      df1['activo']=i
      df1.reset_index(inplace=True)
      df1.rename(columns={'index':'Datetime'},inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)

  
  if guardar=='si':
    df.to_csv("/content/drive/MyDrive/%s.csv" %str(nombre))

  return df

class screener():

  def __init__(self,tickers,inicio,fin):

    self.data=matriztrabajo(tickers,inicio,fin)

class Volumen:
  def __init__(self,tickers,inicio,fin):
    data=screener(tickers,inicio,fin)
    self.data=data.data.set_index(data.data.Date).drop(['Date'],axis=1)
    self.industria=grupo_industria_volumen(data.data)
    self.sectores=grupo_sectores_volumen(data.data)
    self.activos=grupo_activo_volumen(data.data)
