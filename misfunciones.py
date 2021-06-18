# -*- coding: utf-8 -*-
"""
Created on Wed May  5 14:25:45 2021

@author: jgome
"""


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
  eliminar=['CARC.BA','RICH.BA']
  for i in eliminar:
    if i in symbolo:
      a=symbolo.index(i)
      symbolo.pop(a) 
    #a=symbolo.index('RICH.BA')
  #symbolo.pop(a) #13/03/2021 Rich no esta en la lista.

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
  info=pd.read_csv("/content/drive/MyDrive/Colab Notebooks/activosector.csv",index_col='activo')#hola
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
      
      import yfinance as yf
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1m')
      df1['activo']=i
      df1.reset_index(inplace=True)
      
      df=pd.concat([df,df1],ignore_index=True)
  
  else:

    for i in lista:
      import yfinance as yf
      df1=yf.download(tickers=i, start=sd, end=ed,interval='1m')
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
  import yfinance as yf

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
  import yfinance as yf

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
  import yfinance as yf

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

  resumen=set(movmaduro.tolist()+movenmad.tolist()+movinic.tolist()+crecacum.tolist()+crecacumprom.tolist()+crecconstante.tolist())
  nombres=['Maduro','En_Maduracion','Inic_Mov','Inic_Fuerte','Crec_Acum','Crec_A_Prom','Crec_Const']

  resumen=pd.DataFrame(np.zeros((len(resumen),len(nombres))),columns=nombres,index=resumen)

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

    listadoact=df.data.groupby(['Date','industria','activo']).sum()
    listadoact=listadoact.loc[slice(None),slice(None),lideres]# solo lideres  

  
  elif Activos=='general':

    listadoact=df.data.groupby(['Date','industria','activo']).sum()

    tickets=listadoact.index.get_level_values(2).unique().tolist()
    lideres=['ALUA','BBAR','BMA','BYMA','CEPU','COME','CRES','CVH','EDN','GGAL','HARG','LOMA','MIRG','PAMP','SUPV','TECO2','TGNO4','TRAN','TXAR','VALO','YPFD']
    for i in range(len(lideres)):
      lideres[i]+=".BA"
    for i in lideres:
      tickets.pop(tickets.index(i))

    #listadoact=df.data.groupby(['Date','industria','activo']).sum()
    listadoact=listadoact.loc[slice(None),slice(None),tickets]# solo lideres  

  elif Activos=='todos':

    listadoact=df.data.groupby(['Date','industria','activo']).sum()


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

  resumen=set(movmaduro.tolist()+movenmad.tolist()+movinic.tolist()+crecacum.tolist()+crecacumprom.tolist()+crecconstante.tolist())
  nombres=['Maduro','En_Maduracion','Inic_Mov','Inic_Fuerte','Crec_Acum','Crec_A_Prom','Crec_Const']

  resumen=pd.DataFrame(np.zeros((len(resumen),len(nombres))),columns=nombres,index=resumen)

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

def corr_activos(df2,retorno,periodo=24):
  act=retorno.rolling(periodo).corr()
  pre=df2.rolling(periodo).corr()

  corr=pd.DataFrame(index=pre.loc[(slice(None),'Indice'),slice(None)].mean().index)
  corr['CorrMedia']=pre.loc[(slice(None),'Indice'),slice(None)].mean().values

  corr=corr.join(pre.loc[(pre.index[-1],'Indice'),slice(None)].T.droplevel(0,axis=1),how='left')

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

  info=info.join(act.loc[(act.index[-1],'Indice'),slice(None)].T.droplevel(0,axis=1),how='left')

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

def comportamiento_activos(df,corr,Beta,Activos='todos'):
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
