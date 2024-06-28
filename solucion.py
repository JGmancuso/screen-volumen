
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
  sd_e='2024-05-25'
  
  df4=pd.DataFrame()

  if activos!='externos':
  
    info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
    del info['Unnamed: 0']
  
    info.drop(index=['TGLT.BA','IRCP.BA','PGR.BA','BRIO.BA'],inplace=True)
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
        
  df4=df4= df4.rename(columns={'index': 'Date'})
  merval=yf.download('^MERV', start=sd, end=ed)
  merval['activo']='^MERV'
  merval.index = pd.to_datetime(merval.index)
  merval.index=merval.index.strftime('%Y-%m-%d')
  merval.reset_index(inplace=True) 
  df4=pd.concat([df4,merval],ignore_index=True)

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


def prep_corract_sol(df):
  
  df1=df.data.copy()
  df1.reset_index(inplace=True)
  #Agrupacion por semana INDUSTRIA / ACTIVO
  df1['Date'] = pd.to_datetime(df1['Date'])  

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
  df3 = df3.reset_index() # Reset the index to include 'Date' as a column
  df3['Date'] = pd.to_datetime(df3['Date']) # Convert 'Date' column to datetime
  df3 = df3.set_index('Date') # Set 'Date' column as the index
  df3=df3.resample( "W-FRI").last() # Now you can resample with a datetime index
  #df3=df3.reset_index()
  #df3=df3.resample( "W-FRI")
  #df3=df3.groupby(pd.Grouper(key="Date", freq="W-FRI")).last()


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

  df3=df.data[df.data.activo=='^MERV']['Close'].copy()
  df3 = df3.reset_index() # Reset the index to include 'Date' as a column
  df3['Date'] = pd.to_datetime(df3['Date']) # Convert 'Date' column to datetime
  df3 = df3.set_index('Date') # Set 'Date' column as the index
  df3=df3.resample( "W-FRI").last() # Now you can resample with a datetime index
  df3=(df3/df3.shift(1))-1
  df3=(df3-df3.min())/(df3.max()-df3.min())
  #df3.name='Indice'
  df3=df3.rename(columns={'Close':'Indice'})
  df3.fillna(method='ffill', inplace=True)

  retorno=pd.concat([df4,df3],axis=1)


  retorno['Indice'].fillna(method='ffill', inplace=True)
  df2['Indice'].fillna(method='ffill', inplace=True)

  return df2,retorno

def screen_activos_sol(df,industrias='',Activos='todos'):

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
  #base2=inusual.reindex().groupby(['Date','activo']).sum()['V10vs50'].unstack(1).resample('W-FRI').sum()
  #base2 = inusual.reset_index().groupby(['Date','activo'])['V10vs50'].sum().unstack(1).resample('W-FRI', on='Date').sum()
  base2 = inusual.reset_index().groupby(['Date','activo'])['V10vs50'].sum().unstack(1) # removed resample here
  base2.index = pd.to_datetime(base2.index)
  base2 = base2.resample('W-FRI').sum() # resample after unstacking
  #base2 = base2.set_index('Date').resample('W-FRI').sum() 
  
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

def comportamiento_activos_sol(df,corr,Beta,RS,Activos='todos'):
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
  resumen=screen_activos_sol(df,Activos=Activos) # esta funcion es la que aplica el filtro al tipo de activos.
  resumen=pd.concat([resumen,corr],axis=1,join='inner')
  resumen=pd.concat([resumen,Beta],axis=1,join='inner')
  resumen=pd.concat([resumen,RS],axis=1,join='inner')
  resumen=resumen.sort_values('DIF_RS_media',ascending=False)

  return resumen
