import pandas as pd
import numpy as np

def prepdatossector(df,media,benchmark,corte,comparacion):

  """
  df: base de dato.
  media: media para el calculo de RS_SMA, dato que sirve para determinar momento segun RS.
  benchmark: con respecto a que comparamos.
  corte: media de RS_SMA que nos determina el corte en el cual consideramos el momento.
  comparacion: que queremos determinar el comportamiento sector o industria.

  La funcion devuelve 2 DataFrame, una que calcula el comportamiento con respecto al benchmark
  y variables asociadas. La segunda matriz devuelve la performance de cada sector.

  """
  t=corte

  objetivo=benchmark



  #Preparar base para calculos
  indice=df[df['activo']==objetivo]['Close'] # contra que se compara cada activo
  activo=df.groupby(['Date',comparacion])['Close'].sum()

  #NORMALIZACION

  base=activo.reset_index().set_index('Date').copy()


  g=pd.DataFrame()

  for sector in base[comparacion].unique():

    filtro=base[base[comparacion]==sector]['Close']
    max=filtro.max()
    min=filtro.min()

    filtro2=base[base[comparacion]==sector]
    norm=pd.DataFrame(columns=['norm'])
    norm['norm']=(((filtro) - (min)) / ((max) - (min)))
    filtro2 =filtro2.join(norm,on='Date')

    g=pd.concat([g,filtro2])

  base=g.groupby(['Date',comparacion]).sum()

  #Normalizar Benchmark

  indicen=pd.DataFrame(indice.values,columns=['Close'],index=indice.index)
  max=indicen.Close.max()
  min=indicen.min()
  indicen['norm']=(((indicen) - (min)) / ((max) - (min)))

  #CALCULO RS

  RS=base['Close'].unstack(comparacion).T/indicen['norm']
  RS=RS.T
  RS=RS.stack()
  RS.name='RS'

  #ADICION DE DATOS A MATRIZ RS

  RS=pd.DataFrame(RS)
  RS['RS_medio_%s'%(media)]=RS.groupby(level=-1,group_keys=False).apply(lambda x: x.rolling(media).mean())
  RS['DIF_RS_media']=((RS['RS']/RS['RS_medio_%s'%(media)])-1)*100
  RS['DIF_pendiente']=RS.groupby(level=-1,group_keys=False)['DIF_RS_media'].apply(lambda x: ((x/x.shift(1))-1))
  RS['pos_p_DS']=RS.groupby(level=-1,group_keys=False)['DIF_pendiente'].apply(lambda x: x.rolling(2).std())
  RS['neg_p_DS']=RS.groupby(level=-1,group_keys=False)['DIF_pendiente'].apply(lambda x: x.rolling(2).std()*-1)
  RS['DIF_RS_SMA']=RS.groupby(level=-1,group_keys=False)['DIF_RS_media'].apply(lambda x: x.rolling(t).mean())
  RS['pos_DS']=RS.groupby(level=-1,group_keys=False)['DIF_RS_media'].apply(lambda x: x.rolling(media).std())
  RS['neg_DS']=RS.groupby(level=-1,group_keys=False)['DIF_RS_media'].apply(lambda x: x.rolling(media).std()*-1)
  RS['pos_DS*media']=RS['pos_DS']*RS['DIF_RS_media']
  RS['neg_DS*media']=RS['neg_DS']*RS['DIF_RS_media']

  #RETORNO DEL SECTOR.

  performance=base.groupby(level=-1,group_keys=False)['norm'].apply(lambda x: (x/x.shift(1)-1))
  performance=pd.DataFrame(performance.values,columns=['Perf'],index=performance.index)
  performance['cumperf']=performance.groupby(level=-1,group_keys=False)['Perf'].apply(lambda x: x.rolling(7).sum())


  return RS,performance

def clasif_estadoRS(df):
  """
  Se clasifica el tipo de presion de fondo en el sector, si es lider o esta retrasado y si tiene momento positivo o no.
  """


  RS=df.copy()

  #PRESION

  RS['presion']=np.where(RS['DIF_pendiente']>RS['pos_p_DS'], 'alcista',
          (np.where(RS['DIF_pendiente']<RS['neg_p_DS'], 'bajista', 'neutro')))

  #LIDER/RETRASADA

  condicion=[RS['DIF_RS_media']>RS['pos_DS'],
            (RS['pos_DS']>RS['DIF_RS_media'])&(RS['DIF_RS_media']>=0),
            (RS['neg_DS']<RS['DIF_RS_media'])&(RS['DIF_RS_media']<0),
            RS['neg_DS']>RS['DIF_RS_media']
            ]
  tipo=['lider','fuerte','debil','retrasado']

  RS['tipo_sector']=np.select(condicion, tipo, default=np.nan)

  #MOMENTO.

  RS['momento']=np.where(RS['DIF_RS_media']>RS['DIF_RS_SMA'], 'positivo','negativo')

  return RS
def criterioploty(matriz,ventana,tipo):

  """
  matriz: base de datos a la que aplicar clasificacion.
  ventana: periodo en el que se analiza, se mira dias para atras ej> -200, son 200 dias atras
  tipo: es criterio al analizar pueder:
    presion - representa la variacion de la pendiente.
    sector- si el sector esta atrasado o lidera con respecto al benchmark segun RS.
    momento- compara RS con una media.

  """

  matriz=matriz.loc[:,['acumperf','presion','tipo_sector','momento']]

  matriz['color']=0

  t=matriz.iloc[ventana:-1].copy()


  if tipo=='presion':

    green=t[t['presion']=='alcista']['color']
    red=t[t['presion']=='bajista']['color']
    yellow=t[t['presion']=='neutro']['color']
    t.loc[green.index,'color']='green'
    t.loc[red.index,'color']='red'
    t.loc[yellow.index,'color']='yellow'

  elif tipo=='sector':

    green=t[t['tipo_sector']=='lider']['color']
    red=t[t['tipo_sector']=='retrasado']['color']
    yellow=t[t['tipo_sector']=='fuerte']['color']
    orange=t[t['tipo_sector']=='debil']['color']
    t.loc[green.index,'color']='green'
    t.loc[red.index,'color']='red'
    t.loc[yellow.index,'color']='yellow'
    t.loc[orange.index,'color']='orange'

  elif tipo=='momento':

    green=t[t['momento']=='positivo']['color']
    red=t[t['momento']=='negativo']['color']
    t.loc[green.index,'color']='green'
    t.loc[red.index,'color']='red'



  return t


def plotly_sector(df,condicionG,condicionP,name):
  """
  df: matriz base para el grafico.

  condicionG: si es una clasificacion general de columna

  condicionP: si es una condicion puntual, se tiene que poner como filtros en pandas ej si es una condicion & df[(df.condicion=='')&(df.condicion == '').....].
  esta condicion es una lista [condicion, tipo, nombre]

  """
  fig = go.Figure()

  fig.add_trace(go.Scatter(
          x=df.index,
          y=df.acumperf,
          name='Perf'))


  #valori=pd.DataFrame(df[condicionG].unique()).dropna().values

  valori=df[condicionG].unique()

  for i in valori:

      fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df.acumperf.where(df[condicionG] == i),
            mode='lines+markers',
            name=i,
            line_color=df[df[condicionG]==i]['color'].values[0]
            ))


  # more condition.
  if condicionP!='':

    for i in condicionP:

      fig.add_trace(
          go.Scatter(
              x=i[0].index,
              y=i[0]['acumperf'],
              mode='markers',
              marker=dict(
                  symbol=i[1],
                  size=20),
              name=i[2]
              ))

  fig.update_layout(title=name)


  fig.show()

def encontrarNan(df,tipo='columnas'):
  '''
  Se muestra de forma standart las columnas con valores Nan

  Tipo:
  *columnas= ver columas Nan
  *filas= ver filas con valores Nan
  *info= informacion de columnas con valores no vacios
  '''

  if tipo=='columnas':
    df=df.columns[df.isna().any()].tolist() #que columnas son Nan

  if tipo=='fila':
    df=df[df.isnull().any(axis=1)] # muestra las filas con Nan

  if tipo=='info':
    df=df.info(verbose=True,null_counts=True) # muestra la cantidad de valoras no nulos.

  return df

def clasif_vol(base,indice,period_dif,period_med):

  '''
  DF resultado y de tratamiento es de periodo semanal.

  period_dif: identifica el periodo con el cual se analiza el diferencial.
  period_media: periodo que se analiza los valores vs su media.
  
  '''

  # metodos de clasificacion:

  #Acumulado.


  base['volumcum']=base.groupby(level=-1,group_keys=False)['Volume'].apply(lambda x: x.cumsum())
  base_semana=base.groupby([pd.Grouper(level=0, freq='W-FRI'),'industria']).last() # reagrupa por semana los datos.

  base_semana['volumeS']=base.groupby([pd.Grouper(level=0, freq='W-FRI'),'industria']).sum(numeric_only=True)['Volume']

  # mayor volumen shares.

  base_semana['dif_nom']=base_semana.groupby(level=-1,group_keys=False)['volumeS'].apply(lambda x: x.diff())
  base_semana['dif_nom_med']=base_semana.groupby(level=-1,group_keys=False)['dif_nom'].apply(lambda x: x.rolling(period_med).mean())

  # incremento relativo, variacion por sector

  base_semana['Var_S']=base_semana.groupby(level=-1,group_keys=False)['volumeS'].apply(lambda x: x.pct_change())

  # Incremento CP VOL vs med 36

  base_semana['Saldo_CP']=base_semana['acum_dif']-base_semana['acum_med']

  # Incremento LP 36 VS 200

  base_semana['Saldo_LP']=base_semana['acum_med']-base_semana['tendencia_anual']


  # Pendiente

  # Cantidad de dinero que entro en los sectores en las ultimas xxx semanas

  base_semana['dif_s_sector']=base_semana.groupby(level=-1,group_keys=False)['flujo_monetario'].apply(lambda x: x.diff(periods=period_dif))
  base_semana['dif_s_med']=base_semana.groupby(level=-1,group_keys=False)['dif_s_sector'].apply(lambda x: x.rolling(period_med).mean())

  #Clasificacion CP y LP

  n=len(base_semana.index.get_level_values(1).unique())
  sector=base_semana.index.get_level_values(1).unique()

  base_semana['ADCP']=0
  base_semana['ADCP']=0

  for i in sector:

    CP=base_semana.xs(i,level=1)['Saldo_CP']
    LP=base_semana.xs(i,level=1)['Saldo_LP']
    CP_STD=CP.std()
    LP_STD=LP.std()
    CP_m=CP.mean()
    LP_m=LP.mean()
    base_semana.loc[(slice(None),i),'ADCP']=np.where(((CP_STD)>CP)&(CP>(-CP_STD)),0,np.where(CP>(CP_STD),1,-1))
    base_semana.loc[(slice(None),i),'ADLP']=np.where(((LP_STD)>LP)&(LP>(-LP_STD)),0,np.where(LP>(LP_STD),1,-1))

  #MERCADO GENERAL

  #volumen Maximo

  indiceS=indice.resample('W-FRI').sum()

  maximo=indiceS.Volume.max()

  fecha_max=indiceS[indiceS['Volume']==maximo].index.values

  indiceS['prop_max_I']=(indiceS.Volume/maximo)*100 #Calcula Porcentaje de volumen en relacion a su maximo del periodo.

  indiceS['max_roll_I']=indiceS['Volume'].rolling(90).max() # calcula nuevos maximos cada 90 dias

  indiceS['prop_max_roll_I']=(indiceS.Volume/indiceS.max_roll_I)*100 #Calcula Porcentaje de volumen en relacion a su maximo de periodos de 90 dias


  #diferencia 2 semanas.

  indiceS['dif_2S_I']=indiceS['Volume'].diff(periods=period_dif) # demuestra interes, candidad de volumen diferencial cada 2 semanas

  #Flujo de $

  indiceS['flujo_I']=base_semana.groupby('Date').sum(numeric_only=True)['flujo_monetario'] # Cantidad de plata del mercado en total.
  indiceS['dif_f_I']=indiceS['flujo_I'].diff(periods=period_dif) # cantidad de plata o flujo que ingresa o egresa plata al mercado.

  # e+08 es el equivalente de multiplicar x * 100.000.000

  return base_semana, indiceS

def interpretacion_vol(base_semana,indicesS,semana_analisis):
    
  # evolucion de los ultimos 3 flujo
  # MDO

  flujoI=indiceS['dif_f_I'].iloc[-3:]

  propM=indiceS[['prop_max_I','prop_max_roll_I']]

  # SECTORES

  rangoF=base_semana.index.get_level_values(0).unique()[-3:]# ultimas 3 fechas.

  flujo=base_semana[['dif_s_sector','dif_s_med']].loc[rangoF[-semana_analisis],:].sort_values('dif_s_sector',ascending=False)

  # evolucion de los ultimos 3 Shares
  # SECTORES

  shares=base_semana[['dif_nom','dif_nom_med']].loc[rangoF[-semana_analisis],:].sort_values('dif_nom',ascending=False)



  # Constrastar con el var semanl

  var=base_semana['Var_S'].loc[rangoF[-semana_analisis],:].sort_values(ascending=False)#?????

  # interes de CP-

  AC=base_semana['ADCP'].loc[rangoF[-semana_analisis],:]

  # concluir con la realidad perspectiva y la evolucion del interes RS de sectores y activos puntuales.

  print('En las ultimas 2 semanas el mercado ha tenido un ingreso de dinero de $',f"${flujoI.iloc[-2]/1000:.1f}k.\n",'\n',
        'Opero en un',f"{round(propM['prop_max_I'].iloc[-2],2)}%",'del volumen maximo que opero el mercado y a un',f"{round(propM['prop_max_roll_I'].iloc[-2],2)}%",'maximo en los ultimos 90 dias.\n','\n',
        'esa semana los sectores mas beneficiados fueron:\n','\n',
        '  *%s moviendo'%(flujo.index[0]),f"${flujo['dif_s_sector'][0]/1000:.1f}k",f"un {round((flujo['dif_s_sector'][0]-flujo['dif_s_med'][0])/abs(flujo['dif_s_med'][0])*100,2)}% con respecto a su media.\n",
        '  *%s moviendo'%(flujo.index[1]),f"${flujo['dif_s_sector'][1]/1000:.1f}k",f"un {round((flujo['dif_s_sector'][1]-flujo['dif_s_med'][1])/abs(flujo['dif_s_med'][1])*100,2)}% con respecto a su media.\n",
        '  *%s moviendo'%(flujo.index[2]),f"${flujo['dif_s_sector'][2]/1000:.1f}k",f"un {round((flujo['dif_s_sector'][2]-flujo['dif_s_med'][2])/abs(flujo['dif_s_med'][2])*100,2)}% con respecto a su media.\n",'\n',
        'Los sectores que mas movimiento de acciones en la ultima semana son:\n','\n',
        '  *%s moviendo un total de'%(shares.index[0]),f"{shares['dif_nom'][0]/1000:.1f}k acciones,",f"un {round((shares['dif_nom'][0]-shares['dif_nom_med'][0])/abs(shares['dif_nom_med'][0])*100,2)}% con respecto a su media.\n",
        '  *%s moviendo un total de'%(shares.index[1]),f"{shares['dif_nom'][1]/1000:.1f}k acciones,",f"un {round((shares['dif_nom'][1]-shares['dif_nom_med'][1])/abs(shares['dif_nom_med'][1])*100,2)}% con respecto a su media.\n",
        '  *%s moviendo un total de'%(shares.index[2]),f"{shares['dif_nom'][2]/1000:.1f}k acciones,",f"un {round((shares['dif_nom'][2]-shares['dif_nom_med'][2])/abs(shares['dif_nom_med'][2])*100,2)}% con respecto a su media.\n",'\n',
        'A continuacion veremos que sectores son los que tuvieron una variacion semanal porcentual positiva en su volumen','\n','\n',
        round(var[var>0]*100,2),'\n','\n',
        'Registrando un movimiento mayor a media habitual (+ 1 desviacion standar) los siguientes sectores','\n','\n',
        AC[AC>0],'\n')
