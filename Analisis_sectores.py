import pandas as pd
import numpy as np
import plotly.graph_objects as go 

from scipy import stats
import statsmodels.api as sm
import matplotlib.pyplot as plt
from datetime import timedelta
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import datetime
import os.path
import yfinance as yf
import seaborn as sns

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

  RS['tipo_sector']=np.select(condicion, tipo, default='N/A')

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

  matriz['color']='yellow'

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


import plotly.graph_objects as go
import pandas as pd

def plotly_sector(df, condicionG, condicionP, name, stats_df=None):
    """
    df: matriz base (critplot)
    condicionG: 'tipo_sector'
    condicionP: lista de condiciones puntuales (triángulos)
    name: Título principal del gráfico.
    stats_df: DataFrame 'valor' con los conteos de 30/60/90 días.
    """

    fig = go.Figure()

    # 1. Línea de Referencia del Mercado (y=0)
    fig.add_hline(y=0, line_dash="dash", line_color="black", name="Línea Cero")

    # --------------------------------------------------------------------
    # ⭐ 2. NUEVA LÓGICA: Dibujar por Segmentos de Color Continuos
    # --------------------------------------------------------------------

    # 'df' es 'critplot'
    # 'condicionG' es 'tipo_sector'

    # Creamos un 'grupo' cada vez que la categoría (condicionG) cambia
    df['g'] = (df[condicionG] != df[condicionG].shift()).cumsum()

    # Iteramos sobre cada segmento (grupo)
    all_groups = list(df.groupby('g'))

    # Para gestionar la leyenda (mostrar solo una vez cada categoría)
    legend_added = []

    for idx, group in all_groups:

        # Obtenemos el nombre y color del segmento actual
        category_name = group[condicionG].iloc[0]
        category_color = group['color'].iloc[0]

        # Verificamos si ya añadimos esta categoría a la leyenda
        show_legend = False
        if category_name not in legend_added:
            show_legend = True
            legend_added.append(category_name)

        # ----- LA CLAVE PARA CONECTAR LÍNEAS -----
        # Tomamos el segmento actual...
        current_segment = group

        # Buscamos la ubicación del primer punto de este segmento
        first_index_loc = df.index.get_loc(group.index[0])

        if first_index_loc > 0:
            # ...y le añadimos el ÚLTIMO punto del segmento ANTERIOR.
            # Esto fuerza a Plotly a conectar el segmento rojo con el amarillo.
            prev_row = df.iloc[[first_index_loc - 1]]
            current_segment = pd.concat([prev_row, group])
        # -------------------------------------------

        fig.add_trace(go.Scatter(
            x=current_segment.index,
            y=current_segment.acumperf,
            mode='lines+markers', # Líneas y marcadores
            name=category_name,
            line=dict(color=category_color),
            marker=dict(color=category_color, size=4), # Marcadores pequeños
            showlegend=show_legend # Solo mostrar en leyenda la primera vez
        ))

    # 3. Trazado de Condiciones Puntuales (Triángulos 'Muy Fuerte')
    if condicionP:
        for i in condicionP:
            fig.add_trace(
                go.Scatter(
                    x=i[0].index,
                    y=i[0]['acumperf'],
                    mode='markers',
                    marker=dict(symbol=i[1], size=15),
                    name=i[2]
                ))

    # 4. Actualización de Layout (Título y Ejes)
    fig.update_layout(
        title={
            'text': name,
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Fecha",
        yaxis_title="Desempeño Acumulado Relativo al Índice",
        legend_title="Clasificación",
        template="plotly_white",
        # Quitamos el rangeslider como pediste
    )

    # --------------------------------------------------------------------
    # ⭐ 5. Anotación de Estadísticas (LÓGICA ACTUALIZADA) ⭐
    #    (Esta sección ahora ajusta el ancho de las columnas dinámicamente)
    # --------------------------------------------------------------------
    if stats_df is not None:
        
        # 1. Encontrar el ancho máximo del índice
        max_len_index = max(len(idx) for idx in stats_df.index) + 1 
        
        # 2. NUEVO: Calcular anchos de columna dinámicos
        col_widths = {}
        for col in stats_df.columns:
            # El ancho es el máximo entre el título (ej. 'cum_60') y el número más grande (ej. 90)
            try:
                max_num_width = len(str(stats_df[col].max()))
            except ValueError:
                max_num_width = 2 # Default
            
            col_name_width = len(str(col))
            col_widths[col] = max(max_num_width, col_name_width) + 1 # +1 para padding
            
        # 3. Construir el string de texto
        stats_text = f"<b>Clasificación (días)</b><br>"
        
        # Header
        header = " " * max_len_index 
        for col in stats_df.columns:
            width = col_widths[col]
            header += f" | {col:>{width}}" # Usar ancho dinámico
        stats_text += header + "<br>"
        
        # Separador
        separator = "-" * max_len_index
        for col in stats_df.columns:
            width = col_widths[col]
            separator += " | " + ("-" * width)
        stats_text += separator + "<br>"

        # Filas de datos
        for index, row in stats_df.iterrows():
            padded_index = f"{index:<{max_len_index}}" # < alinea a la izquierda
            line = f"{padded_index}"
            for col in stats_df.columns:
                width = col_widths[col]
                line += f" | {row[col]:>{width}}" # Usar ancho dinámico
            stats_text += line + "<br>"
        
        # 4. Añadimos la Anotación
        fig.add_annotation(
            text=stats_text,
            align='left',
            showarrow=False,
            xref='paper',
            yref='paper',
            x=0.01,
            y=0.99,
            yanchor='top',
            xanchor='left',
            bordercolor='black',
            borderwidth=1,
            bgcolor='rgba(255, 255, 240, 0.85)',
            font=dict(
                family="Monospace, Courier New, Courier",
                size=12
            )
        )

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
  
  base=base.reset_index()
  base['Date']=pd.to_datetime(base['Date'])
  base = base.set_index('Date')
  
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
  
  indice=indice.reset_index()
  indice['Date']=pd.to_datetime(indice['Date'])
  indice = indice.set_index('Date')
  
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

def interpretacion_vol(base_semana,indiceS,semana_analisis):
    
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
def base_dato_vol(baseD):

    
  #PRIMERA PARTE
  
  df=pd.DataFrame()
  info=pd.read_excel("/content/drive/MyDrive/activosectorV.xlsx",index_col='activo')#/content/activosectorV.xlsx
  info=info[['sector','industria','investing']]
  info.reset_index(inplace=True)
  symbolo=info.index.values
  
  
  base=baseD.data.copy()
  
  
  for i in base.activo.unique():
  
    base.loc[base["activo"] ==i,'industria']=info[info["activo"] ==i]['investing'].values[0]
  
  
  RS,performance=prepdatossector(base,36,'^MERV',10,'industria')
  
  performance=performance.reset_index()
  performance['Date']=pd.to_datetime(performance['Date'])
  performance = performance.set_index('Date') # Set 'Date' column as index 
  
  
      
  
  
  sem=performance.groupby([pd.Grouper(level=0, freq='W-FRI'),'industria']).sum()
  
  #SEGUNDA PARTE
  
  
  # Calculo Flujo Monetario
  
  df1=baseD.data.copy()
  
  df1['flujo_monetario']=((df1['Low']+df1['High']+df1['Close'])/3)*df1['Volume']
  
  
  
  
  for i in df1.activo.unique():
  
    df1.loc[df1["activo"] ==i,'industria']=info[info["activo"] ==i]['investing'].values[0]
  
  
  info=df1[['Volume','Volvsmed50','activo','industria','flujo_monetario']]
  
  
  comparacion='industria'
  base=info.copy()
  g=pd.DataFrame() #DF de almacenamiento
  
  #indice
  
  #normalizar activo y luego sumar a sector
  
  for sector in base['activo'].unique():
  
    filtro=base[base['activo']==sector]['Volume']
    max=filtro.max()
    min=filtro.min()
  
    filtro2=base[base['activo']==sector]
    norm=pd.DataFrame(columns=['norm'])
    norm['norm']=(((filtro) - (min)) / ((max) - (min)))
    filtro2 =filtro2.join(norm,on='Date')
  
    g=pd.concat([g,filtro2])
  
  base=g.groupby(['Date',comparacion]).sum(numeric_only=False) # suma de valores normalizados individualmente de manera previa
  
  base['normmed50']=base.groupby(level=-1,group_keys=False)['norm'].apply(lambda x: x.rolling(50).mean()) # media de la normalizacion
  base['dif_norm_media']=base.groupby(level=-1,group_keys=False).apply(lambda x: x['norm']-x['normmed50']) # dif del valor normal con su media
  base['acum_dif']=base.groupby(level=-1,group_keys=False).apply(lambda x: x['dif_norm_media'].cumsum()) #acumular diferencia de arriba
  base['acum_med']=base.groupby(level=-1,group_keys=False)['acum_dif'].apply(lambda x: x.rolling(36).mean()) # sacar media del valor acumualdo
  base['tendencia_anual']=base.groupby(level=-1,group_keys=False)['acum_dif'].apply(lambda x: x.rolling(200).mean()) # sacar media del valor acumualdo
  
  #normalizar indice
  '''
  info[info['activo']=='^MERV']
  
  Este filtro no sirve por que trae el indice y el mismo no tiene volumen, hay que ver la suma del volumen de los sectores.
  
  '''
  
  indice=base.reset_index().groupby('Date').sum(numeric_only=True)[['Volume','norm']]
  indice['normmed50']=indice['norm'].rolling(50).mean() # media de la normalizacion
  indice['dif_norm_media']=indice['norm']-indice['normmed50'] # dif del valor normal con su media
  indice['acum_dif']=indice['dif_norm_media'].cumsum() #acumular diferencia de arriba
  indice['acum_med']=indice['acum_dif'].rolling(36).mean() # sacar media del valor acumualdo
  
  #Flujo de efectivo
  
  base['normmed50f']=base.groupby(level=-1,group_keys=False)['flujo_monetario'].apply(lambda x: x.rolling(50).mean()) # media de la normalizacion
  base['dif_flujo_media']=base.groupby(level=-1,group_keys=False).apply(lambda x: x['flujo_monetario']-x['normmed50f']) # dif del valor normal con su media
  base['acum_dif_flujo']=base.groupby(level=-1,group_keys=False).apply(lambda x: x['dif_flujo_media'].cumsum()) #acumular diferencia de arriba
  base['acum_med_flujo']=base.groupby(level=-1,group_keys=False)['acum_dif_flujo'].apply(lambda x: x.rolling(36).mean()) # sacar media del valor acumualdo
  base['tendencia_anual_flujo']=base.groupby(level=-1,group_keys=False)['acum_dif_flujo'].apply(lambda x: x.rolling(200).mean()) # sacar media del valor acumualdo


  return base,indice,sem
