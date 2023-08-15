

def base_inicio(sd,ed,activos='externo'):

  #tickers= tickers# funcion que determine los ticker automaticamente

  df=pd.DataFrame()

  if activos!='externos':

    info=pd.read_csv("/content/drive/MyDrive/activosector.csv",index_col='activo')
    del info['Unnamed: 0']

    #esta parte es para corroborar si hay algun activo mas en cotizacion---CORROBORAR

    #if len(tickers)>len(info.index):
    #  ''
    #else:
    #  tickers=info.index

    tickers=info.index.unique()

    for i in tickers:

      df1=yf.download(tickers=i, start=sd, end=ed)
      df1['activo']=i


      df1.reset_index(inplace=True)

      df=pd.concat([df,df1],ignore_index=True)

  return df

def matriztrabajoS(df,freq):

  df1=df.copy()

  if freq=='semanal':

    df1=df1.groupby(['activo', pd.Grouper(key='Date', freq='W-FRI')]).agg({
        'Open':'first',
        'High':'max',
        'Low':'min',
        'Close':'last',
        'Volume':'sum'})

    #Ultimas 10 semanas.
    df1['Vol_10']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(10).sum())
    #Ultimas 6 semanas.
    df1['Vol_6']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(6).sum())
    #Ultimas 2 semanas.
    df1['Vol_2']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(2).sum())
    #Calcular media de Vol de 10 semanas
    df1['Volmed_10']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(10).mean())
    #Calcular media de Vol de 6 semanas
    df1['Volmed_6']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(6).mean())
    #Calcular media de Vol de 2 semanas
    df1['Volmed_2']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(2).mean())
    #VAR
    df1['Volvsmed10']=round(((df1['Volume']/df1['Volmed_10'])-1)*100,2)
    df1['Volvsmed6']=round(((df1['Volume']/df1['Volmed_6'])-1)*100,2)
    df1['Volvsmed2']=round(((df1['Volume']/df1['Volmed_2'])-1)*100,2)
    df1['V2vs10']=round(((df1['Volmed_2']/df1['Volmed_10'])-1)*100,2)
    df1['V6vs10']=round(((df1['Volmed_6']/df1['Volmed_10'])-1)*100,2)



    df1=df1.reset_index().groupby(['Date','activo']).last()

  elif freq=='diaria':

    #Ultimos 50 dias.
    df1['Vol_50']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(50).sum())
    #Ultimos 30 dias.
    df1['Vol_30']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(30).sum())
    #Ultimos 10 dias.
    df1['Vol_10']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(10).sum())
    #Calcular media de Vol de 50
    df1['Volmed_50']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(50).mean())
    #,30
    df1['Volmed_30']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(30).mean())
    #y 10 dias.
    df1['Volmed_10']=df1.groupby(by='activo',group_keys=False)['Volume'].apply(lambda x: x.rolling(10).mean())
    #VAR
    df1['Volvsmed50']=round(((df1['Volume']/df1['Volmed_50'])-1)*100,2)
    df1['Volvsmed30']=round(((df1['Volume']/df1['Volmed_30'])-1)*100,2)
    df1['Volvsmed10']=round(((df1['Volume']/df1['Volmed_10'])-1)*100,2)
    df1['V10vs50']=round(((df1['Volmed_10']/df1['Volmed_50'])-1)*100,2)
    df1['V30vs50']=round(((df1['Volmed_30']/df1['Volmed_50'])-1)*100,2)

    df1=pd.concat([df,df1],ignore_index=True)

  return df1

def prep_corractS(df):


  df1=df.copy()
  df1.reset_index(inplace=True)
  #Agrupacion por semana INDUSTRIA / ACTIVO
  df1=df1.groupby([pd.Grouper(key='Date', freq='W-FRI'),'activo'])['Close'].last()
  # columnas LEVEL=0 son las industrias/LEVEL=1 son los activos
  df1=df1.unstack(level=(1))
  df1.fillna(method='ffill', inplace=True)
  matriznorm = pd.DataFrame()

  #normaliza entre 0 ~ 1 y suma los sectores/idustria para poder calcular la correlacion.

  for activo in df1.columns:
      data = (df1[activo] - df1[activo].min()) / (df1[activo].max() - df1[activo].min())
      matriznorm[activo] = data

  #Datos normalizados de cada industria/sector CACULA LA CORRELACION EN BASE A PRECIO

  #Saco datos del MERVAL para poder calcular la correlacion.
  df3=df[df.activo=='^MERV']['Close'].copy()
  df3=df3.reset_index()
  df3=df3.groupby(pd.Grouper(key="Date", freq="W-FRI")).last()


  # NORMALIZO DATOS DEL MERVAL

  df3=(df3-df3.min())/(df3.max()-df3.min())
  df3=df3.rename(columns={'Close':'Indice'})

  matrizprecionorm=pd.concat([matriznorm,df3],axis=1)# UNIFICO


  #RETORNOS IDEM ANTERIOR PERO CALCULADO EN RETORNOS

  retorno=pd.DataFrame(index=df1.index,columns=df1.columns)

  df1.fillna(method='ffill', inplace=True)

  #for i in df1.columns:#df1.columns.levels[1]

  retorno=(df1/df1.shift(1))-1

  #objetivo normalizar los retornos.

  df4 = pd.DataFrame()

  for activo in retorno.columns:

    valores=retorno[activo]

    #REEMPLAZAMOS -1 y inf, -inf

    #remplazamos -1

    min_value = np.nanmin(valores[valores != -1])

    #replace inf and -inf in all columns with max value
    valores.replace([-1], min_value, inplace=True)

    #remplazamos inf por valor maximo
    max_value = np.nanmax(valores[valores != np.inf])

    #replace inf and -inf in all columns with max value
    valores.replace([np.inf, -np.inf], [max_value,min_value], inplace=True)

    data = ((valores*1000) - (valores.min()*1000)) / ((valores.max()*1000) - (valores.min()*1000))

    df4[activo] = data

  df3=df[df.activo=='^MERV']['Close']
  df3=df3.reset_index()
  df3=df3.groupby(pd.Grouper(key="Date", freq="W-FRI")).last()
  df3=(df3/df3.shift(1))-1
  df3=(df3-df3.min())/(df3.max()-df3.min())
  #df3.name='Indice'
  df3=df3.rename(columns={'Close':'Indice'})
  df3.fillna(method='ffill', inplace=True)

  retorno=pd.concat([df4,df3],axis=1) 

  retorno['Indice'].fillna(method='ffill', inplace=True) 
  matrizprecionorm['Indice'].fillna(method='ffill', inplace=True)

  return matrizprecionorm,retorno

def RSS(df,benchmark,media=36):
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
  indice=df[df['activo']==objetivo]['Close'] # contra que se compara cada activo
  activo=df.groupby(['Date','activo'])['Close'].sum().unstack('activo')
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

  return (RS)


def prep_comportS(base,benchmark,n=24):


  periodo=24
  df2,retorno=prep_corractS(base)
  corr=corr_activos(df2,retorno,periodo=n)
  Beta=beta(retorno,periodo=n)
  rs_base=RSS(base,benchmark=benchmark)
  ultima_fecha=rs_base.index.get_level_values(0)[-1]
  rs=rs_base.loc[ultima_fecha]['DIF_RS_media']

  return base,corr,Beta,rs

def actividad_inusualS(df,umbralmed=100,umbralprev=40):

  # parametros para inusual.
  df['inusual6']=np.where(df['Volvsmed6']>100,1,0)

  # para definir si es mayor en un 40% al dia anterior, hay que sacar los porcentajes.

  df['inusualprev']=np.where(((df['Volume']/df['Volume'].shift((len(df.index.get_level_values(1).unique()))))-1)*100>40,1,0)

  #unificando concepto de inusual

  df['inusual']=np.where((df['inusual6']==1)&(df['inusualprev']==1),1,0)

  return df

def screen_activosS(df,industrias='',Activos='todos'):

  '''
  Activos: "mas activos" los que salen del los sectores mas activos
  Activos: "lideres" merval lideres
  Activos: "general" panel genral
  Activos: "todos" todos los activos
  '''

  if Activos=='mas activos':

    listadoact=df[df['industria'].isin(industrias.index)].groupby(['Date','activo']).sum()

  elif Activos=='lideres':

    lideres=['ALUA','BBAR','BMA','BYMA','CEPU','COME','CRES','CVH','EDN','GGAL','HARG','LOMA','MIRG','PAMP','SUPV','TECO2','TGNO4','TGSU','TRAN','TXAR','VALO','YPF']
    for i in range(len(lideres)):
      lideres[i]+=".BA"

    listadoact=df.groupby(['Date','activo']).sum(numeric_only=True)
    listadoact=listadoact[listadoact.index.isin(lideres,level='activo')]# solo lideres


  elif Activos=='general':

    listadoact=df.groupby(['Date','activo']).sum(numeric_only=True)

    tickets=listadoact.index.get_level_values(2).unique().tolist()
    lideres=['ALUA','BBAR','BMA','BYMA','CEPU','COME','CRES','CVH','EDN','GGAL','HARG','LOMA','MIRG','PAMP','SUPV','TECO2','TGNO4','TRAN','TXAR','VALO','YPFD']
    for i in range(len(lideres)):
      lideres[i]+=".BA"
    for i in lideres:
      tickets.pop(tickets.index(i))

    #listadoact=df.data.groupby(['Date','industria','activo']).sum()
    listadoact=listadoact[listadoact.index.isin(tickets,level='activo')]# solo generales

  elif Activos=='todos':

    listadoact=df.groupby(['Date','activo']).sum(numeric_only=True)


  #PRIMERA PARTE
  base=actividad_inusualS(listadoact)
  base=base.reindex().groupby(['Date','activo']).sum()
  #normalizar?? No por que no tiene nada que ver que tenga mas activos y por ende no va a tener mas actividad inusual.
  inusual=actividad_inusualS(base)
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
  base2=inusual.reindex().groupby(['Date','activo']).sum()['V2vs10'].unstack(1).resample('W-FRI').sum()

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

def comportamiento_activosS(df,corr,Beta,RS,Activos='todos'):
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
  resumen=screen_activosS(df,Activos=Activos) # esta funcion es la que aplica el filtro al tipo de activos.
  resumen=pd.concat([resumen,corr],axis=1,join='inner')
  resumen=pd.concat([resumen,Beta],axis=1,join='inner')
  resumen=pd.concat([resumen,RS],axis=1,join='inner')
  resumen=resumen.sort_values('DIF_RS_media',ascending=False)

  return resumen
