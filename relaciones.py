# RS, BETA, CORR, ETC
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
