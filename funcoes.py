from plotly.subplots import make_subplots
import pandas as pd
import plotly.graph_objects as go

def rentabilidade_fundo(df: pd.DataFrame, cnpj: str, nome_fundo: str):
    """
    Calcula a rentabilidade mensal e anual do fundo selecionado.

    Parameters:
    df: Dataframe que contem as cotas dos fundos.
    cnpj: cnpj do fundo de investimento que você está procurando.
    nome_fundo: nome do fundo.

    Returns:
    df_ret_mensal: Dataframe dos retornos mensais.
    df_ret_anual: Dataframe dos retornos anuais.

    NOTE:Para calcular a rentabilidade mensal dos fundos - o site MaisRetorno utiliza última cota do mês anterior com a última cota do mês seguinte.
    Para calcular a rentabilidade anual dos fundos - o site MaisRetorno utiliza última cota do ano anterior com a última cota do ano seguinte.
    Por isso, eu tive que baixar a cota do mês 12/2022.
    """
    # Selecionando o fundo de investimentos específicos
    filt_cnpj = (df['CNPJ_FUNDO'] == cnpj)
    fundo_espec = df.loc[filt_cnpj]

    # Selecionando os últimos dias de cada mês
    last_days = fundo_espec.groupby(fundo_espec.index.to_period('M')).tail(1)
    # Selecionando apenas a coluna 'VL_QUOTA'
    last_days = last_days.loc[:, 'VL_QUOTA']
    # Calculando a rentabilidade do mês
    ret_mensal = round(last_days.pct_change()*100, 2)
    # Como eu estou utilizando a última cota do mês anterior com a a última cota do mês seguinte, a 1º rentabilidade ('2022-12-30') dessa série irá ser um NaN
    ret_mensal = ret_mensal.dropna()
    # Criando o df p/ os retornos mensais
    df_ret_mensal = pd.DataFrame(ret_mensal)
    # Renomeando a coluna 'VL_QUOTA'
    df_ret_mensal = df_ret_mensal.rename(columns={f'VL_QUOTA':'ret'})

    # Transformando as poncentagens em taxa unitária
    df_ret_mensal['taxa_unit'] = 1 + (df_ret_mensal[f'ret'] / 100) 
    # Lista dos anos
    lst_years = df_ret_mensal.index.year.unique()
    # Transformando em string
    lst_years = lst_years.astype(str)
    # Calculando o retorno anual - aculumando as porcentagens de cada ano
    lst_ret_anual = []
    for _ in lst_years:
        ret_anual = round((df_ret_mensal.loc[_, 'taxa_unit'].agg(lambda x : x.prod()) -1) * 100, 2)
        lst_ret_anual.append(ret_anual)
    # Criando o df p/ os retornos anuais
    df_ret_anual = pd.DataFrame(lst_ret_anual, index=lst_years, columns=['ret'])
    # Adicionando o nome do fundo na string 
    df_ret_anual = df_ret_anual.rename(columns={'ret': f'ret_{nome_fundo}'})
    # Transformando o index em datetime
    df_ret_anual.index = pd.to_datetime(df_ret_anual.index)
    # Transformando o formato do index ('ano') p/ conseguir concatenar com as rentabilidades dos fundos
    df_ret_anual.index = df_ret_anual.index.to_period('Y')

    # Para não mostrar a coluna 'taxa_unit', selecionando apenas a coluna 'ret_mensal'
    df_ret_mensal_final = df_ret_mensal[[f'ret']]
    # Transformando o formato do index para 'ano-mes'
    df_ret_mensal_final.index = df_ret_mensal.index.to_period('M')
    # Adicionando o nome do fundo na string 
    df_ret_mensal_final = df_ret_mensal_final.rename(columns={'ret': f'ret_{nome_fundo}'})

    return df_ret_mensal_final, df_ret_anual


def rentabilidade_fundo_benchmark(
        df_fundo: pd.DataFrame, 
        df_benchmark: pd.DataFrame, 
        nome_fundo: str,
        nome_benchmark: str
    ) -> pd.DataFrame:
    """
    Parameters:
    df_fundo: Dataframe de rentabilidade do fundo.
    df_benchmark: Dataframe de rentabilidade do benchmark.
    nome_fundo: nome do fundo.
    nome_benchmark: nome do benchmark.

    Returns:
    Dataframe que contém a rentabilidade do fundo e do benchmark.
    """
    # Juntando com o df da rentabilidade mensal com o df de rentabilidade do benchmark
    df_ret_fundo_benchmark = pd.concat([df_fundo, df_benchmark], axis=1)

    # Calculando a performance do fundo - ele conseguiu bater o benchmark?
    df_ret_fundo_benchmark['performance'] = df_ret_fundo_benchmark[f'ret_{nome_fundo}'] - df_ret_fundo_benchmark[f'ret_{nome_benchmark}']

    return df_ret_fundo_benchmark


def open_cda_1(path: str) -> pd.DataFrame:
    """
    Formatando o arquivo 'cda_fi_BLC_1'.

    Parameters:
    path: caminho do arquivo.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_1'
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df['TP_FUNDO'] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    df = df[['TP_FUNDO', 'CNPJ_FUNDO', 'DENOM_SOCIAL','DT_COMPTC' , 'TP_APLIC', 'TP_ATIVO', 'VL_MERC_POS_FINAL', 'TP_TITPUB', 'DT_VENC']]

    # Mesclando as colunas 'TP_TITPUB' e 'DT_VENC' em apenas em uma coluna
    df['TP_TITPUB'] = df['TP_TITPUB'] + ' ' + df['DT_VENC']

    # Removendo a coluna 'DT_VENC'
    df = df.drop('DT_VENC', axis=1)

    # Renomeando a coluna 'TP_TITPUB' p/ 'CD_ATIVO'. Assim fica igual ao df do arquivo cda_fi_BLC_2/4/7/8 para fazer depois juntar os dfs
    df = df.rename(columns={'TP_TITPUB':'CD_ATIVO'})

    # Transformando os dtypes das colunas.
    df['TP_FUNDO'] = df['TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df['DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df['TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df['TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df['VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df['CD_ATIVO'].astype(str)

    return df


def open_cda_2(path: str) -> pd.DataFrame:
    """
    Formatando o arquivo 'cda_fi_BLC_2'.

    Parameters:
    path: caminho do arquivo.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_2'
    """
    # Lendo o arquivo. Adicionei o 'low_memory=False' para não dar o aviso -> DtypeWarning: Columns (7) have mixed types. Specify dtype option on import or set low_memory=False
    df = pd.read_parquet(path)

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df['TP_FUNDO'] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    df = df[['TP_FUNDO', 'CNPJ_FUNDO', 'DENOM_SOCIAL','DT_COMPTC' , 'TP_APLIC', 'TP_ATIVO', 'VL_MERC_POS_FINAL', 'NM_FUNDO_COTA']]

    # Renomeando a coluna 'NM_FUNDO_COTA' p/ 'CD_ATIVO'. Assim fica igual ao df do arquivo cda_fi_BLC_4/7/8 para fazer depois juntar os dfs
    df = df.rename(columns={'NM_FUNDO_COTA':'CD_ATIVO'})

    # Transformando os dtypes das colunas
    df['TP_FUNDO'] = df['TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df['DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df['TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df['TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df['VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df['CD_ATIVO'].astype(str)

    return df


def open_cda_4(path: str) -> pd.DataFrame:
    """
    Formatando o arquivo 'cda_fi_BLC_4'.

    Parameters:
    path: caminho do arquivo.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_4'
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df['TP_FUNDO'] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    df = df[['TP_FUNDO', 'CNPJ_FUNDO', 'DENOM_SOCIAL','DT_COMPTC' , 'TP_APLIC', 'TP_ATIVO', 'VL_MERC_POS_FINAL', 'CD_ATIVO']]

    # Transformando os dtypes das colunas
    df['TP_FUNDO'] = df.loc[:, 'TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df.loc[:, 'CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df.loc[:, 'DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df.loc[:, 'TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df.loc[:, 'TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df.loc[:, 'VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df.loc[:, 'CD_ATIVO'].astype(str)

    return df


def open_cda_7(path: str) -> pd.DataFrame:
    """
    Formatando o arquivo 'cda_fi_BLC_7'.

    Parameters:
    path: caminho do arquivo.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_7'
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df['TP_FUNDO'] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas.
    df = df[['TP_FUNDO', 'CNPJ_FUNDO', 'DENOM_SOCIAL','DT_COMPTC' , 'TP_APLIC', 'TP_ATIVO', 'VL_MERC_POS_FINAL', 'EMISSOR']]

    # Renomeando a coluna 'EMISSOR' p/ 'CD_ATIVO'. Assim fica igual ao df do arquivo cda_fi_BLC_4 p/ fazer depois juntar os dfs.
    df.rename(columns={"EMISSOR": "CD_ATIVO"}, inplace=True)

    # Transformando os dtypes das colunas.
    df['TP_FUNDO'] = df.loc[:, 'TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df.loc[:, 'CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df.loc[:, 'DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df.loc[:, 'TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df.loc[:, 'TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df.loc[:, 'VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df.loc[:, 'CD_ATIVO'].astype(str)

    return df


def open_cda_8(path: str) -> pd.DataFrame:
    """
    Formatando o arquivo 'cda_fi_BLC_8'.

    Parameters:
    path: caminho do arquivo.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_8'
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df['TP_FUNDO'] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    df = df[['TP_FUNDO', 'CNPJ_FUNDO', 'DENOM_SOCIAL','DT_COMPTC' , 'TP_APLIC', 'TP_ATIVO', 'VL_MERC_POS_FINAL', 'DS_ATIVO']]

    # Renomeando a coluna 'DS_ATIVO' p/ 'CD_ATIVO'. Assim fica igual ao df do arquivo cda_fi_BLC_4 p/ fazer depois juntar os dfs
    df.rename(columns={"DS_ATIVO": "CD_ATIVO"}, inplace=True)

    # Transformando os dtypes das colunas
    df['TP_FUNDO'] = df.loc[:, 'TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df.loc[:, 'CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df.loc[:, 'DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df.loc[:, 'TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df.loc[:, 'TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df.loc[:, 'VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df.loc[:, 'CD_ATIVO'].astype(str)

    # Selecionando apenas o ativo 'BDR', porque neste arquivo também possui um ativo chamado 'Títulos Públicos', mas não é o principal 'Títulos Públicos', que está no 'cda_fi_BLC_1'
    filt_bdr = (df['TP_APLIC'] == 'Brazilian Depository Receipt - BDR')
    df = df.loc[filt_bdr].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    
    return df


def pl_fundo(path: str, cnpj: str) -> pd.DataFrame:
    """
    Formatando o arquivo 'cda_fi_PL'.
    
    Paramenters:
    path: caminho do arquivo.
    cnpj: cnpj do fundo de investimento que você está procurando.

    Returns:
    Dataframe com o valor do patrimônio líquido do fundo de investimentos específico.
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df['TP_FUNDO'] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando o fundo de investimentos específicos
    filt_cnpj = df['CNPJ_FUNDO'] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # # Transformando os dtypes da coluna
    # fundo_espec['VL_PATRIM_LIQ'] = fundo_espec.loc[:, 'VL_PATRIM_LIQ'].astype(float)

    return fundo_espec['VL_PATRIM_LIQ']


def fundo_cnpj(df: pd.DataFrame, cnpj: str) -> pd.DataFrame:
    """
    Separa o df do fundo de investimentos em várias categorias.

    Parameters:
    df: Dataframe que contém os ativos dos fundos.
    cnpj: cnpj do fundo de investimento que você está procurando.

    Returns:
    Vários dataframes de categorias diferentes: ações, BDRs, investimentos no exterior, cotas de fundos e títulos públicos.
    """
    # Lendo o df concatenado
    filt_cnpj = df['CNPJ_FUNDO'] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # Ações
    filt_acoes = (fundo_espec['TP_APLIC'] == 'Ações')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_acoes = fundo_espec.loc[filt_acoes].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada ação
    porcentagem_acao = lambda x: (x / df_acoes['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_acoes['PORCENTAGEM'] = list(map(porcentagem_acao, df_acoes['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_acoes = df_acoes.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # BDRs
    filt_bdr = (fundo_espec['TP_APLIC'] == 'Brazilian Depository Receipt - BDR')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_bdr = fundo_espec.loc[filt_bdr].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada ação
    porcentagem_bdr = lambda x: (x / df_bdr['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_bdr['PORCENTAGEM'] = list(map(porcentagem_bdr, df_bdr['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_bdr =  df_bdr.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # Investimentos Exterior
    filt_exterior = (fundo_espec['TP_APLIC'] == 'Investimento no Exterior')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_exterior = fundo_espec.loc[filt_exterior].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada ação
    porcentagem_exterior = lambda x: (x / df_exterior['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_exterior['PORCENTAGEM'] = list(map(porcentagem_exterior, df_exterior['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_exterior = df_exterior.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # Cotas de Fundos
    filt_cotas_fundos = (fundo_espec['TP_APLIC'] == 'Cotas de Fundos')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_cotas_fundos = fundo_espec.loc[filt_cotas_fundos].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada cota de fundo
    porcentagem_cotas = lambda x: (x / df_cotas_fundos['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_cotas_fundos['PORCENTAGEM'] = list(map(porcentagem_cotas, df_cotas_fundos['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_cotas_fundos = df_cotas_fundos.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # Títulos públicos
    filt_titulos_pub = (fundo_espec['TP_APLIC'] == 'Títulos Públicos')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_titulos_pub = fundo_espec.loc[filt_titulos_pub].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada título público
    porcentagem_titulos = lambda x: (x / df_titulos_pub['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_titulos_pub['PORCENTAGEM'] = list(map(porcentagem_titulos, df_titulos_pub['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_titulos_pub = df_titulos_pub.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # Obrigações por ações e outros TVM recebidos em empréstimo
    filt_vendido_acoes = (fundo_espec['TP_APLIC'] == 'Obrigações por ações e outros TVM recebidos em empréstimo')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_vendido_acoes = fundo_espec.loc[filt_vendido_acoes].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada título público
    porcentagem_vendido = lambda x: (x / df_vendido_acoes['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_vendido_acoes['PORCENTAGEM'] = list(map(porcentagem_vendido, df_vendido_acoes['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_vendido_acoes = df_vendido_acoes.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    return (
        df_acoes, 
        df_bdr, 
        df_exterior, 
        df_cotas_fundos, 
        df_titulos_pub, 
        df_vendido_acoes
    )


def fundo_cnpj_acoes(df: pd.DataFrame, cnpj: str) -> pd.DataFrame:
    """
    Separa o df do fundo de investimento apenas na categoria de ações.

    Parameters:
    df: Dataframe que contém os ativos dos fundos.
    cnpj: cnpj do fundo de investimento que você está procurando.

    Returns:
    Dataframe das ações do fundo selecionado.
    
    """
    # Lendo o df concatenado
    filt_cnpj = df['CNPJ_FUNDO'] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # Ações
    filt_acoes = (fundo_espec['TP_APLIC'] == 'Ações')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_acoes = fundo_espec.loc[filt_acoes].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada ação
    porcentagem_acao = lambda x: (x / df_acoes['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_acoes['PORCENTAGEM'] = list(map(porcentagem_acao, df_acoes['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_acoes = df_acoes.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    return df_acoes


def fundo_cnpj_debentures(df: pd.DataFrame, cnpj: str) -> pd.DataFrame:
    """
    Separa o df do fundo de investimento apenas na categoria de debêntures.

    Parameters:
    df: Dataframe que contém os ativos dos fundos.
    cnpj: cnpj do fundo de investimento que você está procurando.

    Returns:
    Dataframe das debêntures do fundo selecionado.
    
    """
    # Lendo o df concatenado
    filt_cnpj = df['CNPJ_FUNDO'] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # Debêntures
    filt_debentures = (fundo_espec['TP_APLIC'] == 'Debêntures')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_debentures = fundo_espec.loc[filt_debentures].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada debênture
    porcentagem_debentures = lambda x: (x / df_debentures['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_debentures['PORCENTAGEM'] = list(map(porcentagem_debentures, df_debentures['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_debentures = df_debentures.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    return df_debentures


def comparar_portfolios(df: pd.DataFrame, nome_fundo: str) -> str:
    """
    Comparação do portfólio - quais foram as ações que foram compradas e vendidas em relação ao mês anteirior.

    Pameters:
    df: DataFrame de cada mês do portfólio do fundo.
    nome_fundo: nome do fundo.

    Returns:
    Texto com as mudanças do portfólio.
    """
    for i in range(1, len(df)):
        mes_atual = df.iloc[i]
        mes_anterior = df.iloc[i - 1]
        
        data_atual = mes_atual['data']
        data_anterior = mes_anterior['data']
        
        acoes_atual = mes_atual['CD_ATIVO']
        acoes_anterior = mes_anterior['CD_ATIVO']
        
        vendidas = acoes_anterior - acoes_atual
        compradas = acoes_atual - acoes_anterior
        
        print(f'Comparando o portfólio de {data_anterior.strftime("%m/%Y")} e {data_atual.strftime("%m/%Y")}:')
        print(f'O {nome_fundo} vendeu as ações: {vendidas}')
        print(f'O {nome_fundo} comprou as ações: {compradas}')
        print('-' * 80)


def num_total_acoes(df: pd.DataFrame) -> pd.Series:
    """
    Parameters:
    df: DataFrame do portfólio do fundo.

    Returns:
    Número total de ações de cada mês do portfpolio.
    """
    num_total_acoes = df.groupby('data')['CD_ATIVO'].count()
    
    return num_total_acoes


def rank_top_5(df: pd.DataFrame) -> pd.Series:
    """
    Parameters:
    df: DataFrame do portfólio do fundo.
    
    Returns:
    rank_portfolio_fundo: rank das 5 maiores posições do fundo.
    """
    # Selecionando os valores únicos (datas) do df
    lst_data = df.index.unique()

    # Rank das 5 maiores posições do fundo
    lst_rank = []
    for _ in lst_data:
        rank = df.loc[_].nlargest(5, 'PORCENTAGEM')[['CD_ATIVO', 'PORCENTAGEM']]
        lst_rank.append(rank)

    # Concatendo os dfs de rank
    rank_portfolio = pd.concat(lst_rank)
    rank_portfolio_fundo = rank_portfolio.groupby('data')['CD_ATIVO'].apply(list)

    return rank_portfolio_fundo


def plot_portfolio(df: pd.DataFrame, nome_fundo: str):
    """
    Parameters:
    df: Dataframe do portfólio do fundo selecionado.
    nome_fundo: nome do fundo.

    Returns:
    Plot do portfólio do fundo.
    """
    # Extraindo os meses únicos do índice
    months = df.index.to_period('M').unique()

    # Títulos dos subplots
    titulos = [f'Distribuição Percentual do Portfólio - {month.strftime("%m/%Y")}' for month in months]

    # Criando a figura com subplots
    fig = make_subplots(rows=len(months),
                        cols=1,
                        subplot_titles=titulos,
                        vertical_spacing=0.02  # Espaço entre os plots
    )

    # Iterando sobre os meses
    for idx, month in enumerate(months):
        # Extraindo os dados do portfólio para o mês atual
        portfolio_mes = df.loc[month.strftime('%Y-%m'), ['CD_ATIVO', 'PORCENTAGEM']]
        
        fig.add_trace(go.Bar(
            x=portfolio_mes['PORCENTAGEM'] * 100,
            y=portfolio_mes['CD_ATIVO'],
            orientation='h',
            name=month.strftime('%m/%Y')
        ), row=idx + 1, col=1)

    fig.update_layout(
        title=f'Portfólio do Fundo {nome_fundo}',
        height=6000,
        width=900
    )

    return fig.show()