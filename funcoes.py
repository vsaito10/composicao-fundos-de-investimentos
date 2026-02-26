from functools import reduce
from plotly.subplots import make_subplots
import calendar
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
import yfinance as yf


def rentabilidade_fundo(df: pd.DataFrame, cnpj: str, nome_fundo: str) -> pd.DataFrame:
    """
    Calcula a rentabilidade mensal e anual do fundo selecionado.

    Parameters
    ----------
    df: pd.DataFrame 
        Dataframe que contem as cotas dos fundos.
    cnpj: str
        Cnpj do fundo de investimento.
    nome_fundo: str
        Nome do fundo.

    Returns
    -------
    df_ret_mensal: pd.DataFrame 
        Dataframe dos retornos mensais.
    df_ret_anual: pd.DataFrame 
        Dataframe dos retornos anuais.

    Notes
    -----
    Para calcular a rentabilidade, o padrão seguido é o do site MaisRetorno:
    * Mensal: Utiliza a última cota do mês anterior vs. última cota do mês atual.
    * Anual: Utiliza a última cota do ano anterior vs. última cota do ano atual.
    
    Devido a essa metodologia, os dados de 12/2022 foram incluídos para viabilizar 
    o cálculo do primeiro período de 2023.
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
    df_ret_mensal = df_ret_mensal.rename(columns={'VL_QUOTA':'ret'})

    # Transformando as poncentagens em taxa unitária
    df_ret_mensal['taxa_unit'] = 1 + (df_ret_mensal['ret'] / 100) 
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
    df_ret_mensal_final = df_ret_mensal[['ret']]
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
    Calcula a rentabiliade do fundo selecionado.

    Parameters
    ----------
    df_fundo: pd.DataFrame
        Dataframe de rentabilidade do fundo.
    df_benchmark: pd.DataFrame
        Dataframe de rentabilidade do benchmark.
    nome_fundo: str
        Nome do fundo.
    nome_benchmark: str
        Nome do benchmark.

    Returns
    -------
    df_ret_fundo_benchmark: pd.DataFrame
        Dataframe que contém a rentabilidade do fundo e do benchmark.
    """
    # Juntando com o df da rentabilidade mensal com o df de rentabilidade do benchmark
    df_ret_fundo_benchmark = pd.concat([df_fundo, df_benchmark], axis=1)

    # Calculando a performance do fundo - ele conseguiu bater o benchmark?
    df_ret_fundo_benchmark['performance'] = df_ret_fundo_benchmark[f'ret_{nome_fundo}'] - df_ret_fundo_benchmark[f'ret_{nome_benchmark}']

    return df_ret_fundo_benchmark


def open_cda_1(path: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_BLC_1'.

    Parameters
    ----------
    path: str
        Caminho do arquivo 'open_cda_1'.

    Returns
    -------
    df: pd.DataFrame
        Dataframe do arquivo 'cda_fi_BLC_1'.
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df[tp_fundo_col] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'TP_TITPUB', 
        'DT_VENC'
    ]
    df = df[columns_to_keep]

    # Mesclando as colunas 'TP_TITPUB' e 'DT_VENC' em apenas uma coluna
    df['TP_TITPUB'] = df['TP_TITPUB'] + ' ' + df['DT_VENC']

    # Removendo a coluna 'DT_VENC'
    df = df.drop('DT_VENC', axis=1)

    # Renomeando as colunas para manter consistência
    df = df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO', 'TP_TITPUB': 'CD_ATIVO'})

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


def open_cda_2(path: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_BLC_2'.

    Parameters
    ----------
    path: str
        Caminho do arquivo 'open_cda_2'.

    Returns
    -------
    df: pd.DataFrame
        Dataframe do arquivo 'cda_fi_BLC_2'.
    """
    # Lendo o arquivo. Adicionei o 'low_memory=False' para não dar o aviso -> DtypeWarning: Columns (7) have mixed types. Specify dtype option on import or set low_memory=False
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")
    
    # Verificando as colunas 'NM_FUNDO_COTA' ou 'NM_FUNDO_CLASSE_SUBCLASSE_COTA'
    if 'NM_FUNDO_COTA' in df.columns:
        nm_fundo_cota_col = 'NM_FUNDO_COTA'
    elif 'NM_FUNDO_CLASSE_SUBCLASSE_COTA' in df.columns:
        nm_fundo_cota_col = 'NM_FUNDO_CLASSE_SUBCLASSE_COTA'
    else:
        raise ValueError("Nenhuma das colunas 'NM_FUNDO_COTA' ou 'NM_FUNDO_CLASSE_SUBCLASSE_COTA' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df[tp_fundo_col] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        nm_fundo_cota_col
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas. Assim fica igual ao df do arquivo cda_fi_BLC_4/7/8 para fazer depois juntar os dfs
    df = df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO', nm_fundo_cota_col:'CD_ATIVO'})

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
    Formata o arquivo 'cda_fi_BLC_4'.

    Parameters
    ----------
    path: str
        Caminho do arquivo 'open_cda_4'.

    Returns
    -------
    df: pd.DataFrame
        Dataframe do arquivo 'cda_fi_BLC_4'.
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos' e 'Fundo de Investimento Financeiro'
    filt_fi = df[tp_fundo_col].isin(['FI', 'CLASSES - FIF'])
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'CD_ATIVO'
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas para manter consistência
    df = df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO'})

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


def open_cda_4_v2(path: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_BLC_4'.

    Parameters
    ----------
    path: str
        Caminho do arquivo 'open_cda_4'.

    Returns
    -------
    df: pd.DataFrame
        Dataframe do arquivo 'cda_fi_BLC_4'.

    Notes
    -----
    Eu criei essa função para analisar melhor as posições de opções dos fundos com duas colunas a mais ('DT_INI_VIGENCIA' e 'DT_FIM_VIGENCIA').
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos' e 'Fundo de Investimento Financeiro'
    filt_fi = df[tp_fundo_col].isin(['FI', 'CLASSES - FIF'])
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'CD_ATIVO',
        'DT_INI_VIGENCIA',
        'DT_FIM_VIGENCIA'
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas para manter consistência
    df = df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO'})

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
    Formata o arquivo 'cda_fi_BLC_7'.

    Parameters
    ----------
    path: str
        Caminho do arquivo 'cda_fi_BLC_7'.

    Returns
    -------
    df: pd.DataFrame
        Dataframe do arquivo 'cda_fi_BLC_7'.
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df[tp_fundo_col] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'EMISSOR'
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas. Assim fica igual ao df do arquivo cda_fi_BLC_4 p/ fazer depois juntar os dfs.
    df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO', 'EMISSOR': 'CD_ATIVO'}, inplace=True)

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
    Formata o arquivo 'cda_fi_BLC_8'.

    Parameters
    ----------
    path: str
        Caminho do arquivo 'cda_fi_BLC_8'.

    Returns
    -------
    df: pd.DataFrame
        Dataframe do arquivo 'cda_fi_BLC_8'.
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df[tp_fundo_col] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'DS_ATIVO'
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas. Assim fica igual ao df do arquivo cda_fi_BLC_4 p/ fazer depois juntar os dfs
    df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO', 'DS_ATIVO': 'CD_ATIVO'}, inplace=True)

    # Transformando os dtypes das colunas
    df['TP_FUNDO'] = df.loc[:, 'TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df.loc[:, 'CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df.loc[:, 'DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df.loc[:, 'TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df.loc[:, 'TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df.loc[:, 'VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df.loc[:, 'CD_ATIVO'].astype(str)

    # Selecionando apenas o ativo 'BDR' e 'Ações' (Units), porque neste arquivo também possui um ativo chamado 'Títulos Públicos', mas não é o principal 'Títulos Públicos', que está no 'cda_fi_BLC_1'
    filt = (df['TP_APLIC'] == 'Brazilian Depository Receipt - BDR') | (df['TP_APLIC'] == 'Ações')
    df = df.loc[filt].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    
    return df


def pl_fundo(path: str, cnpj: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_PL'.
    
    Parameters
    ----------
    path: str
        Caminho do arquivo 'cda_fi_PL'.
    cnpj: str
        Cnpj do fundo de investimento.

    Returns
    -------
    fundo_espec: pd.DataFrame
        Dataframe com o valor do patrimônio líquido do fundo de investimentos específico.
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos' e 'Fundo de Investimento Financeiro'
    filt_fi = df[tp_fundo_col].isin(['FI', 'CLASSES - FIF'])
    df = df.loc[filt_fi]

    # Selecionando o fundo de investimentos específicos
    filt_cnpj = df[cnpj_fundo_col] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # # Transformando os dtypes da coluna
    # fundo_espec['VL_PATRIM_LIQ'] = fundo_espec.loc[:, 'VL_PATRIM_LIQ'].astype(float)

    return fundo_espec['VL_PATRIM_LIQ']


def fundo_cnpj(df: pd.DataFrame, cnpj: str):
    """
    Separa o df do fundo de investimentos em várias categorias.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe que contém os ativos dos fundos.
    cnpj: str
        Cnpj do fundo de investimento.

    Returns
    -------
    df_acoes: pd.DataFrame
        DataFrame das ações.
    df_bdr: pd.DataFrame
        DataFrame das BDRs.  
    df_exterior: pd.DataFrame
        DataFrame das ações nos exterior.
    df_cotas_fundos: pd.DataFrame
        DataFrame das cotas dos fundos de investimentos.
    df_titulos_pub: pd.DataFrame
        DataFrame dos títulos públicos.
    df_vendido_acoes: pd.DataFrame
        DataFrame das ações vendidas.
    """
    # Filtro inicial
    fundo_espec = df[df['CNPJ_FUNDO'] == cnpj].copy()

    # Função auxiliar para evitar repetição (DRY)
    def processar_categoria(tp_aplic):
        sub_df = fundo_espec[fundo_espec['TP_APLIC'] == tp_aplic].copy()
        sub_df = sub_df.sort_values(by='VL_MERC_POS_FINAL', ascending=False)
        
        # Cálculo vetorizado (sem lambda, sem erro de linter)
        total = sub_df['VL_MERC_POS_FINAL'].sum()
        if total != 0:
            sub_df['PORCENTAGEM'] = sub_df['VL_MERC_POS_FINAL'] / total
        else:
            sub_df['PORCENTAGEM'] = 0
            
        return sub_df[['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # Aplicando para cada categoria
    df_acoes = processar_categoria('Ações')
    df_bdr = processar_categoria('Brazilian Depository Receipt - BDR')
    df_exterior = processar_categoria('Investimento no Exterior')
    df_cotas_fundos = processar_categoria('Cotas de Fundos')
    df_titulos_pub = processar_categoria('Títulos Públicos')
    df_vendido_acoes = processar_categoria('Obrigações por ações e outros TVM recebidos em empréstimo')

    return (
        df_acoes, df_bdr, df_exterior, 
        df_cotas_fundos, df_titulos_pub, df_vendido_acoes
    )


def fundo_cnpj_acoes(df: pd.DataFrame, cnpj: str) -> pd.DataFrame:
    """
    Separa o df do fundo de investimento apenas na categoria de ações.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe que contém os ativos dos fundos.
    cnpj: str
        Cnpj do fundo de investimento.

    Returns
    -------
    df_acoes: pd.DataFrame
        Dataframe das ações do fundo selecionado.
    """
    # Filtrando o fundo e criando uma cópia explícita para evitar avisos
    fundo_espec = df.loc[df['CNPJ_FUNDO'] == cnpj].copy()

    # Filtro de Ações
    filt_acoes = (fundo_espec['TP_APLIC'] == 'Ações')
    df_acoes = fundo_espec.loc[filt_acoes].sort_values(by='VL_MERC_POS_FINAL', ascending=False).copy()

    # Calculando a porcentagem de forma vetorizada 
    total_valor = df_acoes['VL_MERC_POS_FINAL'].sum()
    if total_valor > 0:
        df_acoes['PORCENTAGEM'] = df_acoes['VL_MERC_POS_FINAL'] / total_valor
    else:
        df_acoes['PORCENTAGEM'] = 0

    # Selecionando, renomeando e definindo index
    df_acoes = (df_acoes[['DT_COMPTC', 'DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]
                .rename(columns={'DT_COMPTC': 'data'})
                .set_index('data'))

    return df_acoes


def fundo_cnpj_debentures(df: pd.DataFrame, cnpj: str) -> pd.DataFrame:
    """
    Separa o df do fundo de investimento apenas na categoria de debêntures.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe que contém os ativos dos fundos.
    cnpj: str
        Cnpj do fundo de investimento.

    Returns
    -------
    df_debentures: pd.DataFrame
        Dataframe das debêntures do fundo selecionado.
    """
    # Filtrando o CNPJ e criando uma cópia para evitar SettingWithCopyWarning
    fundo_espec = df.loc[df['CNPJ_FUNDO'] == cnpj].copy()

    # Filtrando apenas Debêntures
    filt_debentures = fundo_espec['TP_APLIC'] == 'Debêntures'
    df_debentures = fundo_espec.loc[filt_debentures].sort_values(by='VL_MERC_POS_FINAL', ascending=False).copy()

    # Calculando a porcentagem de forma vetorizada 
    soma_total = df_debentures['VL_MERC_POS_FINAL'].sum()
    
    if soma_total > 0:
        df_debentures['PORCENTAGEM'] = df_debentures['VL_MERC_POS_FINAL'] / soma_total
    else:
        df_debentures['PORCENTAGEM'] = 0

    # Selecionando apenas as colunas necessárias
    colunas = ['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']
    df_debentures = df_debentures[colunas]

    return df_debentures


def fundo_cnpj_opcoes(df: pd.DataFrame, cnpj: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separa o df do fundo de investimento apenas na categoria de opções.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe que contém os ativos dos fundos.
    cnpj: str
        Cnpj do fundo de investimento.

    Returns
    -------
    df_opcoes_compradas: pd.DataFrame
        Dataframe das opções compradas do fundo selecionado.
    df_opcoes_vendidas: pd.DataFrame
        Dataframe das opções vendidas do fundo selecionado.
    """
    # Filtro inicial pelo CNPJ
    fundo_espec = df.loc[df['CNPJ_FUNDO'] == cnpj].copy()

    # Função interna para evitar repetição de código (DRY)
    def processar_opcoes(tipo_aplicacao: str):
        # Filtra e ordena
        df_filtrado = fundo_espec.loc[fundo_espec['TP_APLIC'] == tipo_aplicacao].copy()
        df_filtrado = df_filtrado.sort_values(by='DT_COMPTC', ascending=True)

        # CÁLCULO VETORIZADO: Substitui a lambda e o map
        # Isso é mais rápido e o linter não reclama
        total_posicao = df_filtrado['VL_MERC_POS_FINAL'].sum()
        if total_posicao != 0:
            df_filtrado['PORCENTAGEM'] = df_filtrado['VL_MERC_POS_FINAL'] / total_posicao
        else:
            df_filtrado['PORCENTAGEM'] = 0.0

        # Seleção de colunas, renomeação e índice
        cols = ['DT_COMPTC', 'DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 
                'VL_MERC_POS_FINAL', 'DT_INI_VIGENCIA', 'DT_FIM_VIGENCIA']
        
        df_filtrado = (df_filtrado[cols]
                       .rename(columns={'DT_COMPTC': 'data'})
                       .set_index('data'))
        
        return df_filtrado

    # Processando as duas categorias
    df_opcoes_compradas = processar_opcoes('Opções - Posições titulares')
    df_opcoes_vendidas = processar_opcoes('Opções - Posições lançadas')

    return df_opcoes_compradas, df_opcoes_vendidas


def comparar_portfolios(df: pd.DataFrame, nome_fundo: str) -> str:
    """
    Comparação do portfólio - quais foram as ações que foram compradas e vendidas em relação ao mês anteirior.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame de cada mês do portfólio do fundo.
    nome_fundo: str
        Nome do fundo.

    Returns
    -------
    str
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
    Mostra o número total do portfólio do fundo.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame do portfólio do fundo.

    Returns
    -------
    num_total_acoes: pd.Series
        Número total de ações de cada mês do portfpolio.
    """
    num_total_acoes = df.groupby('data')['CD_ATIVO'].count()
    
    return num_total_acoes


def rank_top_5(df: pd.DataFrame) -> pd.Series:
    """
    Mostra o rank das 5 maiores posições do fundo.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame do portfólio do fundo.
    
    Returns
    -------
    rank_portfolio_fundo: pd.Series
        Rank das 5 maiores posições do fundo.
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
    Gráfico de barras do portfólio do fundo.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe do portfólio do fundo selecionado.
    nome_fundo: str
        Nome do fundo.

    Returns
    -------
    fig : plotly.graph_objects.Figure
        Gráfico de barra do portfólio do fundo.
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


def open_arquivos_fii(fii_ativo_passivo_path: str, fii_complemento_path: str, fii_geral_path: str) -> pd.DataFrame:
    """
    Formata e compila os arquivos mensais dos FIIs.

    Parameters
    ----------
    fii_ativo_passivo_path: str
        Caminho do arquivo "inf_mensal_fii_ativo_passivo_XXXX".
    fii_complemento_path: str
        Caminho do arquivo "inf_mensal_fii_complemento_XXXX".
    fii_geral_path: str
        Caminho do arquivo "inf_mensal_fii_geral_XXXX".

    Returns
    -------
    df_fii: pd.DataFrame
        DataFrame com os principais dados sobre os FIIs.
    """
    # Lendo os arquivos parquet
    df_ativo_passivo = pd.read_parquet(fii_ativo_passivo_path)
    df_complemento = pd.read_parquet(fii_complemento_path)
    df_geral = pd.read_parquet(fii_geral_path)

    # Identificando o nome correto da coluna de CNPJ em cada DataFrame
    cnpj_fundo_col_ativo_passivo = 'CNPJ_Fundo' if 'CNPJ_Fundo' in df_ativo_passivo.columns else 'CNPJ_Fundo_Classe'
    cnpj_fundo_col_complemento = 'CNPJ_Fundo' if 'CNPJ_Fundo' in df_complemento.columns else 'CNPJ_Fundo_Classe'
    cnpj_fundo_col_geral = 'CNPJ_Fundo' if 'CNPJ_Fundo' in df_geral.columns else 'CNPJ_Fundo_Classe'

    # Renomeando a coluna de CNPJ para o mesmo nome p/ fazer a junção dos dfs
    df_ativo_passivo.rename(columns={cnpj_fundo_col_ativo_passivo: 'CNPJ_Fundo'}, inplace=True)
    df_complemento.rename(columns={cnpj_fundo_col_complemento: 'CNPJ_Fundo'}, inplace=True)
    df_geral.rename(columns={cnpj_fundo_col_geral: 'CNPJ_Fundo'}, inplace=True)

    # Selecionando as principais colunas
    df_ativo_passivo = df_ativo_passivo[[
        'Data_Referencia', 
        'CNPJ_Fundo', 
        'Obrigacoes_Aquisicao_Imoveis', 
        'Obrigacoes_Securitizacao_Recebiveis'
    ]]
    df_complemento = df_complemento[[
        'Data_Referencia', 
        'CNPJ_Fundo', 
        'Valor_Ativo', 
        'Patrimonio_Liquido', 
        'Cotas_Emitidas',
        'Valor_Patrimonial_Cotas', 
        'Percentual_Rentabilidade_Efetiva_Mes', 
        'Percentual_Dividend_Yield_Mes'
    ]]
    df_geral = df_geral[[
        'Data_Referencia', 
        'CNPJ_Fundo', 
        'Segmento_Atuacao'
    ]]

    # Ajustando a escala das colunas de porcentagem
    df_complemento['Percentual_Rentabilidade_Efetiva_Mes'] = round(df_complemento['Percentual_Rentabilidade_Efetiva_Mes'] * 100, 2)
    df_complemento['Percentual_Dividend_Yield_Mes'] = round(df_complemento['Percentual_Dividend_Yield_Mes'] * 100, 2)

    # Transformando em datetime a coluna 'Data_Referencia' e removendos os espaços em branco da coluna 'CNPJ_Fundo'
    for df in [df_ativo_passivo, df_complemento, df_geral]:
        df['Data_Referencia'] = pd.to_datetime(df['Data_Referencia'], format='%Y-%m-%d')
        df['CNPJ_Fundo'] = df['CNPJ_Fundo'].str.strip()

    # Lista dos dfs que vão ser mesclados
    lst_dfs = [df_ativo_passivo, df_complemento, df_geral]

    # Mesclando os dataframes
    df_fii = reduce(lambda left, right: pd.merge(left, right, on=['CNPJ_Fundo', 'Data_Referencia']), lst_dfs)

    # Calculando a dívida total
    df_fii['Divida_Total'] = df_fii['Obrigacoes_Aquisicao_Imoveis'] + df_fii['Obrigacoes_Securitizacao_Recebiveis']

    # Calculando o grau de alavancagem
    df_fii['Grau_Alavancagem'] = (df_fii['Divida_Total'] / df_fii['Valor_Ativo']) * 100

    # Preenchendo os NaN com zero
    df_fii = df_fii.fillna(0)

    return df_fii


def fii_cnpj(df: pd.DataFrame, cnpj: str, ticker: str) -> pd.DataFrame:
    """
    Mostra os principais indicadores do FII selecionado.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame que contém todos os FIIs
    cnpj: str
        Cnpj do FII selecionado.
    ticker: str
        Ticker do FII selecionado.

    Returns
    -------
    df_fii_final: pd.DataFrame
        DataFrame do FII selecionado com os seus principais indicadores.
    """
    # Selecionando um FII específico
    filt = df['CNPJ_Fundo'] == cnpj
    df_fii_espec = df.loc[filt]

    # Primeiro Ano
    primeiro_ano = df.index[0].year
    # Primeiro mês
    primeiro_mes = df.index[0].month
    # Último Ano
    ultimo_ano = df.index[-1].year
    # Último mês
    ultimo_mes = df.index[-1].month

    # Se o 'ultimo_mes' for diferente de 12
    if ultimo_mes != 12:
        # Próximo mês
        proximo_mes = ultimo_mes + 1
        # Último dia do próximo mês (dinâmico)
        ultimo_dia_proximo_mes = calendar.monthrange(ultimo_ano, proximo_mes)[1]
        # Fazendo o download dos preço do FII
        fii_preco = yf.download(
            ticker, 
            start=f'{primeiro_ano}-{primeiro_mes}-01', 
            end=f'{ultimo_ano}-{proximo_mes}-{ultimo_dia_proximo_mes}', 
            auto_adjust=True,
            multi_level_index=False
        )['Close']

    # Se o 'ultimo_mes' for 12
    else: 
        # Próximo mês
        proximo_mes = 1
        # Próximo ano
        proximo_ano = ultimo_ano + 1
        # Último dia do próximo mês (dinâmico)
        ultimo_dia_proximo_mes = calendar.monthrange(proximo_ano, proximo_mes)[1]
        # Fazendo o download dos preço do FII
        fii_preco = yf.download(
            ticker, 
            start=f'{primeiro_ano}-{primeiro_mes}-01', 
            end=f'{proximo_ano}-{proximo_mes}-{ultimo_dia_proximo_mes}', 
            auto_adjust=True,
            multi_level_index=False
        )['Close']

    # Resetando o index do df
    fii_preco = fii_preco.reset_index()
    # Renomeando as colunas 
    fii_preco.columns = ['Data_Referencia', 'Close']
    # Transformando a coluna 'Data_Referencia' em  index 
    fii_preco = fii_preco.set_index('Data_Referencia')
    # Usando o resample para agrupar por mês e selecionando o último valor de cada mês
    fii_preco = fii_preco.resample('ME').last()
    # Transformando os dias do index para 01 p/ juntar com o 'df_ifix'
    new_index = fii_preco.index.to_period('M').to_timestamp() + pd.offsets.Day(0)
    # Index novo em que o dia é 01
    fii_preco.index = new_index
    # Cortando o df para ficar no mesmo tamanho do 'df_ifix'
    fii_preco = fii_preco.loc[:f'{ultimo_ano}-{ultimo_mes}']

    # Juntando os dfs
    df_fii_final = pd.merge(fii_preco, df_fii_espec, left_index=True, right_index=True)

    # Calculando o P/VP
    df_fii_final['P/VP'] = round(df_fii_final['Close'] / df_fii_final['Valor_Patrimonial_Cotas'], 2)

    return df_fii_final


def plot_historico_p_vp(df: pd.DataFrame, nome_segmento: str):
    """
    Gráfico de linha do indicador P/VP dos FIIs.

    Parameters
    ----------
    df:  pd.DataFrame
        DataFrame do FII que contém a coluna 'P/VP'.
    nome_segmento: str
        Nome do segmento do FII.

    Returns
    -------
    fig : plotly.graph_objects.Figure
        Gráfico de linha do indicador P/VP dos FIIs.
    """
    # Plotando o histórico do indicador P/VP dos FIIs
    fig = go.Figure()

    for empresa in df['Ticker'].unique():
        fig.add_trace(go.Scatter(
                x=df.loc[df['Ticker'] == empresa].index,
                y=df.loc[df['Ticker'] == empresa, 'P/VP'],
                name=empresa
            ))

    fig.add_hline(y=1, line_color='red', line_width=0.5)

    fig.update_layout(title=f'Histórico do P/VP dos FIIs de {nome_segmento}')

    fig.show()


def filtro_etf(path: str) -> pd.DataFrame:
  """
  Função que filtra a composição da carteira do ETF.

  Parameters
  ----------
  path: str
    Caminho do arquivo do ETF.
  
  Returns
  -------
  df: pd.DataFrame
    DataFrame do ETF.
  """
  # Abrindo arquivo Ibovespa (carteira teórica)
  df = pd.read_csv(path, 
                   sep=';',
                   encoding='ISO-8859-1', 
                   engine='python', 
                   header=1, #cabeçalho vira a 2º linha da tabela
                   skipfooter=2, #pula as duas últimas linhas da tabela
                   index_col=False)
  
  # Convertendo o dtypes das colunas
  df['Código'] = df['Código'].astype(str)
  df['Ação'] = df['Ação'].astype(str)
  df['Tipo'] = df['Tipo'].astype(str)
  df['Qtde. Teórica'] = df['Qtde. Teórica'].str.replace('.', '', regex=False)
  df['Qtde. Teórica'] = df['Qtde. Teórica'].astype(float)
  df['Part. (%)'] = df['Part. (%)'].str.replace(',', '.', regex=False)
  df['Part. (%)'] = df['Part. (%)'].astype(float)

  return df


def vol_anual(lst_ticker: list, start_date: str, ano: str, setor: str) -> pd.DataFrame:
    """
    Calcula a volatilidade anualizada e mostra o gráfico de barras.

    Parameters
    ----------
    lst_ticker: list
        Lista com os tickers das empresas.
    start_date: str
        Data de início.
    ano: str
        Período escolhido.
    setor: str
        Nome do setor.

    Returns
    -------
    df_annualized_volatility: pd.DataFrame
        DataFrame da volatilidade anualiazada.
    fig : plotly.graph_objects.Figure
        Gráfico de barras da volatilidade anualiazada.

    Notes
    -----
    Para calcular a vol mensal trocar apenas o np.sqrt(12).
    Para calcular a vol semanal trocar apenas o np.sqrt(52).
    """
    lst_annualized_volatility = []

    for ticker in lst_ticker:
        # Preço de fechamento do ativo
        df_preco = yf.download(ticker, start=start_date, auto_adjust=True, multi_level_index=False)['Close']
        # Calculando o retorno logarítmico
        log_return = np.log(df_preco.loc[ano] / df_preco.loc[ano].shift(1))
        # Calculando a volatilidade anualizada
        annualized_volatility = round((np.std(log_return) * np.sqrt(252)) * 100, 2)
        # Adicionando na lista
        lst_annualized_volatility.append(annualized_volatility)

    # Criando o df
    df_annualized_volatility = pd.DataFrame(lst_annualized_volatility, columns=['vol_anual'], index=lst_ticker).sort_values(by='vol_anual')

    # Plotando a volatilidade anual
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_annualized_volatility.index,
        y=df_annualized_volatility['vol_anual']
    ))

    fig.update_layout(
        title_text=f'Volatilidade Anual - FIIs {setor}',
        template='seaborn',
        showlegend=False   
    )

    return df_annualized_volatility, fig.show()


def drawdown(ticker: str, start_date: str, setor: str) -> pd.DataFrame:
    """
    Função que calcula drawdown e mostra o gráfico de barras.

    Parameters
    ----------
    ticker: str
        Ticker da empresa.
    start_date: str
        Data de início.
    ano: str
        Período escolhido.
    setor: str
        Nome do setor.

    Returns
    -------
    df_drawdown: pd.DataFrame
        DataFrame do drawdown.
    fig : plotly.graph_objects.Figure
        Gráfico de barras do drawdown.
    """
    # Df do preço do ativo
    df_preco = yf.download(ticker, start=start_date, auto_adjust=True, multi_level_index=False)['Close']

    # Calculando o retorno diário
    df_returns = df_preco.pct_change().dropna()

    # Calculando o retorno acumulado
    cumulative_returns = (1+df_returns).cumprod()

    # Calculando o pico
    peak = cumulative_returns.expanding(min_periods=1).max()

    # Calculando o drawdown
    drawdown = round(((cumulative_returns / peak) - 1) * 100, 2)

    # Criando um df
    df_drawdown = pd.DataFrame(drawdown.min(), columns=['drawdown']).sort_values(by='drawdown')

    # Plotando o drawndown
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_drawdown.index,
        y=df_drawdown['drawdown']
    ))

    fig.update_layout(
        title_text=f'Drawdown - FIIs {setor}',
        template='seaborn',
        showlegend=False   
    )

    return df_drawdown, fig.show()


def ret_anual(lst_ticker: list, start_date: str, df_benchmark: pd.DataFrame, setor: str) -> pd.DataFrame:
    """
    Calcula o retorno anual do ativo e mostra o heatmap.

    Parameters
    ----------
    lst_ticker: list
        Lista com os tickers dos ativos.
    start_date: str
        Data de início.
    df_benchmark: pd.DataFrame
        DataFrame com os preços de fechamento ('Close') do benchmark.
    setor: str
        Nome do setor.

    Returns
    -------
    df_ret_anual_setor_benchmark: pd.DataFrame
        DataFrame do retorno anual do ativo e do benchmark.
    df_ret_anual_setor: pd.DataFrame
        DataFrame do retorno anual do ativo.
    ax : matplotlib.axes.Axes
        Heatmap do retorno anual.
    """
    # Fazendo o download dos dados do setor
    df_preco_setor = yf.download(lst_ticker, start=start_date, auto_adjust=True, multi_level_index=False)['Close']
    # Selecionando os últimos preços de cada ano
    df_preco_setor_anual = df_preco_setor.groupby(df_preco_setor.index.year).last().drop(df_preco_setor.index.year.unique()[-1])
    # Calculando o retorno anual
    df_ret_anual_setor = ((df_preco_setor_anual / df_preco_setor_anual.shift(1)) - 1) * 100
    # Retirando os NaN
    df_ret_anual_setor = df_ret_anual_setor.dropna()

    # Selecionando os últimos preços de cada ano do benchmark
    df_preco_benchmark_anual = df_benchmark['Close'].groupby(df_benchmark.index.year).last().drop(df_benchmark.index.year.unique()[-1])
    # Cortando para o ano de 2021 (não cortei para 2022, porque eu quero calcular o retorno do ano de 2022)
    df_preco_benchmark_anual = df_preco_benchmark_anual.loc['2021':]
    # Calculando o retorno anual
    df_ret_anual_benchmark = ((df_preco_benchmark_anual / df_preco_benchmark_anual.shift(1)) - 1) * 100
    # Retirando os NaN
    df_ret_anual_benchmark = df_ret_anual_benchmark.dropna()

    # Definindo a figura e o eixo 
    fig, ax = plt.subplots(figsize=(20, 5))

    # Se os dfs estiverem do mesmo tamanho concatenar
    if len(df_ret_anual_setor) == len(df_ret_anual_benchmark):
        df_ret_anual_setor_benchmark = pd.concat([df_ret_anual_setor, df_ret_anual_benchmark], axis=1)
        df_ret_anual_setor_benchmark = df_ret_anual_setor.rename(columns={'Close':'IFIX'})

        # Plotando o retorno anual em um heatmap
        sns.heatmap(df_ret_anual_setor_benchmark, annot=True, cmap='Blues', fmt='.2f', linewidths=0.8, ax=ax)
        ax.set_title(f'Retorno Anual - Setor de {setor}')

        return df_ret_anual_setor_benchmark, ax

    else:
        # Plotando o retorno anual em um heatmap
        sns.heatmap(df_ret_anual_setor, annot=True, cmap='Blues', fmt='.2f', linewidths=0.8, ax=ax)
        ax.set_title(f'Retorno Anual - Setor de {setor}')

        return df_ret_anual_setor, ax
    

def ret_acumulado(lst_ticker: list, start_date: str, df_benchmark: pd.DataFrame, setor: str) -> pd.DataFrame:
    """
    Calcula o retorno acumulado do ativo e mostra o gráfico de linha.

    Parameters
    ----------
    lst_ticker: list
        Lista com os tickers dos ativos.
    start_date: str
        Data de início.
    df_benchmark: pd.DataFrame
        DataFrame com os preços de fechamento ('Close') e variação percentual ('pct_change') do benchmark.
    setor: str
        Nome do setor.

    Returns
    -------
    df_ret_acum: pd.DataFrame
        DataFrame do retorno acumulado.
    fig : plotly.graph_objects.Figure
        Gráfico de linha do retorno acumulado.
    """
    # Df do preço do ativo
    df_preco = yf.download(lst_ticker, start=start_date, auto_adjust=True, multi_level_index=False)['Close']
    # Calculando o retorno diário
    df_returns = df_preco.pct_change().dropna()
    # Calculando o retorno acumulado
    df_ret_acum = (1 + df_returns).cumprod()
    # Primeiro dia começa em 1
    df_ret_acum.loc[df_ret_acum.index[0]] = 1

    # Selecionando o período do benchmark e criando uma cópia explícita
    benchmark_precos_sliced = df_benchmark.loc[df_ret_acum.index[0]:, ['Close', 'pct_change']].copy()
    # Calculando a variação percentual acumulada
    benchmark_precos_sliced['pct_change_accum'] = round((1 + (benchmark_precos_sliced['pct_change'] / 100)).cumprod(), 4)
    # Ajustando o primeiro dia do benchmark
    benchmark_precos_sliced.loc[benchmark_precos_sliced.index[0], 'pct_change_accum'] = 1

    # Plotando o retorno acumulado
    fig = go.Figure()

    for empresa in df_ret_acum:
        fig.add_trace(go.Scatter(
            x=df_ret_acum.index,
            y=df_ret_acum[empresa],
            name=empresa
        ))

    fig.add_trace(go.Scatter(
        x=benchmark_precos_sliced.index,
        y=benchmark_precos_sliced['pct_change_accum'],
        name='IFIX'
    ))

    fig.update_layout(
        height=800,
        title_text=f'Retorno Acumulado - Setor de {setor}',
        template='seaborn'
    )

    fig.add_hline(y=1, line_width=1, line_color='red')

    return df_ret_acum, fig.show()


def plot_risk_return(ticker: str, setor: str):
    """
    Gráfico da relação risco x retorno do setor selecionado.

    Parameters
    ----------
    df_setor: pd.DataFrame
        DataFrame que contém os preços de fechamento das empresas do setor selecionado.
    setor: str
        Nome do setor.
    
    Returns
    -------
    fig : plotly.graph_objects.Figure
        Gráfico da relação risco x retorno do setor selecionado.
    """
    # Df do preço do ativo
    df_preco = yf.download(ticker, auto_adjust=True)['Close']
    # Resetando o index do df
    df_preco = df_preco.reset_index()
    # Transformando a coluna 'Date' em  index 
    df_preco = df_preco.set_index('Date')

    # Calculando o retorno diário
    df_returns = np.log(df_preco / df_preco.shift(1))

    # Listas da média do retorno logarítmico e do desvio-padrão do retorno logarítmico
    lst_ret_mean = []
    lst_ret_std = []

    for empresa in df_returns:
        # Média do retorno logarítmico
        ret_mean = df_returns[empresa].dropna().mean() * 100
        # Desvio-padrão do retorno logarítmico médio
        ret_std = df_returns[empresa].dropna().std() * 100
        lst_ret_mean.append(ret_mean)
        lst_ret_std.append(ret_std)

    # Dataframe da relação risco x retorno
    df_risk_return = pd.DataFrame([lst_ret_mean, lst_ret_std], columns=df_returns.columns, index=['mean', 'std'])

    # Plotando o grafico da relação risco x retorno
    fig = go.Figure()

    for empresa in df_risk_return: 
        fig.add_trace(go.Scatter(
            x=[df_risk_return.loc['mean', empresa]],
            y=[df_risk_return.loc['std', empresa]],
            mode='markers',
            marker=dict(symbol='star', size=10),
            name=empresa
        ))

    # Atualizando o layout
    fig.update_layout(
        title=f'Setor de {setor} - Gráfico risco x retorno',
        xaxis=dict(title='Média Esperada Retorno Diário'),
        yaxis=dict(title='Risco Diário'),
        showlegend=True
    )

    fig.add_hline(y=0, line_width=2, line_color='red')
    fig.add_vline(x=0, line_width=2, line_color='red')

    return fig.show()


def dy_fii(df: pd.DataFrame, setor: str) -> pd.DataFrame:
    """
    Calcula o DY médio, máximo e mínimo e mostra o gráfico de barra.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame do FII que contém a coluna 'Percentual_Dividend_Yield_Mes'.
    setor: str
        Nome do setor.

    Returns
    -------
    df_dy: pd.DataFrame
        DataFrame que contém os DY médio, máximo e mínimo.
    fig : plotly.graph_objects.Figure
        Gráfico de barras do DY médio, máximo e mínimo.
    """
    # Calculando o DY médio, máximo e mínimo
    lst_dy = []

    for empresa in df['Ticker'].unique():
        dy_medio = round(df.loc[df['Ticker'] == empresa, 'Percentual_Dividend_Yield_Mes'].mean(), 2)
        dy_max = round(df.loc[df['Ticker'] == empresa, 'Percentual_Dividend_Yield_Mes'].max(), 2)
        dy_min = round(df.loc[df['Ticker'] == empresa, 'Percentual_Dividend_Yield_Mes'].min(), 2)
        lst_dy.append({
            'dy_medio': dy_medio, 
            'dy_max': dy_max, 
            'dy_min': dy_min
        })

    # Transformando a lista em um df
    df_dy = pd.DataFrame(lst_dy)

    # Renomeando as colunas
    df_dy.index = df['Ticker'].unique()

    # Plotando o dividend yield médio, máximo e mínimo
    fig = make_subplots(
        rows=3, 
        cols=1,
        subplot_titles=[
            'Dividend Yield Médio',
            'Dividend Yield Máximo',
            'Dividend Yield Mínimo'
        ],
        vertical_spacing=0.1
        )

    fig.add_trace(go.Bar(
        x=df_dy.index,
        y=df_dy['dy_medio'],
        name='DY médio'
    ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=df_dy.index,
        y=df_dy['dy_max'],
        name='DY máximo'
    ), row=2, col=1)

    fig.add_trace(go.Bar(
        x=df_dy.index,
        y=df_dy['dy_min'],
        name='DY mínimo'
    ), row=3, col=1)

    fig.update_layout(
        title=f'Dividend yield dos FII de {setor}',
        height=1200)

    return df_dy, fig.show()


def dy_fii_acumm_12m(df: pd.DataFrame, setor: str) -> pd.Series:
    """
    Calcula o dividend yield acumulado dos últimos 12 meses e mostra o gráfico de linha.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame do FII que contém a coluna 'Percentual_Dividend_Yield_Mes'.
    setor: str
        Nome do setor.

    Returns
    -------
    df_dy_acum: pd.DataFrame
        DataFrame do dividend yield acumulado dos últimos 12 meses.
    fig : plotly.graph_objects.Figure
        Gráfico de linha do dividend yield acumulado dos últimos 12 meses.
    """
    # DY acumulado dos últimos 12 meses
    lst_dy_acum = []
    for empresa in df['Ticker'].unique():
        # Calculando a taxa unitária
        taxa_unitaria = 1 + (df.loc[df['Ticker'] == empresa, 'Percentual_Dividend_Yield_Mes'] / 100)
        # Calculando o DY acumulado dos últimos 12 meses
        dy_acumulado = round((taxa_unitaria.rolling(window=12).agg(lambda x: x.prod()) - 1) * 100, 2)
        # Retirando os NaN
        dy_acumulado = dy_acumulado.dropna()
        # Adicionando na lista
        lst_dy_acum.append(dy_acumulado)

    # Criando o df
    df_dy_acum = pd.DataFrame(lst_dy_acum).T

    # Renomeando as colunas
    df_dy_acum.columns = df['Ticker'].unique()

    # Plotando o DY acumulados dos últimos 12 meses
    fig = go.Figure()

    for empresa in df_dy_acum.columns:
        fig.add_trace(go.Scatter(
            x=df_dy_acum.index,
            y=df_dy_acum[empresa],
            name=empresa
        ))

    fig.update_layout(title=f'Dividend yield acumulados dos últmos 12 meses - FIIs de {setor}')

    return df_dy_acum, fig.show()


def consulta_bc(codigo_bcb: str, data_inicial: str, data_final: str) -> pd.DataFrame:
    """
    DataFrame do item desejado que está na API do Banco Central.

    Parameters
    ----------
    codigo_bcb: str
        Código da série do BC.
    data_inicial: str
        Data inicial da série - no formato dia/mes/ano -> 01/01/2024.
    data_final: str
        Data final da série - no formato dia/mes/ano -> 01/01/2024.

    Returns
    -------
    df: pd.DataFrame
        DataFrame da série do BC.
    """
    url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_bcb}/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}'
    df = pd.read_json(url)
    df['data'] = pd.to_datetime(df['data'], dayfirst=True)
    df.set_index('data', inplace= True)

    return df


def vm_igti(ano: str, mes: str, start_date: str, num_on: int, num_pn: int) -> float:
    """
    Calcula o valor de mercado da empresa Iguatemi (IGTI3 e IGTI4).

    Parameters
    ----------
    ano: str
        Ano do período do ITR/DFP.
    mes: str
        Mês do período do ITR/DFP.
    start_date: str
        Data de início.
    num_on: int
        Número de ações ordinárias do período do ITR/DFP.
    num_pn: int
        Número de ações preferenciais do período do ITR/DFP.

    Returns
    -------
    igti_vm: float
        Valor de mercado do Iguatemi.
    """
    # Selecioando os últimos preços do IGTI
    # ITR 1T -> mes: '03'
    # ITR 2T -> mes: '06'
    # ITR 3T -> mes: '09'
    # DFP 4T -> mes: '12'

    # Lista com os tickers ON e PN da Iguatemi
    lst_iguatemi = ['IGTI3.SA', 'IGTI4.SA']
    # Df do preço do ativo
    preco_iguatemi = yf.download(lst_iguatemi, start=start_date, auto_adjust=True, multi_level_index=False)['Close']
    # Selecionando pelo ano e mês
    preco_iguatemi = preco_iguatemi.loc[f'{ano}-{mes}']
    # Selecionando o último dado
    preco_iguatemi = preco_iguatemi.iloc[-1]

    # Número de ações do IGTI
    dict_num_acoes_igti = {
        'on': num_on,
        'pn': num_pn
    }

    # Valor de mercado do IGTI
    igti3_vm = preco_iguatemi['IGTI3.SA'] * dict_num_acoes_igti['on']
    igti4_vm = preco_iguatemi['IGTI4.SA'] * dict_num_acoes_igti['pn']
    igti_vm = igti3_vm + igti4_vm

    return igti_vm


def indicadores_acoes_shoppings(
    ano: str, 
    mes: str, 
    start_date: str, 
    dict_shoppings: dict, 
    ticker: str, 
    vm_igti
) -> pd.DataFrame:
    """
    Parameters
    ----------
    ano: str
        Ano do período do ITR/DFP.
    mes: str
        Mês do período do ITR/DFP.
    start_date: str
        Data de início.
    dict_shoppings: dict
        Dicionário que contém os dados das empresas de shoppings.
    ticker: str
        Tickers das empreas de shoppings.
    vm_igti: callable
        Função que calcula o valor de mercado da Iguatemi.

    Returns
    -------
    df_acoes_shoppings: pd.DataFrame
        DataFrame que contém o indicador P/VP das empresas de shoppings.
    """
    # Selecionando os últimos preços das ações de shoppings
    # ITR 1T -> mes: '03'
    # ITR 2T -> mes: '06'
    # ITR 3T -> mes: '09'
    # DFP 4T -> mes: '12'

    # Df do preço do ativo
    preco_shopping = yf.download(ticker, start=start_date, auto_adjust=True, multi_level_index=False)['Close']
    # Selecionando pelo ano e mês
    preco_shopping = preco_shopping.loc[f'{ano}-{mes}']
    # Selecionando o último dado
    ultimo_preco_shopping = preco_shopping.iloc[-1]
    # Renomeando a coluna para 'preco'
    ultimo_preco_shopping = ultimo_preco_shopping.rename('preco')

    # Transformando em um df
    df_acoes_shoppings = pd.DataFrame(dict_shoppings, index=ticker)

    # Concatenando os dfs
    df_acoes_shoppings = pd.concat([df_acoes_shoppings, ultimo_preco_shopping], axis=1)

    # Calculando a diferença entre o 'valor_justo' e 'propriedades_investimentos'
    df_acoes_shoppings['diferenca'] = df_acoes_shoppings['valor_justo'] - df_acoes_shoppings['propriedades_investimento']
    # Calculando o 'pl_ajustado'
    df_acoes_shoppings['pl_ajustado'] = df_acoes_shoppings['pl_contabil'] + df_acoes_shoppings['diferenca']
    # Calculando o 'vpa'
    df_acoes_shoppings['vpa'] = round(df_acoes_shoppings['pl_ajustado'] / df_acoes_shoppings['num_acoes'], 2)
    # Calculando o 'p/vp'
    df_acoes_shoppings['p/vp'] = round(df_acoes_shoppings['preco'] / df_acoes_shoppings['vpa'], 2)
    # A unit da Iguatemi é formada por 3 ações (1 Unit = 1 ação ON + 2 ações PN)
    df_acoes_shoppings.loc['IGTI11.SA', 'p/vp'] = df_acoes_shoppings.loc['IGTI11.SA', 'p/vp']/3
    # Calculando o 'valor_mercado'
    df_acoes_shoppings['valor_mercado'] = df_acoes_shoppings['num_acoes'] * df_acoes_shoppings['preco']
    # Calculando o 'valor_mercado' do IGTI com os valores de ON e PN
    vm_igti_2T24 = vm_igti
    # Substituindo o 'valor_mercado' do IGTI11 pelo correto
    df_acoes_shoppings.loc['IGTI11.SA', 'valor_mercado'] = vm_igti_2T24
    # Calculando o 'enterprise_value'
    df_acoes_shoppings['enterprise_value'] = df_acoes_shoppings['valor_mercado'] + df_acoes_shoppings['divida_liquida'] 
    # Calculando 'ev/abl' (EV/m²)
    df_acoes_shoppings['ev/abl'] =  round(df_acoes_shoppings['enterprise_value'] / df_acoes_shoppings['abl_propria'], 2)

    return df_acoes_shoppings


def valor_absoluto_grafico_pizza(val: float, contagem: pd.Series) -> str:
    """
    Mostra os valores absolutos de uma contagem específica para ser plotado no gráfico de pizza.

    Parameters
    ----------
    val: float
        Valor percentual a ser convertido em um valor absoluto.
    contagem: pd.Series
        Série de contagem que será usada como base para o cálculo.

    Returns
    -------
    str: 
        Valor absoluto como uma string, adequado para ser exibido em gráficos.
    """
    a = int(val / 100. * contagem.sum())
    return f'{a}'
