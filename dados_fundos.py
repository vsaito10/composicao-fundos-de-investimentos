"""
Leitura (duckdb) e cálculos dos dados dos fundos de investimentos usados no app Streamlit ('app_fundos.py').
"""
import re

import duckdb
import numpy as np
import pandas as pd

PATH_CDA = 'C:/B3/historico-arquivos/fundos-b3/fundos_cvm'
PATH_INF_DIARIO = 'C:/B3/historico-arquivos/fundos-b3/inf_diario_cvm'
PATH_COTAHIST = 'C:/B3/historico-arquivos/cotacoes-historicas-b3/cotahist-parquet'
PATH_CDI = 'C:/B3/historico-arquivos/bcb/cdi_sgs4389.parquet'

FUNDOS = {
    'Verde': '16.929.553/0001-63',
    'Dynamo': '37.916.879/0001-26',
    'IP': '11.435.298/0001-89',
    'Squadra': '09.412.648/0001-40',
    'Guepardo': '14.213.077/0001-54',
}

# Agrupamento dos 'TP_APLIC' do CDA em classes e sinal do valor (posições passivas entram negativas)
CLASSES = {
    'Ações': ('Ações (comprado)', 1),
    'Ações e outros TVM cedidos em empréstimo': ('Ações (comprado)', 1),
    'Certificado ou recibo de depósito de valores mobiliários': ('Ações (comprado)', 1),
    'Brazilian Depository Receipt - BDR': ('Ações (comprado)', 1),
    'Outros valores mobiliários registrados na CVM objeto de oferta pública': ('Ações (comprado)', 1),
    'Obrigações por ações e outros TVM recebidos em empréstimo': ('Ações (vendido)', -1),
    'Opções - Posições titulares': ('Opções (titular)', 1),
    'Opções - Posições lançadas': ('Opções (lançada)', -1),
    'Mercado Futuro - Posições compradas': ('Mercado futuro', 1),
    'Mercado Futuro - Posições vendidas': ('Mercado futuro', -1),
    'Títulos Públicos': ('Renda fixa', 1),
    'Operações Compromissadas': ('Renda fixa', 1),
    'Debêntures': ('Renda fixa', 1),
    'Cotas de Fundos': ('Cotas de fundos', 1),
    'Investimento no Exterior': ('Exterior', 1),
    'Disponibilidades': ('Caixa e outros', 1),
    'Valores a receber': ('Caixa e outros', 1),
    'Outras aplicações': ('Caixa e outros', 1),
    'Valores a pagar': ('Valores a pagar', -1),
    'Outras operações passivas e exigibilidades': ('Valores a pagar', -1),
    'Obrigações por compra a termo a pagar': ('Valores a pagar', -1),
}

# Bloco 7 (exterior): o TP_ATIVO é inconsistente (muitas ações vêm como 'Outros'), então além dos tipos que
# claramente não são ações, exclui os fundos, caixa e títulos encontrados nas carteiras dos 5 fundos
TP_ATIVO_EXTERIOR_NAO_ACOES = ['Fundos Offshore', 'Opção de compra', 'Opção de venda', 'SWAP']
ATIVOS_EXTERIOR_NAO_ACOES = {
    'FUNDO OFF', 'SQ BIASE',                           # Squadra: fundo offshore
    'IPPARTIN', 'IPPARTOF', 'USTI', 'USDXPCAS', '99',  # IP: fundos offshore e caixa
    '82',                                              # Verde: VA Fund Ltd
    'DYCMFUND', '12301398', '8587404', '10588488',     # Dynamo: fundo, Treasury Bill e bonds da B2W
}


def _lista_sql(valores) -> str:
    return "('" + "', '".join(valores) + "')"


def carregar_cotas(cnpjs: list[str]) -> pd.DataFrame:
    """Cota, PL, captação e resgate diários (arquivos 'inf_diario_fi')."""
    df = duckdb.sql(f"""
        select coalesce(CNPJ_FUNDO_CLASSE, CNPJ_FUNDO) as cnpj, cast(DT_COMPTC as date) as data,
               max(VL_QUOTA) as cota, max(VL_PATRIM_LIQ) as pl,
               max(CAPTC_DIA) as captacao, max(RESG_DIA) as resgate, max(NR_COTST) as cotistas
        from read_parquet('{PATH_INF_DIARIO}/*.parquet', union_by_name=true)
        where coalesce(CNPJ_FUNDO_CLASSE, CNPJ_FUNDO) in {_lista_sql(cnpjs)}
        group by all
        order by cnpj, data
    """).df()
    df['data'] = pd.to_datetime(df['data'])
    return df


def carregar_benchmarks() -> pd.DataFrame:
    """Índices diários (base 1) do BOVA11 (proxy do Ibovespa) e do CDI."""
    bova = duckdb.sql(f"""
        select cast(data_pregao as date) as data, fechamento
        from read_parquet('{PATH_COTAHIST}/cotahist_20*.parquet')
        where codneg = 'BOVA11' and tipo_mercado = 10
        qualify row_number() over (partition by data_pregao order by volume_financeiro desc) = 1
        order by data
    """).df().set_index('data')['fechamento']
    cdi = duckdb.sql(f"select data, cdi_anual from '{PATH_CDI}' order by data").df().set_index('data')['cdi_anual']

    df = pd.DataFrame({'BOVA11': bova})
    df.index = pd.to_datetime(df.index)
    cdi.index = pd.to_datetime(cdi.index)
    # O CDI do dia 't' remunera de 't-1' até 't'
    cdi_diario = ((1 + cdi / 100) ** (1 / 252) - 1).reindex(df.index).ffill()
    df['CDI'] = (1 + cdi_diario.shift(1).fillna(0)).cumprod()
    return df.dropna()


def carregar_carteiras(cnpjs: list[str]) -> pd.DataFrame:
    """Todas as posições (blocos 1, 2, 4, 7 e 8 do CDA) com classe, ativo e % do PL."""
    df = duckdb.sql(f"""
        with posicoes as (
            select coalesce(CNPJ_FUNDO_CLASSE, CNPJ_FUNDO) as cnpj,
                   cast(DT_COMPTC as date) as data,
                   regexp_extract(filename, 'BLC_(\\d)', 1) as bloco,
                   TP_APLIC as tp_aplic,
                   case
                       when TP_APLIC = 'Operações Compromissadas' then 'Compromissada ' || coalesce(TP_TITPUB, '')
                       when TP_APLIC = 'Títulos Públicos' then TP_TITPUB || ' ' || coalesce(cast(DT_VENC as varchar), '')
                       else coalesce(CD_ATIVO, CD_ATIVO_BV_MERC, DS_ATIVO_EXTERIOR, NM_FUNDO_CLASSE_SUBCLASSE_COTA,
                                     NM_FUNDO_COTA, DS_ATIVO, EMISSOR, TP_APLIC)
                   end as ativo,
                   coalesce(DS_ATIVO, DS_ATIVO_EXTERIOR, EMISSOR, NM_FUNDO_CLASSE_SUBCLASSE_COTA, NM_FUNDO_COTA, TP_TITPUB) as descricao,
                   DT_CONFID_APLIC is not null as confidencial,
                   TP_ATIVO as tp_ativo,
                   VL_MERC_POS_FINAL as valor,
                   coalesce(VL_AQUIS_NEGOC, 0) as valor_compras,
                   coalesce(VL_VENDA_NEGOC, 0) as valor_vendas
            from read_parquet('{PATH_CDA}/*/cda_fi_BLC_[12478]_*.parquet', union_by_name=true, filename=true)
            where coalesce(CNPJ_FUNDO_CLASSE, CNPJ_FUNDO) in {_lista_sql(cnpjs)}
        ),
        pl as (
            select coalesce(CNPJ_FUNDO_CLASSE, CNPJ_FUNDO) as cnpj, cast(DT_COMPTC as date) as data, max(VL_PATRIM_LIQ) as pl
            from read_parquet('{PATH_CDA}/*/cda_fi_PL_*.parquet', union_by_name=true)
            where coalesce(CNPJ_FUNDO_CLASSE, CNPJ_FUNDO) in {_lista_sql(cnpjs)}
            group by all
        )
        select posicoes.*, pl.pl
        from posicoes join pl using (cnpj, data)
    """).df()

    df['data'] = pd.to_datetime(df['data'])
    # No bloco 8 o ativo vem como 'TIPO/TICKER/ISIN' (ex.: 'PAT/BPAC11/BRBPACUNT006')
    ticker_blc_8 = df['ativo'].str.extract(r'^[^/]+/([^/]+)/')[0]
    df['ativo'] = ticker_blc_8.where(df['bloco'].eq('8') & ticker_blc_8.notna(), df['ativo']).str.strip()
    df['classe'] = df['tp_aplic'].map(lambda x: CLASSES.get(x, ('Outros', 1))[0])
    df['valor'] = df['valor'] * df['tp_aplic'].map(lambda x: CLASSES.get(x, ('Outros', 1))[1])
    df['peso'] = df['valor'] / df['pl']
    df['acao_exterior'] = (
        df['classe'].eq('Exterior')
        & ~df['tp_ativo'].isin(TP_ATIVO_EXTERIOR_NAO_ACOES)
        & ~df['ativo'].isin(ATIVOS_EXTERIOR_NAO_ACOES)
    )
    return df


def nome_acao_exterior(df_carteira: pd.DataFrame) -> pd.Series:
    """Identificador das ações no exterior. Quando o código é só um número (ex.: Verde usa '1' para várias ações),
    usa a descrição sem a quantidade no final ('XP INC - CLASS A - 427' -> 'XP INC - CLASS A')."""
    nome_descricao = (
        df_carteira['descricao'].fillna('').str.replace(r'\s*-\s*\d+$', '', regex=True)
        .str.replace(r'^[A-Z]+ US - ', '', regex=True).str.strip()
    )
    return df_carteira['ativo'].where(~df_carteira['ativo'].str.fullmatch(r'\d+'), nome_descricao)


def posicoes_por_ativo(df_carteira: pd.DataFrame) -> pd.DataFrame:
    """Agrega as posições por (data, classe, ativo) - ex.: ação em carteira + ação cedida em empréstimo."""
    return (
        df_carteira.groupby(['data', 'classe', 'ativo'], as_index=False)
        .agg(descricao=('descricao', 'first'), valor=('valor', 'sum'), peso=('peso', 'sum'),
             valor_compras=('valor_compras', 'sum'), valor_vendas=('valor_vendas', 'sum'),
             confidencial=('confidencial', 'any'), acao_exterior=('acao_exterior', 'any'))
    )


def retornos_mensais(serie: pd.Series) -> pd.Series:
    """Retorno mensal: última cota do mês vs última cota do mês anterior (padrão MaisRetorno)."""
    fim_mes = serie.groupby(serie.index.to_period('M')).last()
    return fim_mes.pct_change().dropna()


def tabela_rentabilidade(cota: pd.Series, benchmarks: pd.DataFrame) -> pd.DataFrame:
    """Tabela ano x mês com a rentabilidade do fundo e o acumulado do ano do fundo, BOVA11 e CDI.

    O primeiro mês conta a partir da primeira cota da série, para o acumulado fechar com o retorno do período.
    """
    def mensais_desde_inicio(serie: pd.Series) -> pd.Series:
        fim_mes = serie.groupby(serie.index.to_period('M')).last()
        return fim_mes / fim_mes.shift(1, fill_value=serie.iloc[0]) - 1

    ret = mensais_desde_inicio(cota)
    tabela = pd.DataFrame({'ano': ret.index.year, 'mes': ret.index.month, 'ret': ret.values})
    tabela = tabela.pivot(index='ano', columns='mes', values='ret')
    meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    tabela.columns = [meses[m - 1] for m in tabela.columns]
    tabela['Ano'] = (1 + ret).groupby(ret.index.year).prod() - 1
    # Retorno composto desde o início do período até o último mês de cada ano
    tabela['Acumulado'] = (1 + tabela['Ano']).cumprod() - 1

    for col in ['BOVA11', 'CDI']:
        ret_bench = mensais_desde_inicio(benchmarks[col].loc[cota.index.min():cota.index.max()].dropna())
        tabela[f'{col} no ano'] = (1 + ret_bench).groupby(ret_bench.index.year).prod() - 1
    return tabela


def metricas(cota: pd.Series, benchmarks: pd.DataFrame) -> dict:
    """Métricas de risco e retorno do período da série 'cota'."""
    ret = cota.pct_change().dropna()
    bench = benchmarks.reindex(cota.index).ffill()
    ret_bova = bench['BOVA11'].pct_change().dropna()
    _ret_cdi = bench['CDI'].pct_change().dropna()
    anos = len(ret) / 252

    retorno_total = cota.iloc[-1] / cota.iloc[0] - 1
    retorno_anual = (1 + retorno_total) ** (1 / anos) - 1 if anos > 0 else np.nan
    cdi_anual = (bench['CDI'].iloc[-1] / bench['CDI'].iloc[0]) ** (1 / anos) - 1 if anos > 0 else np.nan
    vol = ret.std() * np.sqrt(252)
    drawdown = cota / cota.cummax() - 1
    beta = ret.cov(ret_bova) / ret_bova.var()

    return {
        'Retorno no período': retorno_total,
        'Retorno anualizado': retorno_anual,
        'BOVA11 no período': bench['BOVA11'].iloc[-1] / bench['BOVA11'].iloc[0] - 1,
        'CDI no período': bench['CDI'].iloc[-1] / bench['CDI'].iloc[0] - 1,
        'Volatilidade anual': vol,
        'Sharpe': (retorno_anual - cdi_anual) / vol,
        'Drawdown máximo': drawdown.min(),
        'Beta vs BOVA11': beta,
        'Correlação vs BOVA11': ret.corr(ret_bova),
        '% meses acima do CDI': (retornos_mensais(cota) > retornos_mensais(bench['CDI'])).mean(),
        '% meses abaixo do CDI': (retornos_mensais(cota) < retornos_mensais(bench['CDI'])).mean(),
    }


def drawdown(cota: pd.Series) -> pd.Series:
    return cota / cota.cummax() - 1


JANELAS = ['No mês', 'No ano', '12 meses', '24 meses', '36 meses', 'Desde o início']


def retornos_janelas(serie: pd.Series, data_ref: pd.Timestamp) -> pd.Series:
    """Retorno em janelas padrão até 'data_ref'. NaN quando a série não cobre a janela inteira."""
    serie = serie.dropna().loc[:data_ref]
    if len(serie) < 2:
        return pd.Series(np.nan, index=JANELAS)
    data_ref = serie.index[-1]
    inicios = {
        'No mês': data_ref.to_period('M').start_time - pd.Timedelta(days=1),
        'No ano': data_ref.to_period('Y').start_time - pd.Timedelta(days=1),
        '12 meses': data_ref - pd.DateOffset(months=12),
        '24 meses': data_ref - pd.DateOffset(months=24),
        '36 meses': data_ref - pd.DateOffset(months=36),
    }
    retornos = {}
    for janela, inicio in inicios.items():
        base = serie.loc[:inicio]
        # A base é a última cota até o início da janela; sem cota antes dele, a janela não está completa
        retornos[janela] = serie.iloc[-1] / base.iloc[-1] - 1 if len(base) else np.nan
    retornos['Desde o início'] = serie.iloc[-1] / serie.iloc[0] - 1
    return pd.Series(retornos)[JANELAS]


def metricas_benchmark(cota: pd.Series, benchmarks: pd.DataFrame, benchmark: str = 'BOVA11') -> dict:
    """Métricas relativas ao benchmark e de risco de cauda no período da série 'cota'."""
    bench = benchmarks.reindex(cota.index).ffill()
    ret = cota.pct_change().dropna()
    ret_bench = bench[benchmark].pct_change().reindex(ret.index)
    ret_cdi = bench['CDI'].pct_change().reindex(ret.index)

    # Alfa de Jensen: regressão dos excessos de retorno sobre o CDI (diário, anualizado)
    exc, exc_bench = ret - ret_cdi, ret_bench - ret_cdi
    beta = exc.cov(exc_bench) / exc_bench.var()
    alfa = (exc.mean() - beta * exc_bench.mean()) * 252

    ativo = ret - ret_bench
    tracking_error = ativo.std() * np.sqrt(252)

    # Captura de alta e de baixa com retornos mensais (média geométrica do fundo / do benchmark)
    mensal = pd.DataFrame({'fundo': retornos_mensais(cota), 'bench': retornos_mensais(bench[benchmark])}).dropna()

    def captura(meses: pd.DataFrame) -> float:
        if meses.empty:
            return np.nan
        geo = lambda r: (1 + r).prod() ** (1 / len(r)) - 1
        return geo(meses['fundo']) / geo(meses['bench'])

    anos = len(ret) / 252
    retorno_anual = (cota.iloc[-1] / cota.iloc[0]) ** (1 / anos) - 1 if anos > 0 else np.nan
    cdi_anual = (bench['CDI'].iloc[-1] / bench['CDI'].iloc[0]) ** (1 / anos) - 1 if anos > 0 else np.nan
    desvio_negativo = np.sqrt((exc.clip(upper=0) ** 2).mean()) * np.sqrt(252)
    var_95 = ret.quantile(0.05)
    ret_mensal = retornos_mensais(cota)

    return {
        'Alfa anual': alfa,
        'Tracking error': tracking_error,
        'Information ratio': ativo.mean() * 252 / tracking_error,
        'Captura de alta': captura(mensal[mensal['bench'] > 0]),
        'Captura de baixa': captura(mensal[mensal['bench'] < 0]),
        'Sortino': (retorno_anual - cdi_anual) / desvio_negativo,
        'Pior mês': ret_mensal.min() if len(ret_mensal) else np.nan,
        'Melhor mês': ret_mensal.max() if len(ret_mensal) else np.nan,
        'VaR 95% (1 dia)': var_95,
        'CVaR 95% (1 dia)': ret[ret <= var_95].mean(),
    }


def janelas_moveis(cota: pd.Series, bench: pd.Series, pregoes: int = 252) -> pd.DataFrame:
    """Retorno e volatilidade em janelas móveis (padrão: 12 meses ~ 252 pregões) do fundo e do benchmark."""
    bench = bench.reindex(cota.index).ffill()
    return pd.DataFrame({
        'Retorno fundo': cota.pct_change(pregoes),
        'Retorno benchmark': bench.pct_change(pregoes),
        'Volatilidade fundo': cota.pct_change().rolling(pregoes).std() * np.sqrt(252),
        'Volatilidade benchmark': bench.pct_change().rolling(pregoes).std() * np.sqrt(252),
    }).dropna()


def beta_rolante(cota: pd.Series, bova: pd.Series, janela: int = 63) -> pd.Series:
    ret = cota.pct_change()
    ret_bova = bova.reindex(cota.index).ffill().pct_change()
    return (ret.rolling(janela).cov(ret_bova) / ret_bova.rolling(janela).var()).dropna()


# Trocas de código conhecidas (antigo -> novo): renomeações, incorporações e conversões de classe
TICKERS_RENOMEADOS = {
    'ELET3': 'AXIA3', 'ELET6': 'AXIA6', 'CCRO3': 'MOTV3', 'NTCO3': 'NATU3', 'SOMA3': 'AZZA3', 'ARZZ3': 'AZZA3',
    'BRML3': 'ALOS3', 'ALSO3': 'ALOS3', 'CPLE6': 'CPLE5', 'CPLE5': 'CPLE3',
}
RE_TICKER = r'[A-Z]{4}\d{1,2}'
# Direitos (1, 2) e recibos de subscrição (9, 10, 12): viram a ação com o mesmo prefixo (ex.: RENT1 -> RENT9 -> RENT3)
RE_DIREITO_RECIBO = r'[A-Z]{4}(1|2|9|10|12)'
# Uma Saída que volta em até N carteiras pode ter sido posição omitida (ex.: confidencial), não venda
CARTEIRAS_VOLTA = 3


def movimentacoes(df_pos: pd.DataFrame, data_atual: pd.Timestamp, data_anterior: pd.Timestamp) -> pd.DataFrame:
    """Compara as posições (por ativo) entre duas carteiras e classifica a movimentação."""
    atual = df_pos[df_pos['data'] == data_atual].set_index(['classe', 'ativo'])
    anterior = df_pos[df_pos['data'] == data_anterior].set_index(['classe', 'ativo'])

    df = pd.DataFrame({
        'peso_anterior': anterior['peso'],
        'peso_atual': atual['peso'],
    }).fillna(0)
    df['variacao_peso'] = df['peso_atual'] - df['peso_anterior']
    df['compras_no_mes'] = atual['valor_compras'].reindex(df.index).fillna(0)
    df['vendas_no_mes'] = atual['valor_vendas'].reindex(df.index).fillna(0)

    df['movimento'] = np.select(
        [(df['peso_anterior'] == 0) & (df['peso_atual'] != 0),
         (df['peso_anterior'] != 0) & (df['peso_atual'] == 0),
         df['variacao_peso'] > 0.0025,
         df['variacao_peso'] < -0.0025],
        ['Entrada', 'Saída', 'Aumento', 'Redução'],
        default='Sem mudança relevante',
    )
    df = df.reset_index()
    df['observacao'] = ''

    # Saída + Entrada da mesma empresa = troca de código (incorporação, renomeação, direito de subscrição)
    saidas = df[df['movimento'] == 'Saída']
    entradas = df[df['movimento'] == 'Entrada']
    for i_saida, saida in saidas.iterrows():
        mesma_classe = entradas[entradas['classe'] == saida['classe']]
        novo = TICKERS_RENOMEADOS.get(saida['ativo'])
        if novo in mesma_classe['ativo'].values:
            i_entrada = mesma_classe.index[mesma_classe['ativo'] == novo][0]
        elif re.fullmatch(RE_DIREITO_RECIBO, saida['ativo']):
            # Só quando sai o direito/recibo; PETR3 -> PETR4 ou RENT3 -> RENT1 são trocas de posição, não de código
            candidatos = mesma_classe[mesma_classe['ativo'].str.fullmatch(RE_TICKER)
                                      & (mesma_classe['ativo'].str[:4] == saida['ativo'][:4])]
            if candidatos.empty:
                continue
            i_entrada = candidatos.index[0]
        else:
            continue
        df.loc[[i_saida, i_entrada], 'movimento'] = 'Troca de código'
        df.loc[i_saida, 'observacao'] = f"Virou {df.loc[i_entrada, 'ativo']}"
        df.loc[i_entrada, 'observacao'] = f"Era {saida['ativo']}"
        entradas = entradas.drop(i_entrada)

    # Saídas que voltam logo depois podem ter sido posições omitidas (ex.: confidenciais), não vendas
    proximas = sorted(d for d in df_pos['data'].unique() if d > data_atual)[:CARTEIRAS_VOLTA]
    posteriores = df_pos[df_pos['data'].isin(proximas) & (df_pos['peso'] != 0)]
    volta_em = posteriores.groupby(['classe', 'ativo'])['data'].min()
    for i, linha in df[df['movimento'] == 'Saída'].iterrows():
        data_volta = volta_em.get((linha['classe'], linha['ativo']))
        if data_volta is not None:
            df.loc[i, 'observacao'] = f'Volta à carteira em {data_volta:%m/%Y}'
    return df.sort_values('variacao_peso', key=abs, ascending=False)


def peso_total(df_carteira: pd.DataFrame) -> pd.Series:
    """Soma dos pesos de cada carteira. Bem abaixo de 100% indica posições omitidas (ex.: confidenciais)."""
    return df_carteira.groupby('data')['peso'].sum()


# ---------------------------------------------------------------------------------------------------------------------
# Atribuição de desempenho
# ---------------------------------------------------------------------------------------------------------------------
# ETFs que aparecem no bloco 'Cotas de Fundos' e o ticker usado para precificá-los
ETFS_COTAS = {
    'ISHARES IBOVESPA': 'BOVA11',
    'IT NOW IBOVESPA': 'BOVV11',
    'ISHARES BM&FBOVESPA SMALL CAP': 'SMAL11',
    'IT NOW SMALL CAPS': 'SMAC11',
}
# Cotas de fundos de renda fixa/liquidez (rendem, aproximadamente, o CDI)
REGEX_COTAS_CDI = r'RENDA FIXA|REFERENCIADO|LIQUIDEZ|\bRF\b|SOBERANO'


def carregar_retornos_diarios(tickers: list[str]) -> pd.DataFrame:
    """
    Retornos diários (mercado à vista) a partir do COTAHIST.
    O COTAHIST não é ajustado por proventos: desdobramentos/grupamentos (mudança no número de distribuição com
    variação > 15%) têm o retorno zerado e os dividendos/JCP ficam de fora.
    """
    df = duckdb.sql(f"""
        select cast(data_pregao as date) as data, codneg as ticker, fechamento, num_distribuicao
        from read_parquet('{PATH_COTAHIST}/cotahist_20*.parquet')
        where tipo_mercado = 10 and codneg in {_lista_sql(tickers)}
        qualify row_number() over (partition by codneg, data_pregao order by volume_financeiro desc) = 1
    """).df()
    df['data'] = pd.to_datetime(df['data'])

    # Ações sem negócio no dia repetem o último fechamento (até 5 pregões)
    fechamento = df.pivot(index='data', columns='ticker', values='fechamento').sort_index().ffill(limit=5)
    distribuicao = df.pivot(index='data', columns='ticker', values='num_distribuicao').sort_index().ffill(limit=5)

    ret = fechamento.pct_change(fill_method=None)
    evento = distribuicao.ne(distribuicao.shift()) & distribuicao.shift().notna()
    return ret.mask(evento & (ret.abs() > 0.15), 0.0)


def _retornos_mensais_acoes(ret_diario: pd.DataFrame) -> pd.DataFrame:
    """Retorno mensal de cada ação; NaN se faltar preço em algum pregão do mês (ex.: troca de ticker)."""
    meses = ret_diario.index.to_period('M')
    ret_mensal = (1 + ret_diario).groupby(meses).prod() - 1
    completo = ret_diario.notna().groupby(meses).all()
    return ret_mensal.where(completo)


def _grupo_atribuicao(classe: str, ativo: str) -> tuple[str, str | None]:
    """Define como cada posição é precificada: (grupo, ticker usado no preço)."""
    if classe == 'Ações (comprado)':
        return 'Ações compradas', ativo
    if classe == 'Ações (vendido)':
        return 'Ações vendidas', ativo
    if classe in ('Renda fixa', 'Caixa e outros', 'Valores a pagar'):
        return 'Caixa e renda fixa (CDI)', None
    if classe == 'Cotas de fundos':
        nome = str(ativo).upper()
        for prefixo, ticker in ETFS_COTAS.items():
            if nome.startswith(prefixo):
                return 'Ações compradas', ticker
        if re.search(REGEX_COTAS_CDI, nome):
            return 'Caixa e renda fixa (CDI)', None
    return 'Não precificado', None


def tickers_atribuicao(df_pos: pd.DataFrame) -> list[str]:
    """Tickers que precisam de preço para a atribuição."""
    pares = df_pos[['classe', 'ativo']].drop_duplicates().itertuples(index=False)
    return sorted({t for c, a in pares if (t := _grupo_atribuicao(c, a)[1])})


def atribuicao_mensal(df_pos: pd.DataFrame, cota: pd.Series, ret_diario: pd.DataFrame, cdi: pd.Series):
    """
    Atribuição de desempenho baseada na carteira (buy-and-hold no mês).

    Para cada mês 'm', usa a carteira (CDA) do fim do mês 'm-1':
        contribuição do ativo = peso no início do mês x retorno do ativo no mês
        caixa e renda fixa    = peso x CDI do mês
        resíduo (return gap)  = retorno da cota - soma das contribuições
    O resíduo inclui: operações feitas dentro do mês, posições não precificadas (exterior, opções, futuros, ações sem
    preço), dividendos/JCP (o COTAHIST não é ajustado), posições confidenciais, taxas e custos.

    Returns
    -------
    df_resumo: pd.DataFrame
        Uma linha por mês com o retorno da cota e a contribuição de cada grupo.
    df_detalhe: pd.DataFrame
        Uma linha por (mês, ativo) com peso, retorno e contribuição.
    """
    ret_cota = retornos_mensais(cota)
    ret_cdi = retornos_mensais(cdi)
    ret_acoes = _retornos_mensais_acoes(ret_diario)

    df_pos = df_pos.copy()
    grupos = [_grupo_atribuicao(c, a) for c, a in zip(df_pos['classe'], df_pos['ativo'])]
    df_pos['grupo'] = [g for g, _ in grupos]
    df_pos['ticker_preco'] = [t for _, t in grupos]
    # A carteira do fim do mês 'm-1' explica o retorno do mês 'm'
    df_pos['mes'] = df_pos['data'].dt.to_period('M') + 1

    lst_detalhe = []
    for mes, carteira in df_pos[df_pos['mes'].isin(ret_cota.index)].groupby('mes'):
        carteira = carteira.copy()
        carteira['retorno'] = np.nan
        precificavel = carteira['ticker_preco'].notna()
        if mes in ret_acoes.index:
            carteira.loc[precificavel, 'retorno'] = carteira.loc[precificavel, 'ticker_preco'].map(ret_acoes.loc[mes])
        carteira.loc[carteira['grupo'] == 'Caixa e renda fixa (CDI)', 'retorno'] = ret_cdi.get(mes, np.nan)
        # Ações sem preço no mês (troca de ticker, deslistagem etc.) passam para 'Não precificado'
        carteira.loc[precificavel & carteira['retorno'].isna(), 'grupo'] = 'Não precificado'
        carteira['contribuicao'] = carteira['peso'] * carteira['retorno']
        lst_detalhe.append(carteira)

    colunas = ['mes', 'grupo', 'classe', 'ativo', 'descricao', 'peso', 'retorno', 'contribuicao']
    if not lst_detalhe:
        return pd.DataFrame(), pd.DataFrame(columns=colunas)
    df_detalhe = pd.concat(lst_detalhe)[colunas].reset_index(drop=True)

    grupos_precificados = ['Ações compradas', 'Ações vendidas', 'Caixa e renda fixa (CDI)']
    df_resumo = (
        df_detalhe[df_detalhe['grupo'].isin(grupos_precificados)]
        .pivot_table(index='mes', columns='grupo', values='contribuicao', aggfunc='sum')
        .reindex(index=sorted(df_detalhe['mes'].unique()), columns=grupos_precificados)
        .fillna(0)
    )
    df_resumo.insert(0, 'Retorno da cota', ret_cota.reindex(df_resumo.index))
    df_resumo['Resíduo (return gap)'] = df_resumo['Retorno da cota'] - df_resumo[grupos_precificados].sum(axis=1)
    df_resumo['Não precificado (% PL)'] = (
        df_detalhe[df_detalhe['grupo'] == 'Não precificado'].groupby('mes')['peso'].sum().reindex(df_resumo.index).fillna(0)
    )
    return df_resumo, df_detalhe
