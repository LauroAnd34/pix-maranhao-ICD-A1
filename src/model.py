"""
Modelagem dimensional (Star Schema): data/trusted/ → data/gold/

Este script lê a base trusted do Pix dos municípios do Maranhão, constrói
as dimensões (com chaves substitutas, exceto tempo) e as tabelas fato (atômica
e agregada anual), valida a integridade referencial antes de qualquer gravação
(política Fail-Stop com relatório de chaves faltantes) e confere as métricas
lado a lado entre trusted e gold.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("model")

# Caminhos
ROOT = Path.cwd()
TRUSTED_PATH = ROOT / "data" / "trusted" / "pix_maranhao_trusted.parquet"
GOLD_DIR = ROOT / "data" / "gold"

# 1. METADADOS DO MODELO
TABELAS_TRUSTED = [str(TRUSTED_PATH.as_posix())]

TABELAS_GOLD = [
    "dim_tempo",
    "dim_municipio",
    "fato_pix_mensal",
    "fato_pix_anual_municipio",
]

CHAVES = {
    "dim_tempo": "data",
    "dim_municipio": "sk_municipio",
    "fato_pix_mensal": ["sk_municipio", "data"],
    "fato_pix_anual_municipio": ["sk_municipio", "ano"],
}

CONFERENCIA = [
    ("VL_PagadorPF", "vl_pagador_pf"),
    ("VL_PagadorPJ", "vl_pagador_pj"),
    ("VL_RecebedorPF", "vl_recebedor_pf"),
    ("VL_RecebedorPJ", "vl_recebedor_pj"),
    ("QT_PagadorPF", "qt_pagador_pf"),
    ("QT_PagadorPJ", "qt_pagador_pj"),
    ("QT_RecebedorPF", "qt_recebedor_pf"),
    ("QT_RecebedorPJ", "qt_recebedor_pj"),
    ("QT_PES_PagadorPF", "qt_pes_pagador_pf"),
    ("QT_PES_PagadorPJ", "qt_pes_pagador_pj"),
    ("QT_PES_RecebedorPF", "qt_pes_recebedor_pf"),
    ("QT_PES_RecebedorPJ", "qt_pes_recebedor_pj"),
]

NOMES_MESES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}


def ler_trusted() -> pd.DataFrame:
    """Lê os dados da camada trusted."""
    if not TRUSTED_PATH.exists():
        log.error("Arquivo trusted não encontrado: %s", TRUSTED_PATH)
        log.error("Execute antes: uv run python src/transform.py")
        sys.exit(1)
    df = pd.read_parquet(TRUSTED_PATH)
    log.info("Trusted carregado: %d registros de %s", len(df), TRUSTED_PATH.name)
    return df


def dimensoes(df_trusted: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Gera as tabelas de dimensão (dim_tempo e dim_municipio).

    - dim_tempo: dimensão de data contínua cobrindo todos os meses do período,
      inclusive se houvesse meses sem eventos. Usa a própria data como chave primária.
    - dim_municipio: dimensão com chave substituta (sk_municipio) inteira e sequencial.
      Atributos de estado e região ficam desnormalizados aqui (Star Schema).
    """
    # 1. Dimensão Tempo contínua
    data_min = df_trusted["AnoMes"].min()
    data_max = df_trusted["AnoMes"].max()
    datas_continuas = pd.date_range(start=data_min, end=data_max, freq="MS")

    dim_tempo = pd.DataFrame({"data": datas_continuas})
    dim_tempo["ano"] = dim_tempo["data"].dt.year
    dim_tempo["mes"] = dim_tempo["data"].dt.month
    dim_tempo["nome_mes"] = dim_tempo["mes"].map(NOMES_MESES)
    dim_tempo["trimestre"] = ((dim_tempo["mes"] - 1) // 3) + 1
    dim_tempo["semestre"] = ((dim_tempo["mes"] - 1) // 6) + 1

    # 2. Dimensão Município com Chave Substituta
    cols_mun = [
        "Municipio_Ibge",
        "Municipio",
        "Estado_Ibge",
        "Estado",
        "Sigla_Regiao",
        "Regiao",
    ]
    mun_unicos = df_trusted[cols_mun].drop_duplicates().sort_values("Municipio_Ibge").reset_index(drop=True)

    dim_municipio = pd.DataFrame({
        "sk_municipio": range(1, len(mun_unicos) + 1),
        "codigo_ibge": mun_unicos["Municipio_Ibge"],
        "nome_municipio": mun_unicos["Municipio"],
        "codigo_estado": mun_unicos["Estado_Ibge"],
        "sigla_estado": "MA",
        "estado": mun_unicos["Estado"],
        "sigla_regiao": mun_unicos["Sigla_Regiao"],
        "regiao": mun_unicos["Regiao"],
    })

    log.info("Dimensões construídas: dim_tempo (%d linhas), dim_municipio (%d linhas)", len(dim_tempo), len(dim_municipio))
    return dim_tempo, dim_municipio


def fatos(
    df_trusted: pd.DataFrame,
    dim_municipio: pd.DataFrame,
    dim_tempo: pd.DataFrame,
) -> pd.DataFrame:
    """Gera a tabela fato atômica (fato_pix_mensal) ligada às dimensões."""
    # Mapeamento do código natural IBGE para a chave substituta sk_municipio
    de_para_mun = dict(zip(dim_municipio["codigo_ibge"], dim_municipio["sk_municipio"]))

    fato = pd.DataFrame()
    fato["sk_municipio"] = df_trusted["Municipio_Ibge"].map(de_para_mun)
    fato["data"] = df_trusted["AnoMes"]

    # Medidas financeiras (aditivas)
    fato["vl_pagador_pf"] = df_trusted["VL_PagadorPF"]
    fato["vl_pagador_pj"] = df_trusted["VL_PagadorPJ"]
    fato["vl_recebedor_pf"] = df_trusted["VL_RecebedorPF"]
    fato["vl_recebedor_pj"] = df_trusted["VL_RecebedorPJ"]
    fato["vl_total"] = (
        fato["vl_pagador_pf"] + fato["vl_pagador_pj"] +
        fato["vl_recebedor_pf"] + fato["vl_recebedor_pj"]
    )

    # Medidas de contagem de transações (aditivas)
    fato["qt_pagador_pf"] = df_trusted["QT_PagadorPF"]
    fato["qt_pagador_pj"] = df_trusted["QT_PagadorPJ"]
    fato["qt_recebedor_pf"] = df_trusted["QT_RecebedorPF"]
    fato["qt_recebedor_pj"] = df_trusted["QT_RecebedorPJ"]
    fato["qt_total"] = (
        fato["qt_pagador_pf"] + fato["qt_pagador_pj"] +
        fato["qt_recebedor_pf"] + fato["qt_recebedor_pj"]
    )

    # Pessoas distintas (semi-aditivas: não somam no tempo)
    fato["qt_pes_pagador_pf"] = df_trusted["QT_PES_PagadorPF"]
    fato["qt_pes_pagador_pj"] = df_trusted["QT_PES_PagadorPJ"]
    fato["qt_pes_recebedor_pf"] = df_trusted["QT_PES_RecebedorPF"]
    fato["qt_pes_recebedor_pj"] = df_trusted["QT_PES_RecebedorPJ"]

    # Saldo relativo (razão / não-aditiva)
    fato["saldo_relativo"] = df_trusted["saldo_relativo"]

    log.info("Fato atômica construída: fato_pix_mensal (%d linhas)", len(fato))
    return fato


def agregada(fato_mensal: pd.DataFrame, dim_tempo: pd.DataFrame) -> pd.DataFrame:
    """Gera a tabela fato agregada anual por município (fato_pix_anual_municipio).

    Atende à pergunta analítica da equipe: comparação de saldo relativo entre
    2024 e 2025. Perde a chave atômica da data mensal e recalcula a razão.
    """
    mapa_ano = dict(zip(dim_tempo["data"], dim_tempo["ano"]))
    df = fato_mensal.copy()
    df["ano"] = df["data"].map(mapa_ano)

    df["vl_recebido"] = df["vl_recebedor_pf"] + df["vl_recebedor_pj"]
    df["vl_pago"] = df["vl_pagador_pf"] + df["vl_pagador_pj"]

    agg = df.groupby(["sk_municipio", "ano"], as_index=False).agg({
        "vl_recebido": "sum",
        "vl_pago": "sum",
        "vl_total": "sum",
        "qt_total": "sum",
    }).rename(columns={
        "vl_recebido": "vl_recebido_anual",
        "vl_pago": "vl_pago_anual",
        "vl_total": "vl_total_anual",
        "qt_total": "qt_total_transacoes",
    })

    # Recálculo exato da razão no grão anual
    agg["saldo_relativo_anual"] = (
        (agg["vl_recebido_anual"] - agg["vl_pago_anual"]) / agg["vl_total_anual"]
    ).round(6)

    log.info("Fato agregada construída: fato_pix_anual_municipio (%d linhas)", len(agg))
    return agg


def verificar_integridade_referencial(
    fato_mensal: pd.DataFrame,
    fato_anual: pd.DataFrame,
    dim_municipio: pd.DataFrame,
    dim_tempo: pd.DataFrame,
) -> None:
    """Valida se todas as chaves estrangeiras existem nas dimensões (Fail-Stop).

    Se houver qualquer chave órfã, o pipeline para imediatamente e aponta as
    chaves que faltam antes de gravar qualquer arquivo em data/gold/.
    """
    erros: list[str] = []

    # 1. Chave de município na fato mensal
    chaves_mun_dim = set(dim_municipio["sk_municipio"])
    orfas_mun_mensal = set(fato_mensal["sk_municipio"].dropna()) - chaves_mun_dim
    nulos_mun = fato_mensal["sk_municipio"].isna().sum()

    if nulos_mun > 0:
        erros.append(f"fato_pix_mensal possui {nulos_mun} linha(s) sem chave de município (sk_municipio nulo).")
    if orfas_mun_mensal:
        erros.append(f"fato_pix_mensal possui chaves sk_municipio inexistentes na dim_municipio: {sorted(orfas_mun_mensal)[:5]}")

    # 2. Chave de data na fato mensal
    chaves_tempo_dim = set(dim_tempo["data"])
    orfas_tempo_mensal = set(fato_mensal["data"]) - chaves_tempo_dim
    if orfas_tempo_mensal:
        erros.append(f"fato_pix_mensal possui datas inexistentes na dim_tempo: {sorted(orfas_tempo_mensal)[:5]}")

    # 3. Chave de município na fato anual
    orfas_mun_anual = set(fato_anual["sk_municipio"]) - chaves_mun_dim
    if orfas_mun_anual:
        erros.append(f"fato_pix_anual_municipio possui chaves sk_municipio inexistentes na dim_municipio: {sorted(orfas_mun_anual)[:5]}")

    if erros:
        log.error("=" * 60)
        log.error("FALHA DE INTEGRIDADE REFERENCIAL NO STAR SCHEMA (Fail-Stop)!")
        for err in erros:
            log.error("  -> %s", err)
        log.error("Pipeline interrompido. Nenhuma tabela foi gravada em data/gold/.")
        log.error("=" * 60)
        sys.exit(1)

    log.info("Integridade referencial validada: 100% das chaves batem com as dimensões.")


def conferir_trusted_vs_gold(
    df_trusted: pd.DataFrame,
    fato_mensal: pd.DataFrame,
    fato_anual: pd.DataFrame,
) -> None:
    """Imprime a conferência estrutural lado a lado entre trusted e gold."""
    log.info("=" * 70)
    log.info("CONFERÊNCIA ESTRUTURAL: TRUSTED vs GOLD")
    log.info("=" * 70)

    # 1. Contagem de linhas
    log.info("%-30s | %-15s | %-15s | %s", "Métrica / Linhas", "Trusted", "Gold (Mensal)", "Status")
    log.info("-" * 70)
    n_trusted = len(df_trusted)
    n_gold = len(fato_mensal)
    status_linhas = "OK" if n_trusted == n_gold else "DIVERGENTE"
    log.info("%-30s | %-15d | %-15d | %s", "Contagem de registros", n_trusted, n_gold, status_linhas)

    if status_linhas != "OK":
        log.error("Contagem de linhas divergente! Interrompendo...")
        sys.exit(1)

    # 2. Soma de cada medida
    divergencias = 0
    for col_trusted, col_gold in CONFERENCIA:
        soma_trusted = df_trusted[col_trusted].sum()
        soma_gold = fato_mensal[col_gold].sum()

        diff = abs(soma_trusted - soma_gold)
        status = "OK" if diff < 1e-2 else "DIVERGENTE"
        if status != "OK":
            divergencias += 1

        if "VL_" in col_trusted:
            log.info("%-30s | %-15.2f | %-15.2f | %s", col_trusted, soma_trusted, soma_gold, status)
        else:
            log.info("%-30s | %-15d | %-15d | %s", col_trusted, int(soma_trusted), int(soma_gold), status)

    # 3. Conferência da Fato Agregada contra a Fato Atômica
    log.info("-" * 70)
    log.info("CONFERÊNCIA DA FATO AGREGADA (fato_pix_anual_municipio vs fato_pix_mensal)")
    log.info("-" * 70)
    soma_vol_mensal = fato_mensal["vl_total"].sum()
    soma_vol_anual = fato_anual["vl_total_anual"].sum()
    diff_vol = abs(soma_vol_mensal - soma_vol_anual)
    status_vol = "OK" if diff_vol < 1e-2 else "DIVERGENTE"

    soma_qt_mensal = fato_mensal["qt_total"].sum()
    soma_qt_anual = fato_anual["qt_total_transacoes"].sum()
    status_qt = "OK" if soma_qt_mensal == soma_qt_anual else "DIVERGENTE"

    log.info("%-30s | %-15.2f | %-15.2f | %s", "Volume total financeiro", soma_vol_mensal, soma_vol_anual, status_vol)
    log.info("%-30s | %-15d | %-15d | %s", "Quantidade de transações", soma_qt_mensal, soma_qt_anual, status_qt)
    log.info("Registros da fato anual: %d linhas (esperado 217 mun x 2 anos = 434)", len(fato_anual))
    log.info("=" * 70)

    if divergencias > 0 or status_vol != "OK" or status_qt != "OK":
        log.error("Divergência detectada nas métricas! Pipeline abortado.")
        sys.exit(1)


def salvar_gold(
    dim_tempo: pd.DataFrame,
    dim_municipio: pd.DataFrame,
    fato_mensal: pd.DataFrame,
    fato_anual: pd.DataFrame,
) -> None:
    """Grava as tabelas do Star Schema em formato Parquet na pasta data/gold/."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    tabelas = {
        "dim_tempo": dim_tempo,
        "dim_municipio": dim_municipio,
        "fato_pix_mensal": fato_mensal,
        "fato_pix_anual_municipio": fato_anual,
    }

    for nome, df in tabelas.items():
        destino = GOLD_DIR / f"{nome}.parquet"
        df.to_parquet(destino, index=False)
        log.info("Gravado: %s (%d linhas)", destino.relative_to(ROOT), len(df))


def main() -> None:
    log.info("Iniciando construção da camada gold (Star Schema)...")

    df_trusted = ler_trusted()
    dim_tempo, dim_municipio = dimensoes(df_trusted)
    fato_mensal = fatos(df_trusted, dim_municipio, dim_tempo)
    fato_anual = agregada(fato_mensal, dim_tempo)

    # Validação Fail-Stop antes de gravar
    verificar_integridade_referencial(fato_mensal, fato_anual, dim_municipio, dim_tempo)

    # Conferência lado a lado das métricas
    conferir_trusted_vs_gold(df_trusted, fato_mensal, fato_anual)

    # Gravação dos arquivos finais em data/gold/
    salvar_gold(dim_tempo, dim_municipio, fato_mensal, fato_anual)

    log.info("Camada gold construída com sucesso em %s!", GOLD_DIR.relative_to(ROOT))


if __name__ == "__main__":
    main()
