"""
Pipeline de transformação: data/raw/ → data/trusted/

Política escolhida: Fail-Stop
Justificativa: os dados do Pix são publicados pelo Banco Central do Brasil,
uma fonte institucional com controle de qualidade próprio. Qualquer violação
do contrato indica uma mudança estrutural na API (colunas renomeadas, tipos
alterados) e não um simples erro pontual de preenchimento. Nesse cenário,
continuar o pipeline geraria métricas silenciosamente erradas. Portanto,
a decisão mais segura é parar imediatamente, exibir o relatório de erros
agrupado por motivo, e exigir intervenção humana antes de prosseguir.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pandas as pd
import pandera as pa

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("transform")

# Caminhos
RAW_DIR = Path("data/raw/pix_dados_abertos")
TRUSTED_DIR = Path("data/trusted")
QUARENTENA_DIR = Path("data/quarentena")

# 1. CONTRATO DE DADOS (Pandera)
class ContratoPix(pa.DataFrameModel):
    AnoMes: int = pa.Field(
        ge=202401, le=202512,
    )
    Estado: str = pa.Field()
    Estado_Ibge: int = pa.Field(eq=21)
    Municipio: str = pa.Field()
    Municipio_Ibge: int = pa.Field()
    Regiao: str = pa.Field()
    Sigla_Regiao: str = pa.Field()
    QT_PagadorPF: int = pa.Field(ge=0)
    QT_PagadorPJ: int = pa.Field(ge=0)
    QT_RecebedorPF: int = pa.Field(ge=0)
    QT_RecebedorPJ: int = pa.Field(ge=0)
    QT_PES_PagadorPF: int = pa.Field(ge=0)
    QT_PES_PagadorPJ: int = pa.Field(ge=0)
    QT_PES_RecebedorPF: int = pa.Field(ge=0)
    QT_PES_RecebedorPJ: int = pa.Field(ge=0)
    VL_PagadorPF: float = pa.Field(ge=0)
    VL_PagadorPJ: float = pa.Field(ge=0)
    VL_RecebedorPF: float = pa.Field(ge=0)
    VL_RecebedorPJ: float = pa.Field(ge=0)

    # 2. REGRA DE NEGÓCIO DE TABELA (envolvendo duas ou mais colunas)
    # ---------------------------------------------------------------
    # Origem da regra: dedução lógica do domínio financeiro.
    # Em uma transação Pix legítima, o valor total recebido pelo município
    # (VL_RecebedorPF + VL_RecebedorPJ) e o valor total pago
    # (VL_PagadorPF + VL_PagadorPJ) são grandezas independentes, mas ambas
    # devem ser estritamente positivas em um mês com atividade registrada.
    # Um volume total igual a zero tornaria o cálculo do Saldo Relativo
    # indefinido (divisão por zero), o que invalidaria a pergunta analítica.
    # Suposição declarada: todo município presente na base do BCB teve ao
    # menos uma transação Pix (pagamento ou recebimento) no mês reportado.
    @pa.dataframe_check(error="volume_total_nao_positivo")
    def volume_total_positivo(cls, df: pd.DataFrame) -> pd.Series:
        """O volume total (recebido + pago) de cada município-mês deve ser > 0."""
        total_recebido = df["VL_RecebedorPF"] + df["VL_RecebedorPJ"]
        total_pago = df["VL_PagadorPF"] + df["VL_PagadorPJ"]
        return (total_recebido + total_pago) > 0

    class Config:
        coerce = True
        strict = True

# Funções do pipeline (uma responsabilidade por função)
def ler_dados_brutos() -> pd.DataFrame:
    """Lê todos os arquivos JSON da pasta raw e consolida em um único DataFrame."""
    arquivos_json = list(RAW_DIR.glob("*/*.json"))
    if not arquivos_json:
        log.error("Nenhum arquivo de dados brutos encontrado em data/raw/.")
        sys.exit(1)

    todos_registros: list[dict] = []
    for arquivo in arquivos_json:
        with open(arquivo, "r", encoding="utf-8") as f:
            conteudo = json.load(f)
            todos_registros.extend(conteudo.get("registros", []))

    df = pd.DataFrame(todos_registros)
    log.info("Lidos %d registros brutos de %d arquivo(s).", len(df), len(arquivos_json))
    return df


def validar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    3. POLÍTICA: FAIL-STOP com lazy=True.

    Usa lazy=True para acumular TODOS os erros de validação antes de parar.
    Isso permite imprimir o resumo agrupado por motivo mesmo com Fail-Stop.
    Se houver qualquer violação, o pipeline exibe o relatório e encerra com sys.exit(1).
    """
    try:
        df_validado = ContratoPix.validate(df, lazy=True)
        log.info("%d registros validados — nenhuma violação encontrada.", len(df_validado))
        return df_validado

    except pa.errors.SchemaErrors as exc:
        # RESUMO POR MOTIVO
        resumo = (
            exc.failure_cases
            .groupby("check")
            .size()
            .reset_index(name="contagem")
            .rename(columns={"check": "motivo"})
            .sort_values("contagem", ascending=False)
        )

        log.error("=" * 60)
        log.error("FAIL-STOP: violações encontradas no contrato!")
        log.error("Resumo por motivo:")
        for _, linha in resumo.iterrows():
            log.error("  %-40s %d caso(s)", linha["motivo"], linha["contagem"])
        log.error("Total de casos com falha: %d", resumo["contagem"].sum())
        log.error("=" * 60)
        log.error(
            "Pipeline interrompido. Corrija os dados ou revise o contrato."
        )
        sys.exit(1)


def transformar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Cálculo da métrica de Saldo Relativo para responder à Pergunta Analítica."""
    df = df.copy()

    # Higiene de Formato
    df["AnoMes"] = pd.to_datetime(df["AnoMes"].astype(str), format="%Y%m")
    colunas_financeiras = [
        "VL_PagadorPF", "VL_PagadorPJ",
        "VL_RecebedorPF", "VL_RecebedorPJ",
    ]
    df[colunas_financeiras] = df[colunas_financeiras].round(2)

    # Cálculo da Pergunta Analítica: Saldo Relativo
    # (valor recebido − valor pago) ÷ volume total
    valor_recebido = df["VL_RecebedorPF"] + df["VL_RecebedorPJ"]
    valor_pago = df["VL_PagadorPF"] + df["VL_PagadorPJ"]
    volume_total = valor_recebido + valor_pago

    df["saldo_relativo"] = (valor_recebido - valor_pago) / volume_total

    return df


def salvar_trusted(df: pd.DataFrame) -> None:
    """Salva os dados finais validados e transformados na camada trusted."""
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)
    QUARENTENA_DIR.mkdir(parents=True, exist_ok=True)

    caminho_saida = TRUSTED_DIR / "pix_maranhao_trusted.parquet"
    df.to_parquet(caminho_saida, index=False)
    log.info("Dados salvos em %s", caminho_saida)


def main() -> None:
    log.info("Iniciando pipeline de transformação (Raw → Trusted)")

    df_raw = ler_dados_brutos()
    df_validado = validar_dados(df_raw)
    df_transformado = transformar_dados(df_validado)
    salvar_trusted(df_transformado)

    # RESUMO FINAL
    log.info("=" * 60)
    log.info("RESUMO DA EXECUÇÃO")
    log.info("  Registros aprovados  : %d", len(df_transformado))
    log.info("  Registros rejeitados : 0")
    log.info("  Política             : Fail-Stop (lazy=True)")
    log.info("  Motivos de rejeição  : nenhum")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
