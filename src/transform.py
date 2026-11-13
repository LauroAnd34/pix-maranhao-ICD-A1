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

Limite da decisão: a política vale enquanto a fonte se mantiver estável. O
fato de a coleta atual passar sem nenhuma violação é o resultado de uma
coleta, não uma garantia. Se o Banco Central passar a publicar com frequência
linhas com problemas pontuais, e não mudanças de estrutura, a quarentena passa
a ser a escolha mais adequada e esta decisão deve ser revista.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pandas as pd
import pandera.pandas as pa

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
    # Tipo de cada coluna. As faixas ficam nos checks logo abaixo, cada um com
    # um error= em snake_case: é esse texto que aparece como motivo no resumo
    # do Fail-Stop.
    AnoMes: int
    Estado: str
    Estado_Ibge: int
    Municipio: str
    Municipio_Ibge: int
    Regiao: str
    Sigla_Regiao: str
    QT_PagadorPF: int
    QT_PagadorPJ: int
    QT_RecebedorPF: int
    QT_RecebedorPJ: int
    QT_PES_PagadorPF: int
    QT_PES_PagadorPJ: int
    QT_PES_RecebedorPF: int
    QT_PES_RecebedorPJ: int
    VL_PagadorPF: float
    VL_PagadorPJ: float
    VL_RecebedorPF: float
    VL_RecebedorPJ: float

    @pa.check("AnoMes", error="anomes_fora_do_periodo")
    def anomes_no_periodo(cls, serie: pd.Series) -> pd.Series:
        """A coleta cobre 2024 e 2025 inteiros."""
        return serie.between(202401, 202512)

    @pa.check("Estado_Ibge", error="estado_ibge_fora_do_escopo")
    def estado_e_maranhao(cls, serie: pd.Series) -> pd.Series:
        """O filtro da coleta é Estado_Ibge eq 21 (Maranhão)."""
        return serie == 21

    @pa.check(
        "QT_PagadorPF", "QT_PagadorPJ", "QT_RecebedorPF", "QT_RecebedorPJ",
        "QT_PES_PagadorPF", "QT_PES_PagadorPJ", "QT_PES_RecebedorPF", "QT_PES_RecebedorPJ",
        error="quantidade_negativa",
    )
    def quantidade_nao_negativa(cls, serie: pd.Series) -> pd.Series:
        return serie >= 0

    @pa.check(
        "VL_PagadorPF", "VL_PagadorPJ", "VL_RecebedorPF", "VL_RecebedorPJ",
        error="valor_negativo",
    )
    def valor_nao_negativo(cls, serie: pd.Series) -> pd.Series:
        return serie >= 0

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

    # Regra de tabela: a chave do grão é (AnoMes, Municipio_Ibge), um município
    # por mês. Sem esta regra, uma coleta lida duas vezes (ou duas coletas de
    # dias diferentes) passaria pelo contrato com todas as linhas repetidas.
    @pa.dataframe_check(error="chave_municipio_mes_repetida")
    def chave_unica(cls, df: pd.DataFrame) -> pd.Series:
        """Cada par (AnoMes, Municipio_Ibge) deve aparecer uma única vez."""
        return ~df.duplicated(subset=["AnoMes", "Municipio_Ibge"], keep=False)

    # Regra de tabela que relaciona colunas entre si.
    # Origem da regra: dedução do grupo a partir da definição das colunas. Toda
    # pessoa contada em QT_PES_ fez pelo menos uma transação, então a quantidade
    # de transações (QT_) nunca pode ser menor que a de pessoas distintas
    # (QT_PES_), em cada uma das quatro combinações pagador/recebedor e PF/PJ.
    # A regra não está na documentação do Bacen, é suposição nossa. Foi
    # verificada no diagnostico.qmd, onde vale para as 5.208 linhas.
    @pa.dataframe_check(error="transacoes_menor_que_pessoas_distintas")
    def transacoes_maiores_que_pessoas(cls, df: pd.DataFrame) -> pd.Series:
        """QT_ >= QT_PES_ nas quatro combinações pagador/recebedor e PF/PJ."""
        return (
            (df["QT_PagadorPF"] >= df["QT_PES_PagadorPF"])
            & (df["QT_PagadorPJ"] >= df["QT_PES_PagadorPJ"])
            & (df["QT_RecebedorPF"] >= df["QT_PES_RecebedorPF"])
            & (df["QT_RecebedorPJ"] >= df["QT_PES_RecebedorPJ"])
        )

    class Config:
        coerce = True
        strict = True

# Funções do pipeline (uma responsabilidade por função)
def ler_dados_brutos() -> pd.DataFrame:
    """Lê a coleta mais recente da pasta raw e devolve um DataFrame.

    O ingest.py grava uma pasta por data de coleta. Ler todas juntaria coletas
    de dias diferentes e repetiria cada município-mês, então lemos só a última.
    As pastas têm a data em formato ISO, por isso a ordem alfabética é a
    ordem cronológica.
    """
    arquivos_json = sorted(RAW_DIR.glob("*/pix_municipios_maranhao.json"))
    if not arquivos_json:
        log.error("Nenhum arquivo de dados brutos encontrado em data/raw/.")
        sys.exit(1)

    arquivo = arquivos_json[-1]
    with open(arquivo, "r", encoding="utf-8") as f:
        conteudo = json.load(f)

    df = pd.DataFrame(conteudo.get("registros", []))
    log.info("Lidos %d registros brutos de %s.", len(df), arquivo)
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
        # failure_cases traz uma falha por célula, então uma linha ruim aparece
        # várias vezes. Contamos linhas distintas por motivo; falha sem linha
        # (de estrutura, como coluna que não está no contrato) conta uma por entrada.
        resumo = (
            exc.failure_cases
            .groupby("check")["index"]
            .agg(lambda s: s.nunique() if s.notna().any() else len(s))
            .reset_index(name="contagem")
            .rename(columns={"check": "motivo"})
            .sort_values("contagem", ascending=False)
        )

        log.error("=" * 60)
        log.error("FAIL-STOP: violações encontradas no contrato!")
        log.error("Resumo por motivo:")
        for _, linha in resumo.iterrows():
            log.error("  %-40s %d ocorrência(s)", linha["motivo"], linha["contagem"])
        log.error("Total de falhas: %d", resumo["contagem"].sum())
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
    # A contagem sai da diferença entre o que foi lido e o que foi aprovado,
    # e não de um número escrito no código.
    rejeitados = len(df_raw) - len(df_transformado)
    log.info("=" * 60)
    log.info("RESUMO DA EXECUÇÃO")
    log.info("  Registros lidos      : %d", len(df_raw))
    log.info("  Registros aprovados  : %d", len(df_transformado))
    log.info("  Registros rejeitados : %d", rejeitados)
    log.info("  Política             : Fail-Stop (lazy=True)")
    log.info("  Motivos de rejeição  : %s", "nenhum" if rejeitados == 0 else "ver relatório acima")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
