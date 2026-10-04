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
    AnoMes: int = pa.Field()
    Estado: str = pa.Field()
    Estado_Ibge: int = pa.Field()
    Municipio: str = pa.Field()
    Municipio_Ibge: int = pa.Field()
    Regiao: str = pa.Field()
    Sigla_Regiao: str = pa.Field()
    QT_PagadorPF: int = pa.Field()
    QT_PagadorPJ: int = pa.Field()
    QT_RecebedorPF: int = pa.Field()
    QT_RecebedorPJ: int = pa.Field()
    QT_PES_PagadorPF: int = pa.Field()
    QT_PES_PagadorPJ: int = pa.Field()
    QT_PES_RecebedorPF: int = pa.Field()
    QT_PES_RecebedorPJ: int = pa.Field()
    VL_PagadorPF: float = pa.Field()
    VL_PagadorPJ: float = pa.Field()
    VL_RecebedorPF: float = pa.Field()
    VL_RecebedorPJ: float = pa.Field()

    # Regra de negócio envolvendo múltiplas colunas:
    # A soma dos valores transacionados não pode ser menor que zero.
    @pa.dataframe_check(error="soma_valores_negativa")
    def valida_valores_positivos(cls, df: pd.DataFrame) -> pd.Series:
        soma = df["VL_PagadorPF"] + df["VL_PagadorPJ"] + df["VL_RecebedorPF"] + df["VL_RecebedorPJ"]
        return soma >= 0

    class Config:
        coerce = True
        strict = True


def ler_dados_brutos() -> pd.DataFrame:
    """Lê todos os arquivos JSON da pasta raw e consolida em um único DataFrame."""
    arquivos_json = list(RAW_DIR.glob("*/*.json"))
    if not arquivos_json:
        log.error("Nenhum arquivo de dados brutos encontrado em data/raw/.")
        sys.exit(1)

    todos_registros = []
    for arquivo in arquivos_json:
        with open(arquivo, 'r', encoding='utf-8') as f:
            conteudo = json.load(f)
            todos_registros.extend(conteudo.get("registros", []))
            
    df = pd.DataFrame(todos_registros)
    return df


def validar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica o contrato Pandera. Política: Fail-Stop."""
    try:
        df_validado = ContratoPix.validate(df)
        log.info(f"{len(df_validado)} registros validados com sucesso no Contrato.")
        return df_validado
    except pa.errors.SchemaError as exc:
        log.error(f"Erro de validação (Fail-Stop ativado): {exc}")
        sys.exit(1)


def transformar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Cálculo da métrica de Saldo Relativo para responder à Pergunta Analítica."""
    df = df.copy()
    
    # Higiene de Formato
    df["AnoMes"] = pd.to_datetime(df["AnoMes"].astype(str), format="%Y%m")
    colunas_financeiras = ["VL_PagadorPF", "VL_PagadorPJ", "VL_RecebedorPF", "VL_RecebedorPJ"]
    df[colunas_financeiras] = df[colunas_financeiras].round(2)

    # Cálculo da Pergunta Analítica: Saldo Relativo
    # (valor recebido - valor pago) / volume total
    valor_recebido = df["VL_RecebedorPF"] + df["VL_RecebedorPJ"]
    valor_pago = df["VL_PagadorPF"] + df["VL_PagadorPJ"]
    volume_total = valor_recebido + valor_pago
    
    # Evitando divisão por zero (retorna nulo se o volume for zero)
    df["saldo_relativo"] = (valor_recebido - valor_pago) / volume_total.replace(0, pd.NA)
    
    return df


def salvar_trusted(df: pd.DataFrame) -> None:
    """Salva os dados finais validados e transformados na camada trusted."""
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)
    QUARENTENA_DIR.mkdir(parents=True, exist_ok=True)
    
    caminho_saida = TRUSTED_DIR / "pix_maranhao_trusted.parquet"
    df.to_parquet(caminho_saida, index=False)
    log.info(f"Dados salvos com sucesso em {caminho_saida}")


def main() -> None:
    log.info("Iniciando pipeline de transformação (Raw -> Trusted)")
    
    df_raw = ler_dados_brutos()
    df_validado = validar_dados(df_raw)
    df_transformado = transformar_dados(df_validado)
    salvar_trusted(df_transformado)
    
    log.info("=" * 60)
    log.info("RESUMO DA EXECUÇÃO:")
    log.info(f"Registros processados e aprovados: {len(df_transformado)}")
    log.info("Registros enviados para quarentena: 0 (Política Fail-Stop ativada)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
