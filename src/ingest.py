from __future__ import annotations

import json
import logging
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/"
    "TransacoesPixPorMunicipio(DataBase=@DataBase)"
)

# O parâmetro DataBase do endpoint NÃO filtra um mês exato. 
# Fica fixo num valor antigo, e quem
# realmente seleciona o mês é o filtro AnoMes eq <mês>.

DATABASE_BASE = "202001"
ESTADO_IBGE_MA = 21  # Mais robusto que Estado eq 'MARANHÃO'
FONTE_NOME = "BCB - Pix_DadosAbertos - TransacoesPixPorMunicipio"
FONTE_URL = "https://dadosabertos.bcb.gov.br/dataset/pix"

RAW_DIR = Path("data/raw") / "pix_dados_abertos"
NOME_ARQUIVO = "pix_municipios_maranhao.json"
TIMEOUT_SEGUNDOS = 30
MAX_TENTATIVAS = 5
ESPERA_BASE_SEGUNDOS = 2
ANOS = [2024, 2025]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingest")

# Exceções específicas para cada tipo de erro, para que o chamador possa
# reagir de forma diferenciada.

class ErroCredencial(Exception):
    """401 - a fonte recusou a credencial. Fatal: tentar de novo não resolve,
    é erro de configuração."""

class ErroLimiteRequisicoes(Exception):
    """429 - limite de requisições esgotado mesmo após esperar e tentar de
    novo várias vezes."""

class ErroServidor(Exception):
    """5xx - falha do lado do servidor, persistente mesmo após novas
    tentativas com espera crescente."""

def meses_dos_anos(anos: list[int]) -> list[str]:
    """AAAAMM de janeiro a dezembro de cada ano da lista, em ordem."""
    return [f"{ano}{mes:02d}" for ano in anos for mes in range(1, 13)]

# Coleta de um mês, com tratamento diferenciado de erros de rede, 
# limite de requisições e falha do servidor. Levanta exceções 
# específicas para cada caso, para que o chamador possa reagir

def montar_url(ano_mes: str) -> str:
    """Monta a URL completa para buscar os dados de um mês específico."""
    valor_database = quote(f"'{DATABASE_BASE}'", safe="")
    filtro = quote(
        f"AnoMes eq {int(ano_mes)} and Estado_Ibge eq {ESTADO_IBGE_MA}", safe=""
    )
    return (
        f"{BASE_URL}?@DataBase={valor_database}"
        f"&$format=json&$filter={filtro}&$top=300"
    )

def buscar_mes(ano_mes: str) -> list[dict]:
    """Busca os dados de um mês (filtrado via AnoMes eq <mês> e
    Estado_Ibge eq 21) para o Maranhão. Levanta uma exceção específica
    conforme o tipo de erro."""
    url = montar_url(ano_mes)
    ultimo_status = None

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            resposta = requests.get(url, timeout=TIMEOUT_SEGUNDOS)
        except requests.exceptions.RequestException as exc:
            log.warning(
                "Falha de rede em %s (tentativa %d/%d): %s",
                ano_mes, tentativa, MAX_TENTATIVAS, exc,
            )
            time.sleep(ESPERA_BASE_SEGUNDOS * tentativa)
            continue

        if resposta.status_code == 200:
            return resposta.json().get("value", [])

        if resposta.status_code == 401:
            raise ErroCredencial(
                f"401 ao buscar {ano_mes}: credencial recusada pela API."
            )

        if resposta.status_code == 429:
            ultimo_status = 429
            espera = int(resposta.headers.get("Retry-After", ESPERA_BASE_SEGUNDOS * tentativa))
            log.warning(
                "429 em %s: limite de requisições atingido. Aguardando %ds "
                "(tentativa %d/%d).",
                ano_mes, espera, tentativa, MAX_TENTATIVAS,
            )
            time.sleep(espera)
            continue

        if 500 <= resposta.status_code < 600:
            ultimo_status = resposta.status_code
            espera = ESPERA_BASE_SEGUNDOS * (2 ** (tentativa - 1))
            log.warning(
                "Erro %d (servidor) em %s. Aguardando %ds (tentativa %d/%d).",
                resposta.status_code, ano_mes, espera, tentativa, MAX_TENTATIVAS,
            )
            time.sleep(espera)
            continue

        raise RuntimeError(
            f"Erro inesperado ({resposta.status_code}) ao buscar {ano_mes}: "
            f"{resposta.text[:300]}"
        )

    if ultimo_status == 429:
        raise ErroLimiteRequisicoes(
            f"Limite de requisições persistente em {ano_mes} após {MAX_TENTATIVAS} tentativas."
        )
    raise ErroServidor(
        f"Falha do servidor persistente em {ano_mes} após {MAX_TENTATIVAS} tentativas."
    )

def main() -> None:
    data_coleta = date.today().isoformat()
    pasta_do_dia = RAW_DIR / data_coleta
    saida = pasta_do_dia / NOME_ARQUIVO

    if saida.exists():
        pacote_existente = json.loads(saida.read_text(encoding="utf-8"))
        log.info(
            "%s já existe — coleta de hoje já feita, não duplica. "
            "Registros já coletados hoje: %d",
            saida, pacote_existente["quantidade_registros"],
        )
        return

    meses = meses_dos_anos(ANOS)
    log.info(
        "Coletando %d meses (%s a %s) para Estado_Ibge=%d (Maranhão).",
        len(meses), meses[0], meses[-1], ESTADO_IBGE_MA,
    )

    todos_os_registros: list[dict] = []
    meses_ok: list[str] = []
    meses_falhos: list[str] = []

    for ano_mes in meses:
        try:
            registros = buscar_mes(ano_mes)
        except ErroCredencial as exc:
            log.error("Erro de credencial: %s", exc)
            log.error("Interrompendo a coleta: não é um erro que se resolve tentando de novo.")
            sys.exit(1)
        except (ErroLimiteRequisicoes, ErroServidor, RuntimeError) as exc:
            log.error("Não foi possível coletar %s: %s", ano_mes, exc)
            meses_falhos.append(ano_mes)
            continue

        todos_os_registros.extend(registros)
        meses_ok.append(ano_mes)
        log.info("%s: %d registros.", ano_mes, len(registros))

    if not todos_os_registros:
        log.error("Nenhum registro coletado. Nada gravado.")
        sys.exit(1)

    # Os registros vão sem alteração nenhuma, o raw é imutável.
    pacote = {
        "fonte": FONTE_NOME,
        "fonte_url": FONTE_URL,
        "endpoint": "TransacoesPixPorMunicipio",
        "filtro": f"AnoMes eq <mes> and Estado_Ibge eq {ESTADO_IBGE_MA}",
        "anos_cobertos": ANOS,
        "coletado_em": datetime.now(timezone.utc).isoformat(),
        "meses_coletados_com_sucesso": meses_ok,
        "meses_com_falha": meses_falhos,
        "quantidade_registros": len(todos_os_registros),
        "registros": todos_os_registros,
    }

    pasta_do_dia.mkdir(parents=True, exist_ok=True)
    saida.write_text(json.dumps(pacote, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info("=" * 60)
    log.info("Coleta finalizada.")
    log.info(
        "Meses coletados com sucesso: %d/%d (%s)",
        len(meses_ok), len(meses), ", ".join(meses_ok) or "-",
    )
    if meses_falhos:
        log.warning("Meses que falharam: %s", ", ".join(meses_falhos))
    log.info("Total de registros: %d", len(todos_os_registros))
    log.info("Gravado em: %s", saida.resolve())

if __name__ == "__main__":
    main()