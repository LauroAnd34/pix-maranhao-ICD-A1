"""Leva o Pix Maranhão de data/trusted/ para data/gold/, no modelo estrela.

Artefato da aula de Star Schema: aplicação. Constrói o desenho da aula de Star
Schema em DuckDB, confere e grava em Parquet. Roda depois do transform.py, sem
nenhuma edição:

    uv run python src/model.py

O que ele faz, na ordem de Kimball e Ross (2013, cap. 20):

1. dimensoes()        dim_data e dim_municipio, cada uma com a sua chave.
                      A dim_data cobre o período inteiro (24 meses), inclusive
                      se houvesse meses sem eventos.
2. conferir_chaves()  toda chave natural de pix_maranhao_trusted existe na dimensão?
                      Se faltar alguma, para aqui, antes de existir a fato.
3. fatos()            fato_pix_mensal, com a chave natural trocada pela
                      substituta (municipio_sk). Nenhuma chave pode ficar nula.
4. conferir_fatos()   contagem de linhas e soma de cada medida, gold contra
                      trusted.
5. agregada()         fato_pix_anual_municipio, derivada de fato_pix_mensal e
                      conferida contra ela.
6. gravar()           uma tabela por arquivo em data/gold/, e relê cada um.

Qualquer conferência que não bate para o script com a mensagem, e nada é
gravado: as tabelas vivem na memória até o passo 6.

A troca de chave é por LEFT JOIN, nunca por JOIN. Com JOIN, a linha cuja chave
não está na dimensão some da fato sem erro nenhum; com LEFT JOIN, ela fica, com
a chave nula, e a conferência do passo 3 acusa.
"""

from pathlib import Path

import duckdb

DIR_TRUSTED = Path("data/trusted")
DIR_GOLD = Path("data/gold")

TABELAS_TRUSTED = ("pix_maranhao_trusted",)
TABELAS_GOLD = ("dim_data", "dim_municipio", "fato_pix_mensal", "fato_pix_anual_municipio")

# (origem, chave natural, dimensão, coluna da dimensão)
CHAVES = [
    ("pix_maranhao_trusted", "o.AnoMes::DATE", "dim_data", "data"),
    ("pix_maranhao_trusted", "o.Municipio_Ibge", "dim_municipio", "codigo_ibge"),
]

# (o que se confere, na trusted, no gold)
CONFERENCIA = [
    ("linhas", "SELECT COUNT(*) FROM pix_maranhao_trusted", "SELECT COUNT(*) FROM fato_pix_mensal"),
    ("VL_PagadorPF", "SELECT SUM(VL_PagadorPF) FROM pix_maranhao_trusted", "SELECT SUM(vl_pagador_pf) FROM fato_pix_mensal"),
    ("VL_PagadorPJ", "SELECT SUM(VL_PagadorPJ) FROM pix_maranhao_trusted", "SELECT SUM(vl_pagador_pj) FROM fato_pix_mensal"),
    ("VL_RecebedorPF", "SELECT SUM(VL_RecebedorPF) FROM pix_maranhao_trusted", "SELECT SUM(vl_recebedor_pf) FROM fato_pix_mensal"),
    ("VL_RecebedorPJ", "SELECT SUM(VL_RecebedorPJ) FROM pix_maranhao_trusted", "SELECT SUM(vl_recebedor_pj) FROM fato_pix_mensal"),
    ("QT_PagadorPF", "SELECT SUM(QT_PagadorPF) FROM pix_maranhao_trusted", "SELECT SUM(qt_pagador_pf) FROM fato_pix_mensal"),
    ("QT_PagadorPJ", "SELECT SUM(QT_PagadorPJ) FROM pix_maranhao_trusted", "SELECT SUM(qt_pagador_pj) FROM fato_pix_mensal"),
    ("QT_RecebedorPF", "SELECT SUM(QT_RecebedorPF) FROM pix_maranhao_trusted", "SELECT SUM(qt_recebedor_pf) FROM fato_pix_mensal"),
    ("QT_RecebedorPJ", "SELECT SUM(QT_RecebedorPJ) FROM pix_maranhao_trusted", "SELECT SUM(qt_recebedor_pj) FROM fato_pix_mensal"),
    ("QT_PES_PagadorPF", "SELECT SUM(QT_PES_PagadorPF) FROM pix_maranhao_trusted", "SELECT SUM(qt_pes_pagador_pf) FROM fato_pix_mensal"),
    ("QT_PES_PagadorPJ", "SELECT SUM(QT_PES_PagadorPJ) FROM pix_maranhao_trusted", "SELECT SUM(qt_pes_pagador_pj) FROM fato_pix_mensal"),
    ("QT_PES_RecebedorPF", "SELECT SUM(QT_PES_RecebedorPF) FROM pix_maranhao_trusted", "SELECT SUM(qt_pes_recebedor_pf) FROM fato_pix_mensal"),
    ("QT_PES_RecebedorPJ", "SELECT SUM(QT_PES_RecebedorPJ) FROM pix_maranhao_trusted", "SELECT SUM(qt_pes_recebedor_pj) FROM fato_pix_mensal"),
]


def parar(mensagem: str) -> None:
    raise SystemExit(f"{mensagem}\nnada foi gravado em {DIR_GOLD}/")


def abrir() -> duckdb.DuckDBPyConnection:
    """Banco em memória, com uma view por tabela da trusted."""
    con = duckdb.connect()
    for tabela in TABELAS_TRUSTED:
        caminho = DIR_TRUSTED / f"{tabela}.parquet"
        if not caminho.exists():
            raise SystemExit(f"{caminho} não encontrado: rode antes o src/transform.py")
        con.execute(f"CREATE VIEW {tabela} AS SELECT * FROM '{caminho}'")
    return con


def uma_linha_por_membro(con: duckdb.DuckDBPyConnection, tabela: str, chave: str) -> None:
    """A chave substituta só se atribui se cada membro tiver uma linha só."""
    linhas, distintos = con.sql(f"SELECT COUNT(*), COUNT(DISTINCT {chave}) FROM {tabela}").fetchall()[0]
    if linhas != distintos:
        parar(f"{tabela}: {linhas} linhas para {distintos} valores de {chave}")


def dimensoes(con: duckdb.DuckDBPyConnection) -> None:
    """As duas dimensões. ROW_NUMBER() numera pela ordem da chave natural.

    A dim_data se gera do primeiro ao último mês das coletas, e não se tira das
    datas que aparecem no trusted: os meses sem nenhum evento também seriam linhas.
    O generate_series inclui o último mês. O range, não: com ele, o último mês
    fica de fora da dimensão.
    """
    con.execute("""
        CREATE TABLE dim_data AS
        SELECT dia::DATE AS data,
               year(dia) AS ano,
               month(dia) AS mes,
               monthname(dia) AS nome_mes,
               quarter(dia) AS trimestre,
               CASE WHEN month(dia) <= 6 THEN 1 ELSE 2 END AS semestre
        FROM generate_series(
            (SELECT MIN(AnoMes)::DATE FROM pix_maranhao_trusted),
            (SELECT MAX(AnoMes)::DATE FROM pix_maranhao_trusted),
            INTERVAL 1 MONTH) AS t(dia)
    """)

    con.execute("""
        CREATE TABLE dim_municipio AS
        SELECT ROW_NUMBER() OVER (ORDER BY Municipio_Ibge) AS municipio_sk,
               Municipio_Ibge AS codigo_ibge,
               Municipio AS nome_municipio,
               Estado_Ibge AS codigo_estado,
               'MA' AS sigla_estado,
               Estado AS estado,
               Sigla_Regiao AS sigla_regiao,
               Regiao AS regiao
        FROM (
            SELECT DISTINCT Municipio_Ibge, Municipio, Estado_Ibge, Estado, Sigla_Regiao, Regiao
            FROM pix_maranhao_trusted
        )
    """)

    uma_linha_por_membro(con, "dim_data", "data")
    uma_linha_por_membro(con, "dim_municipio", "codigo_ibge")


def conferir_chaves(con: duckdb.DuckDBPyConnection) -> None:
    """Antes da troca: toda chave natural existe na dimensão?

    Conta linhas, e não chaves distintas: é o que se perderia da fato.
    """
    print("linhas cuja chave não está na dimensão, antes da troca:")
    faltando = 0
    for origem, chave, dimensao, coluna in CHAVES:
        n = con.sql(f"""
            SELECT COUNT(*)
            FROM {origem} o LEFT JOIN {dimensao} d ON d.{coluna} = {chave}
            WHERE d.{coluna} IS NULL
        """).fetchall()[0][0]
        print(f"  {origem + '.' + chave.removeprefix('o.'):35} -> {dimensao:15} {n:5d}")
        if n:
            ausentes = con.sql(f"""
                SELECT DISTINCT {chave} AS chave
                FROM {origem} o LEFT JOIN {dimensao} d ON d.{coluna} = {chave}
                WHERE d.{coluna} IS NULL
                ORDER BY 1
                LIMIT 5
            """).fetchall()
            print(f"    chaves que faltam em {dimensao} (até 5): {[str(c) for (c,) in ausentes]}")
        faltando += n
    if faltando:
        parar(f"{faltando} linhas sem chave na dimensão: corrija a dimensão antes de construir a fato")


def fatos(con: duckdb.DuckDBPyConnection) -> None:
    """A fato atômica mensal, com a chave substituta no lugar da natural.

    A data é a própria chave da dim_data.
    """
    con.execute("""
        CREATE TABLE fato_pix_mensal AS
        SELECT
            m.municipio_sk,
            d.data,
            p.VL_PagadorPF AS vl_pagador_pf,
            p.VL_PagadorPJ AS vl_pagador_pj,
            p.VL_RecebedorPF AS vl_recebedor_pf,
            p.VL_RecebedorPJ AS vl_recebedor_pj,
            (p.VL_PagadorPF + p.VL_PagadorPJ + p.VL_RecebedorPF + p.VL_RecebedorPJ) AS vl_total,
            p.QT_PagadorPF AS qt_pagador_pf,
            p.QT_PagadorPJ AS qt_pagador_pj,
            p.QT_RecebedorPF AS qt_recebedor_pf,
            p.QT_RecebedorPJ AS qt_recebedor_pj,
            (p.QT_PagadorPF + p.QT_PagadorPJ + p.QT_RecebedorPF + p.QT_RecebedorPJ) AS qt_total,
            p.QT_PES_PagadorPF AS qt_pes_pagador_pf,
            p.QT_PES_PagadorPJ AS qt_pes_pagador_pj,
            p.QT_PES_RecebedorPF AS qt_pes_recebedor_pf,
            p.QT_PES_RecebedorPJ AS qt_pes_recebedor_pj,
            p.saldo_relativo
        FROM pix_maranhao_trusted p
        LEFT JOIN dim_municipio m ON m.codigo_ibge = p.Municipio_Ibge
        LEFT JOIN dim_data d ON d.data = p.AnoMes::DATE
        ORDER BY d.data, m.municipio_sk
    """)

    nulas = con.sql("""
        SELECT COUNT(*) FROM fato_pix_mensal
        WHERE data IS NULL OR municipio_sk IS NULL
    """).fetchall()[0][0]
    if nulas:
        parar(f"{nulas} linhas das fatos com chave nula")


def conferir_fatos(con: duckdb.DuckDBPyConnection) -> None:
    """Contagem e soma de cada medida, gold contra trusted. Tem de bater no centavo."""
    print("\nconferência das fatos contra a trusted:")
    print(f"  {'':20} {'trusted':>18} {'gold':>18}")
    diferentes = []
    for medida, na_trusted, no_gold in CONFERENCIA:
        trusted, gold = con.sql(f"SELECT ({na_trusted}), ({no_gold})").fetchall()[0]
        formato = "18d" if isinstance(trusted, int) else "18.2f"
        print(f"  {medida:20} {trusted:{formato}} {gold:{formato}}")
        if round(float(trusted), 2) != round(float(gold), 2):
            diferentes.append(medida)
    if diferentes:
        parar(f"o gold não bate com a trusted em: {', '.join(diferentes)}")


def agregada(con: duckdb.DuckDBPyConnection) -> None:
    """Uma linha por município e ano, calculada de fato_pix_mensal e conferida contra ela."""
    con.execute("""
        CREATE TABLE fato_pix_anual_municipio AS
        SELECT
            f.municipio_sk,
            year(f.data) AS ano,
            COUNT(*) AS meses,
            ROUND(SUM(f.vl_recebedor_pf + f.vl_recebedor_pj), 2) AS vl_recebido_anual,
            ROUND(SUM(f.vl_pagador_pf + f.vl_pagador_pj), 2) AS vl_pago_anual,
            ROUND(SUM(f.vl_total), 2) AS vl_total_anual,
            SUM(f.qt_total)::BIGINT AS qt_total_transacoes,
            ROUND((SUM(f.vl_recebedor_pf + f.vl_recebedor_pj) - SUM(f.vl_pagador_pf + f.vl_pagador_pj)) / SUM(f.vl_total), 6) AS saldo_relativo_anual
        FROM fato_pix_mensal f
        GROUP BY ALL
        ORDER BY f.municipio_sk, ano
    """)

    somas = "COUNT(*), ROUND(SUM(vl_total), 2), SUM(qt_total)"
    atomica = con.sql(f"SELECT {somas} FROM fato_pix_mensal").fetchall()[0]
    agregada_res = con.sql("""
        SELECT SUM(meses), ROUND(SUM(vl_total_anual), 2), SUM(qt_total_transacoes)
        FROM fato_pix_anual_municipio
    """).fetchall()[0]
    if atomica != agregada_res:
        parar(f"a agregada não bate com a fato_pix_mensal: {agregada_res} contra {atomica}")


def gravar(con: duckdb.DuckDBPyConnection) -> None:
    """Uma tabela por arquivo Parquet, e a releitura de cada um."""
    DIR_GOLD.mkdir(parents=True, exist_ok=True)
    print(f"\ngold  ->  {DIR_GOLD}/")
    for tabela in TABELAS_GOLD:
        caminho = DIR_GOLD / f"{tabela}.parquet"
        con.execute(f"COPY {tabela} TO '{caminho}' (FORMAT parquet)")
        relidas = con.sql(f"SELECT COUNT(*) FROM '{caminho}'").fetchall()[0][0]
        print(f"  {tabela:26} {relidas:5d} linhas")


def main() -> None:
    con = abrir()
    dimensoes(con)
    conferir_chaves(con)
    fatos(con)
    conferir_fatos(con)
    agregada(con)
    gravar(con)


if __name__ == "__main__":
    main()
