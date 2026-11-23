# Pix por município — Maranhão (2024-2025)

Projeto da Etapa 1 
Introdução à Ciência de Dados, IFMA, prof. Josenildo Silva.

## Fonte de dados

**API Pix_DadosAbertos, do Banco Central do Brasil (via Olinda).**
Endpoint: `TransacoesPixPorMunicipio`
Documentação: https://dadosabertos.bcb.gov.br/dataset/pix

**Não exige chave nem cadastro.** O `.env.example` existe só por padrão do
projeto; não há nada para preencher nele.

Filtro usado: `Estado_Ibge eq 21` (Maranhão) — orientação do professor em
28/09/2026, mais robusta que comparar pelo nome do estado por extenso.

## Pergunta analítica

Quais municípios do Maranhão apresentaram as maiores variações no saldo
relativo do Pix entre 2024 e 2025?

(saldo relativo = (valor recebido − valor pago) ÷ volume total, para
normalizar o crescimento de mais de 100x do Pix desde 2020 — ver as notas
da aula-3)

## Como rodar

```bash
git clone https://github.com/LauroAnd34/pix-maranhao-ICD-A1.git
cd pix-maranhao-ICD-A1
cp .env.example .env 
uv sync
uv run python src/ingest.py
uv run quarto render analises/diagnostico.qmd
uv run python src/transform.py
uv run quarto render analises/exploracao.qmd
uv run python src/model.py
```

## O que cada etapa produz

- **`src/ingest.py`** — busca os 24 meses de 2024 e 2025, filtrados para o
  Maranhão, e grava um **JSON com envelope de proveniência** (fonte,
  endpoint, filtro, meses coletados, e a lista de registros sem alteração)
  em `data/raw/pix_dados_abertos/AAAA-MM-DD/pix_municipios_maranhao.json` —
  uma pasta por data de coleta, como pede a aula-3. Idempotente: se o
  arquivo de hoje já existe, a segunda execução não refaz a coleta. Se algum mês falhar, nada é gravado e o
  script sai com erro, para que uma coleta parcial não bloqueie a reexecução.
  Testado de verdade contra a API: **5.208 registros** (217 municípios × 24
  meses), coleta 24/24 meses sem falha.

- **`analises/diagnostico.qmd`** — lê a coleta mais recente de `data/raw/` e diagnostica o dado bruto em SQL (DuckDB): perfil com `SUMMARIZE`, duplicatas, ausências (tabela de decisão com a consulta de cada conclusão) e extremos de cada município contra a própria série. O dado veio sem nulos e sem duplicatas, mas 13 linhas de 9 municípios passam de 5 vezes a mediana do próprio município em alguma coluna. Elas foram mantidas e marcadas com `extremo_serie_municipio`, e mudam o ranking da pergunta analítica (2 municípios invertem o sinal da variação), então a Etapa 2 deve apresentar o resultado com e sem elas.

- **`src/transform.py`** — lê a coleta mais recente em `data/raw/` e aplica o contrato Pandera (`strict` e `coerce`), com o tipo e a faixa de cada coluna, a chave única (`AnoMes`, `Municipio_Ibge`) e duas regras de tabela entre colunas: o volume total do município-mês é maior que zero, e a quantidade de transações nunca é menor que a de pessoas distintas. Depois calcula o *saldo relativo* e grava `data/trusted/pix_maranhao_trusted.parquet`. A política é Fail-Stop: se qualquer linha violar o contrato, o pipeline para, mostra o resumo por motivo e não grava nada, porque um relatório feito sobre uma base desfalcada é pior que um pipeline parado. Na coleta atual ele aprovou as 5.208 linhas e rejeitou 0, mas isso é o resultado desta coleta e não uma garantia: a política vale enquanto a fonte se mantiver estável. A pasta `data/quarentena/` é criada pelo pipeline e fica vazia, porque no Fail-Stop nenhuma linha é desviada para ela.

- **`analises/exploracao.qmd`** — análise exploratória sobre o recorte de 2024 (guardando 2025 para a Etapa 3). Investiga 3 perguntas analíticas com formulação de hipóteses, varreduras univariada, bivariada e trivariada com controle por quartis de volume e participação de PJ, tabelas cruzadas e formulação da tabela de hipóteses a confirmar.

- **`src/model.py`** — lê a camada `trusted` e constrói o modelo dimensional (Star Schema) em DuckDB na camada `data/gold/`. Cria dimensões com chaves substitutas (`dim_municipio`), dimensão contínua de data (`dim_data`), fato atômica mensal (`fato_pix_mensal`) e fato agregada anual (`fato_pix_anual_municipio`). Valida chaves naturais por LEFT JOIN antes da gravação (Fail-Stop), confere 100% das métricas e grava os arquivos em Parquet.

### Tabelas em `data/gold/`

- **`dim_data.parquet`** (24 linhas): dimensão temporal contínua cobrindo 2024-01 a 2025-12, chave primária `data`, com atributos `ano`, `mes`, `nome_mes`, `trimestre` e `semestre`.
- **`dim_municipio.parquet`** (217 linhas): dimensão de municípios maranhenses com chave substituta `municipio_sk`, chave natural `codigo_ibge`, `nome_municipio`, `codigo_estado`, `sigla_estado`, `estado`, `sigla_regiao` e `regiao`.
- **`fato_pix_mensal.parquet`** (5.208 linhas): fato atômica no grão município-mês ligada a `dim_municipio` por `municipio_sk` e a `dim_data` por `data`, contendo medidas monetárias (`vl_*`), transações (`qt_*`), pessoas distintas (`qt_pes_*`) e `saldo_relativo`.
- **`fato_pix_anual_municipio.parquet`** (434 linhas): fato agregada no grão município-ano ligada a `municipio_sk`, contendo `vl_recebido_anual`, `vl_pago_anual`, `vl_total_anual`, `qt_total_transacoes` e `saldo_relativo_anual`.



## Uso de IA

ChatGPT foi usado para: pesquisa e comparação de fontes de dados
abertas; formulação e refinamento iterativo da pergunta analítica.
Gemini foi usado para: revisão do código e do projeto.