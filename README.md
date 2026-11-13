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
uv run quarto render diagnostico.qmd
uv run python src/transform.py
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

- **`diagnostico.qmd`** — lê a coleta mais recente de `data/raw/` e diagnostica o dado bruto em SQL (DuckDB): perfil com `SUMMARIZE`, duplicatas, ausências por coluna (tabela de decisão com a consulta que sustenta cada conclusão) e extremos de cada município contra a própria série. O dado veio sem nulos e sem duplicatas. Os dois casos mais extremos (Arame em jun/2025 e Santo Antônio dos Lopes em dez/2025) foram analisados e mantidos, com a conclusão e o efeito no saldo relativo descritos no notebook.

- **`src/transform.py`** — lê a coleta mais recente em `data/raw/` e aplica o contrato Pandera (`strict` e `coerce`), com o tipo e a faixa de cada coluna, a chave única (`AnoMes`, `Municipio_Ibge`) e duas regras de tabela entre colunas: o volume total do município-mês é maior que zero, e a quantidade de transações nunca é menor que a de pessoas distintas. Depois calcula o *saldo relativo* e grava `data/trusted/pix_maranhao_trusted.parquet`. A política é Fail-Stop: se qualquer linha violar o contrato, o pipeline para, mostra o resumo por motivo e não grava nada, porque um relatório feito sobre uma base desfalcada é pior que um pipeline parado. Na coleta atual ele aprovou as 5.208 linhas e rejeitou 0, mas isso é o resultado desta coleta e não uma garantia: a política vale enquanto a fonte se mantiver estável. A pasta `data/quarentena/` é criada pelo pipeline e fica vazia, porque no Fail-Stop nenhuma linha é desviada para ela.



## Uso de IA

ChatGPT foi usado para: pesquisa e comparação de fontes de dados
abertas; formulação e refinamento iterativo da pergunta analítica.
Gemini foi usado para: revisão do código e do projeto.