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
git clone <aqui vai a url do nosso repositório>
cd <pasta do projeto>
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
  arquivo de hoje já existe, a segunda execução não refaz a coleta.
  Testado de verdade contra a API: **5.208 registros** (217 municípios × 24
  meses), coleta 24/24 meses sem falha.

- **`src/transform.py`** — lê os JSONs da pasta raw, aplica o Contrato Pandera garantindo a tipagem das colunas financeiras e geográficas, e cria a métrica da Pergunta Analítica (o *saldo relativo*). Como política, adotou-se o Fail-Stop, visto que a base bruta foi provada 100% íntegra (0 nulos). O pipeline grava o arquivo final tipado e otimizado em `data/trusted/pix_maranhao_trusted.parquet`. A pasta `data/quarentena/` é recriada para cumprir o pipeline, mas permanece vazia.



## Uso de IA

ChatGPT foi usado para: pesquisa e comparação de fontes de dados
abertas; formulação e refinamento iterativo da pergunta analítica.
Gemini foi usado para: revisão do código e do projeto.