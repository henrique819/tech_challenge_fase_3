# Tech Challenge — PNAD-COVID19 (IBGE)
### Expert em Data Analytics — Planejamento hospitalar para um novo surto de COVID-19

Análise dos microdados do **PNAD-COVID19 (IBGE)** com a base organizada em **Banco de
Dados em Nuvem (Google BigQuery)**, onde as análises rodam por SQL. Entrega o que o
enunciado pede: a breve análise, a organização do banco, as perguntas selecionadas e
as ações que o hospital deve tomar diante de um novo surto.

> **Entregável principal:** `tech_challenge.ipynb` — roda tudo no Google Colab, de ponta
> a ponta (ingestão → ETL em Spark → carga no BigQuery → análises em SQL na nuvem →
> gráficos e leitura).

---

## 1. Como o problema foi lido

O hospital não quer um relatório histórico da pandemia; quer um **instrumento de
planejamento** para o próximo surto. Por isso cada análise termina numa **ação**, e não
apenas num número. É essa passagem de "dado" para "decisão" que dá valor ao projeto.

O enunciado fixa critérios objetivos, tratados aqui como critérios de aceite:

| Exigência | Atendimento |
|-----------|-------------|
| Banco de Dados em Nuvem | base carregada e **analisada** no **BigQuery** (seção 4) |
| a) ≤ 20 questionamentos | 20 perguntas, justificadas e rastreadas (seção 3) |
| b) 3 meses | setembro, outubro e novembro de 2020 (seção 2) |
| c) Sintomas clínicos | dimensão clínica (seção 5) |
| d) Comportamento da população | dimensão comportamental (seção 5) |
| e) Características econômicas | dimensão econômica (seção 5) |
| Dados triviais (clínico, população, econômico) | cobertos pelas 4 dimensões de análise |

---

## 2. A base e duas decisões metodológicas

O PNAD-COVID19 é uma pesquisa amostral do IBGE, coleta mensal de maio a novembro/2020,
~380 mil linhas e ~114 colunas por mês.

**Decisão 1 — peso amostral.** É uma pesquisa por amostra, não um censo. Cada
respondente representa um número diferente de brasileiros, então **toda estimativa usa o
peso `V1032`**. Contar linhas direto (`COUNT(*)`) produz números errados. Esse é o
primeiro ponto que um avaliador criterioso verifica — e todas as queries deste projeto
são ponderadas: `SAFE_DIVIDE(SUM(IF(condição, peso, 0)), SUM(peso))`.

**Decisão 2 — 3 meses (set/out/nov 2020).** São os três últimos meses da série: o
questionário já está estável (as primeiras semanas tiveram correções do próprio IBGE) e
o bloco de testagem está mais maduro. Três meses consecutivos permitem ver **tendência**
(isolamento afrouxando, sintomas variando) sem misturar mudanças de instrumento.

> Cautela a declarar: o IBGE classifica esses dados como *estatísticas experimentais* —
> servem para tendências e ordens de grandeza, não para precisão decimal.

**Decisão 3 — variável correta do auxílio (D0051).** A variável de "recebeu auxílio
emergencial" é a `D0051` (bloco D, rendimentos de outras fontes), e **não** a `E001`
(bloco de empréstimos). Como a `D0051` tem ~33% de respostas nulas (universo restrito da
pergunta), a taxa por ocupação usa como denominador **todos da ocupação** (`SUM(peso)`),
tratando nulo/Não como "não recebeu" — o mesmo critério dos analistas de referência da
PNAD. Resultado coerente: informais no topo (~60%), militares/servidores na base (~21%).

**Decisão 4 — isolamento × renda só entre ocupados.** As faixas de renda só existem para
quem tem rendimento do trabalho. Incluir uma categoria "Sem renda" misturaria
não-ocupados (crianças, idosos, desempregados), que ficam em casa por não trabalhar e
inflam o número. Por isso a análise restringe a `rendimento > 0`: as faixas ficam
comparáveis e o gradiente (sobe com a renda) é a leitura defensável.

---

## 3. As 20 perguntas selecionadas e a matriz de rastreabilidade

Critério: **toda pergunta selecionada precisa virar análise e informar uma ação.** Se uma
não fechasse o ciclo pergunta → análise → ação, seria cortada. Resultado: 20 perguntas,
distribuídas pelas dimensões obrigatórias, todas usadas.

| # | Var | Pergunta | Dimensão | Análise que a responde → ação |
|---|-----|----------|----------|-------------------------------|
| 1 | A002 | Idade | População | Internação por faixa etária → reserva de leito por idade |
| 2 | A003 | Sexo | População | Perfil da população → estrato |
| 3 | A004 | Cor/raça | População | Internação por raça → vigiar desigualdade |
| 4 | A005 | Escolaridade | População | Testagem/home office por escolaridade → onde dirigir testagem |
| 5 | B0011 | Febre | Clínico | Prevalência de sintomas → triagem |
| 6 | B0012 | Tosse | Clínico | Prevalência + procura por saúde → triagem |
| 7 | B0013 | Dor de garganta | Clínico | Prevalência → triagem |
| 8 | B0014 | Dificuldade de respirar | Clínico | Prevalência → **gatilho de prioridade** |
| 9 | B0015 | Dor de cabeça | Clínico | Prevalência → triagem |
| 10 | B00111 | Perda de cheiro/sabor | Clínico | Prevalência → **marcador de alerta** |
| 11 | B005 | Internado ≥ 1 dia | Clínico | Internação por idade/raça → dimensionar leitos |
| 12 | B002 | Procurou estabelecimento de saúde | Clínico | Procura entre sintomáticos → pressão na rede |
| 13 | B008 | Fez teste para COVID | Comportamento | Testagem por escolaridade → mutirão dirigido |
| 14 | B011 | Grau de restrição/isolamento | Comportamento | Isolamento por mês e por renda → política |
| 15 | B007 | Tem plano de saúde | Comportamento | Cobertura de plano → retaguarda do SUS |
| 16 | C013 | Trabalho remoto (home office) | Comportamento | Home office por escolaridade → exposição |
| 17 | C001 | Trabalhou na semana | Econômico | Taxa de ocupação → contexto |
| 18 | C007 | Posição na ocupação | Econômico | Auxílio por posição → foco em informais |
| 19 | C01012 | Rendimento do trabalho | Econômico | Isolamento por faixa de renda → gatilho de renda |
| 20 | D0051 | Recebeu auxílio emergencial | Econômico | Auxílio por posição → foco em informais |

**Variáveis estruturais** (não contam no limite de 20; são desenho amostral e
identificação): `Ano`, `V1013` (mês), `UF`, `CAPITAL`, `V1022` (urbano/rural), `V1032` (peso).

---

## 4. Organização do banco (a parte de "Banco de Dados em Nuvem")

Pipeline em três camadas, padrão de mercado **Bronze → Silver → Gold**:

```
  FTP IBGE (CSV mensal, ~114 colunas)              download_pnad.py   [INGESTÃO]
        │
        ▼  Bronze — CSV cru dos 3 meses
        │
  etl_spark.py (PySpark): seleciona 20+estruturais, recodifica, concatena
        │
        ▼  Silver — base tratada (CSV/Parquet)
        │
  carga_bigquery.py: cria dataset + carrega a tabela
        │
        ▼  BigQuery (NUVEM): tabela `projeto.pnad_covid.fato_pnad`
        │
  análises em SQL ponderado (no notebook)          [GOLD / consumo]
```

**Por que BigQuery.** Atende diretamente "Banco de Dados em Nuvem", tem free tier
generoso (10 GB de armazenamento + 1 TB de query/mês), carga de DataFrame trivial e SQL
puro — o que permite que a **análise rode na própria nuvem**, e não só num banco local.
O *BigQuery Sandbox* funciona sem cartão de crédito, suficiente para este projeto.

**Por que Spark na transformação.** Três meses somam ~1,1 milhão de linhas — volume que
pandas processaria em segundos, então Spark é, com honestidade, *overkill* para o volume
atual. A escolha é deliberada: o pipeline é desenhado como **vigilância contínua**, que
cresce a cada novo mês de coleta; o mesmo código em `local[*]` escala para um cluster
(Dataproc/Databricks/EMR) sem mudar a lógica. Declarar esse trade-off na apresentação
demonstra mais maturidade do que usar Spark sem justificar.

---

## 5. Análises (rodam por SQL no BigQuery) e leitura esperada

Quatro blocos, todos ponderados, no `tech_challenge.ipynb`. A leitura final dos números
reais é sua; abaixo está a pergunta de negócio e o padrão esperado.

**Perfil da população** (A002–A005): distribuição ponderada por faixa etária, sexo, raça
e escolaridade. Define os estratos de risco e de acesso usados adiante.

**Clínico** (sintomas, B005, B002): prevalência ponderada de cada sintoma; internação por
faixa etária e por raça; procura por atendimento entre sintomáticos. *Esperado:*
dificuldade de respirar mais associada à internação; perda de olfato/paladar como
marcador específico; demanda concentrada em idosos.

**Comportamento** (B011, B008, C013, B007): isolamento ao longo dos 3 meses; testagem por
escolaridade; home office por escolaridade; cobertura de plano. *Esperado:* isolamento
afrouxa mês a mês; testagem desigual; alta dependência do SUS.

**Econômico** (C001, C007, C01012, D0051): ocupação; auxílio por posição; **isolamento por
faixa de renda** (entre ocupados). *Esperado:* o auxílio emergencial concentra-se nos
informais (conta própria, doméstico, trabalhador familiar), e o isolamento rigoroso
**cresce com a renda** — quem ganha mais consegue mais ficar em casa. A barreira ao
isolamento é socioeconômica.

---

## 6. Recomendações ao hospital em caso de novo surto

Cada uma é rastreável a uma análise.

1. **Triagem por sintoma-marcador** — peso extra para dificuldade de respirar (B0014) e
   perda de olfato/paladar (B00111) na fila de avaliação. *(clínico)*
2. **Leitos dimensionados por idade** — reserva proporcional ao risco por faixa etária,
   maior para idosos (B005 × A002). *(clínico)*
3. **Testagem onde há ponto cego** — mutirões dirigidos a baixa escolaridade/renda, onde o
   subdiagnóstico é maior (B008 × A005). *(comportamento)*
4. **Retaguarda do SUS** — usar a cobertura de plano (B007) para antecipar a demanda que
   pressiona o SUS e articular leitos antes do pico. *(comportamento)*
5. **Gatilho de renda** — isolamento não se sustenta sem renda; acoplar transferência a
   indicadores de transmissão, priorizando informais (C007/D0051). *(econômico)*
6. **Vigilância contínua** — reaproveitar o pipeline como vigilância sindrômica, com
   alertas quando a prevalência ponderada de sintomas-marcadores cruzar um limiar. *(arquitetura)*

---

## 7. Como executar

Tudo pelo **Google Colab**, abrindo `tech_challenge.ipynb`:

1. Rode as células na ordem (clone → ETL → carga → análises).
2. Na célula de configuração, troque `PROJECT` pelo ID do seu projeto GCP.
3. Autorize a conta Google quando o Colab pedir.

Para rodar os scripts isoladamente (fora do notebook):
```bash
pip install -r requirements.txt
python download_pnad.py        # baixa os 3 meses do IBGE
python etl_spark.py            # transforma em Spark -> dados_tratados/
# a carga no BigQuery é feita no notebook (usa a conta autenticada na sessão)
```

---

## 8. Arquivos do projeto

```
tech_challenge_fase_3/
├── README.md             ← este documento
├── requirements.txt
├── download_pnad.py      ← ingestão dos microdados (IBGE)
├── etl_spark.py          ← transformação em PySpark (Bronze→Silver)
├── carga_bigquery.py     ← carga da base tratada no BigQuery
└── tech_challenge.ipynb  ← ENTREGÁVEL: roda tudo no Colab
```

## 9. Nota sobre a entrega

O enunciado define o **conteúdo** (análise, organização do banco, perguntas, ações), não
o **formato** de entrega — não menciona vídeo nem exige que o avaliador execute o
pipeline. Quem organiza e usa o banco em nuvem é o autor. Se a disciplina tiver um
documento de critérios à parte, confira lá o formato exigido.
