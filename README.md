# Mercado Livre ETL Data Pipeline: Lakehouse & Medallion Architecture 🚀

## 📌 Sobre o Projeto
Este projeto consiste no desenvolvimento de um pipeline de dados (ETL) robusto e de nível corporativo para a ingestão, estruturação e modelagem de dados do ecossistema de e-commerce do **Mercado Livre**. O objetivo principal é consolidar informações de **Perfil do Vendedor, Vendas, Catálogo de Produtos e Distribuição Geográfica (Regiões)** em um ambiente analítico altamente otimizado para tomada de decisão estratégica.

Como os dados reais de transações de terceiros são protegidos por tokens privados e regras rígidas de LGPD, o pipeline implementa adicionalmente um **mecanismo inteligente de simulação de payloads sintéticos complexos (Mock API Engine)**, garantindo um volume expressivo de dados para simular cenários reais de Big Data e volumetria de mercado.

---

## 🏗️ Arquitetura e Design do Data Lakehouse

O projeto adota o padrão de **Arquitetura Medallion** integrado ao **Unity Catalog** do Databricks, garantindo governança, linhagem de dados e isolamento completo de ambientes através de Schemas dedicados.

[API / Mock] ──> [landing.schema (.json)] ──> [bronze.schema (Delta)] ──> [silver.schema (Star Schema SQL)]


### Detalhamento das Camadas:

1. **Landing Zone (`landing` schema):** Os payloads brutos vindos da API (ou do simulador) são persistidos exatamente como foram recebidos, no formato **JSON String**. Isso isola o processo de extração do de transformação e garante auditoria total em caso de reprocessamento.
2. **Camada Bronze (`bronze` schema):** O Spark consome os dados da Landing Zone, realiza um pré-processamento de compatibilidade de tipos nulos e strings complexas para ambientes Serverless, injeta metadados de auditoria (`dh_ingestao_etl`, `origem_dados`) e grava os dados em tabelas formatadas em **Delta Lake**.
3. **Camada Silver (`silver` schema):** É a camada onde ocorre a **normalização e a transição para o modelo relacional (Star Schema)** utilizando Spark SQL. Os dados semiestruturados (JSON strings) da Bronze são processados e divididos em tabelas limpas e tipadas:
   * `dim_clientes`: Consolida o ID do comprador, nickname e as informações de **região geográfica** (Estado e Cidade) extraídas do endereço de entrega.
   * `dim_produtos`: Catálogo centralizado dos itens e produtos únicos.
   * `fato_vendas`: Tabela de movimento contendo as métricas de quantidade, valores unitários, totais, status e chaves estrangeiras.
4. **Camada Gold (`gold` schema - Em Desenvolvimento):** Criação de visões agregadas e cubos de negócios prontos para consumo por ferramentas de BI (Power BI/Looker Studio) para responder dores de negócio (Ex: *Faturamento por Região*, *Ticket Médio por Estado*).

---

## 🛠️ Tecnologias e Decisões Técnicas

* **Linguagens:** Python 3 (Extração e Orquestração) e **Spark SQL** (Transformação Relacional).
* **Motor de Processamento:** **PySpark (SparkSession Estruturado)** otimizado especificamente para **Databricks Serverless Compute**. A arquitetura elimina dependências legadas de baixo nível como o `SparkContext (sc)`, operando de forma resiliente sob políticas rígidas de isolamento de cluster na nuvem.
* **Armazenamento:** **Delta Lake**, aproveitando recursos avançados como *Time Travel* (viagem no tempo), transações ACID e evolução automática de esquema (`mergeSchema`).
* **Governança:** Configurado via **Unity Catalog**, aplicando Tags de rastreabilidade corporativa (`env`, `project`, `data_governance`) e comentários automáticos para dicionário de dados.
* **Desacoplamento de Código (Best Practice):** O projeto foi dividido modularmente para evitar credenciais expostas (*hardcoded*):
  * `config_secrets`: Centraliza parâmetros de controle de fluxo, tokens e dicionário dinâmico de endpoints.
  * `pipeline_mercado_livre_bronze`: Contém puramente as instruções de execução do motor do Spark.

---

## 🧠 Complexidade Técnica Superada

* **Parsing de JSON Semiestruturado em SQL:** Utilização da função `get_json_object` na camada Silver para ler caminhos complexos dentro de colunas strings, permitindo extrair dados aninhados como os metadados de clientes (`buyer`) sem precisar quebrar o esquema da tabela.
* **Granularidade com Padrão Explode (Tratamento de Arrays):** Pedidos de e-commerce podem conter múltiplos produtos em uma lista interna. Para garantir a granularidade correta na `fato_vendas`, foi implementado o padrão `LATERAL VIEW explode` combinado com `from_json` via Spark SQL, transformando arrays complexos em linhas individuais por item comprado.
* **Resiliência Serverless:** Tratamento avançado em PySpark utilizando loops iterativos com `spark.createDataFrame` para contornar restrições de serialização (`sc.parallelize`) comuns em runtimes Serverless modernos.

---

## 📁 Estrutura do Repositório

```text
├── notebooks/
│   ├── config_secrets.py                    # Configurações globais, catálogos e schemas
│   ├── pipeline_mercado_livre_bronze.py     # Script de ETL (Landing -> Bronze)
│   └── transform_mercado_livre_silver.sql   # Normalização e Modelagem Dimensional (Bronze -> Silver)
├── README.md                                # Documentação do projeto