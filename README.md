# Mercado Livre End-to-End ETL Data Pipeline: Lakehouse & Medallion Architecture 🚀

## 📌 Sobre o Projeto e Contexto de Negócio
Este projeto simula o desenvolvimento de uma plataforma de dados (*Data Lakehouse*) de nível corporativo voltada para o ecossistema de e-commerce do **Mercado Livre**. O objetivo principal é consolidar, tratar e modelar informações estratégicas de **Perfil do Vendedor, Transações de Vendas, Catálogo de Produtos e Distribuição Geográfica (Mapeamento de Regiões)** para subsidiar tomadas de decisão executivas, análises de cohort e inteligência de mercado (BI).

### O Desafio da Engenharia de Dados Real:
Em ambientes produtivos, os dados transacionais de e-commerce e os dados privados de clientes são protegidos por tokens de acesso restritos e conformidades rígidas de segurança (como a **LGPD**). Para superar essa barreira de forma realista, o projeto implementa uma **Engine de Simulação de Payloads Sintéticos Complexos (Mock API Engine)**. Essa engine gera volumetria massiva de Big Data mantendo o mesmo nível de aninhamento, variabilidade estrutural e complexidade de tipos de dados de uma API de produção real.

---

## 🏗️ Arquitetura e Design do Data Lakehouse

O projeto adota o padrão de **Arquitetura Medallion** totalmente integrado ao **Unity Catalog** do Databricks, garantindo governança centralizada, linhagem de dados estruturada de ponta a ponta e isolamento completo de ambientes analíticos através de Schemas segregados.



### Detalhamento Avançado das Camadas (Data Lifecycle):

1. **Landing Zone (`landing` schema):**
   * **Abordagem:** Os payloads brutos gerados pela API ou pela Engine de Simulação são persistidos no formato original de **JSON String**.
   * **Justificativa Técnica:** Seguir o padrão de *Immutable Storage* (Armazenamento Imutável). Isolar a fase de extração (I/O de rede) da fase de transformação garante resiliência. Em caso de falha de lógica nos estágios posteriores, não há necessidade de reonerar a API externa; basta reprocessar os dados a partir da Landing Zone.

2. **Camada Bronze (`bronze` schema - Ingestion):**
   * **Abordagem:** O Spark lê as strings JSON brutas da Landing Zone, injeta metadados técnicos obrigatórios de auditoria corporativa (`dh_ingestao_etl` e `origem_dados`) e converte os dados para tabelas físicas estruturadas sob o formato **Delta Lake**.
   * **Justificativa Técnica:** Esta camada garante a integridade dos tipos primitivos, resolve problemas comuns de compatibilidade com valores nulos em ambientes *Serverless* e prepara o ambiente para o motor de processamento distribuído.

3. **Camada Silver (`silver` schema - Trusted/Enrichment):**
   * **Abordagem:** É a camada de higienização, conformidade e transição para o modelo relacional. Os dados semiestruturados são desaninhados através de funções analíticas avanzadas. É aqui que nascem as **Surrogate Keys (SK)** geradas via algoritmos de hash. Os dados são salvos em formato puramente relacional em tabelas preparatórias (`trusted_clientes`, `trusted_produtos`, `trusted_status_venda`, `trusted_calendario` e `evento_vendas`).
   * **Justificativa Técnica:** Eliminar dados duplicados, padronizar strings (tratamento de aspas e caracteres especiais vindo de strings de texto da API) e quebrar a dependência de IDs naturais de sistemas de terceiros através das SKs.

4. **Camada Gold (`gold` schema - Analytical Consumption):**
   * **Abordagem:** O estágio final do Lakehouse, onde os dados foram estruturados utilizando a metodologia clássica de **Modelagem Dimensional (Star Schema / Modelo Estrela)** do Data Warehouse moderno. As tabelas são consolidadas em tabelas altamente otimizadas e prontas para consumo analítico:
     * `dim_clientes`: Cadastro mestre e unificado de clientes. Contém o ID natural, nome descritivo (nickname), dados cadastrais de tipo de documento (CPF/CNPJ) e segmentação geográfica por cidade e estado para análises regionais de vendas.
     * `dim_produtos`: Inventário e catálogo central com rastreabilidade de preços unitários históricos e volumetria, permitindo rastrear o comportamento de precificação da plataforma.
     * `dim_status_venda`: Centralização estável dos estados de ciclo de vida dos pedidos (`paid`, `delivered`, `shipped`), isolando colunas categóricas para otimizar filtros em relatórios.
     * `dim_calendario`: Dimensão temporal rica que converte registros cronológicos textuais da API em chaves numéricas inteligentes (`sk_calendario`), vital para análises de sazonalidade, faturamento mensal e *Time-Intelligence*.
     * `fato_vendas`: Tabela de fatos central e granularizada por item de pedido. Ela interliga todas as dimensões analíticas por meio puramente de chaves substitutas (`sk_`), armazenando métricas numéricas agregáveis brutas essenciais para o negócio (quantidade de itens, valor unitário transacionado e valor total acumulado por venda).

---

## 🛠️ Stack Tecnológica e Decisões de Engenharia

* **Linguagens Híbridas:** Desenvolvimento focado em **Python 3** (para scripts de orquestração, segurança e captura de metadados) e **Spark SQL** (padrão de mercado para transformações relacionais pesadas, DDLs corporativas e modelagem Star Schema).
* **Motor de Processamento:** **PySpark (SparkSession Estruturado)** otimizado nativamente para **Databricks Serverless Compute**. Toda a engenharia de código foi concebida para contornar restrições severas de runtimes Serverless modernos, eliminando o uso de chamadas de baixo nível depreciadas como o `SparkContext (sc.parallelize)` e aplicando padrões de loops estruturados com `spark.createDataFrame()`.
* **Mecanismo de Armazenamento:** **Delta Lake**, aproveitando propriedades ACID essenciais para evitar corrupção de dados durante cargas concorrentes, histórico de auditoria e auditoria temporal (*Time Travel*) e evolução controlada de esquema de tabelas via `.option("mergeSchema", "true")`.
* **Governança e Linhagem:** Implementado sobre o **Unity Catalog**, aplicando chaves corporativas de rastreabilidade via Tags (`env: dev`, `project: mercado_livre_analytics`, `data_governance: restricted_lgpd`) e comentários automáticos em catálogos para a criação automática de dicionários de dados visíveis para analistas e cientistas de dados.
* **Modularização e Segurança (Best Practices):** Abordagem desacoplada e limpa de código. O projeto separa os notebooks em responsabilidades bem definidas, impedindo a exposição de credenciais confidenciais (*hardcoded*):
  * `config_secrets`: Isolamento de credenciais, tokens, parâmetros de controle de ambiente e mapeamento de dicionários de endpoints.
  * `pipeline_mercado_livre_bronze`: Orquestração técnica pura do motor Spark (Landing Zone para Bronze).
  * `transform_mercado_livre_silver` & `modelagem_mercado_livre_gold`: Scripts declarativos SQL focados em regras de negócio.

---

## 🧠 Complexidades Técnicas Superadas (O que os Gestores buscam)

* **Modelagem Star Schema com Surrogate Keys:** Implementação de ponta a ponta de um modelo multidimensional dentro de um Lakehouse. O uso de chaves substitutas garante estabilidade nas consultas de BI e isola o data warehouse de mudanças bruscas ou deleções nos IDs originais vindo da API do Mercado Livre.
* **Desaninhamento de Listas Complexas (Padrão Explode):** APIs de e-commerce agrupam múltiplos produtos comprados em um único carrinho como uma lista interna (Array JSON) dentro do nó da venda. Para evitar distorções de cálculo em análises de produtos individuais, foi implementada a técnica avançada de `LATERAL VIEW explode` combinada com `from_json` via Spark SQL. Isso transformou coleções dinâmicas complexas em linhas atômicas individuais na `fato_vendas` com granularidade exata por item.
* **Parsing de Estruturas Dinâmicas sem Schema Drift:** Manipulação de caminhos textuais complexos e semiestruturados em strings Delta utilizando expressões regulares e funções analíticas nativas do Spark como `get_json_object` e `REPLACE(col, '"', '')`. Isso permitiu extrair metadados flutuantes de compradores sem forçar a quebra do esquema de tabelas principais, mitigando cenários de *Schema Drift*.

---

## 📁 Estrutura Organizada do Repositório

```text
├── notebooks/
│   ├── config_secrets.py                    # Configurações globais, catálogos, mapeamentos e segurança
│   ├── pipeline_mercado_livre_bronze.py     # Pipeline técnico e ingestão resiliente (Landing ➔ Bronze)
│   ├── transform_mercado_livre_silver.sql   # Higienização de dados, parsing JSON e criação de SKs (Bronze ➔ Silver)
│   └── modelagem_mercado_livre_gold.sql     # Estruturação e carga dimensional Star Schema (Silver ➔ Gold)
├── README.md                                # Documentação e memorial descritivo do projeto