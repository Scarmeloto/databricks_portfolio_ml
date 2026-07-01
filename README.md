# Portfolio Mercado livre
# Mercado Livre ETL Data Pipeline: Lakehouse & Medallion Architecture 🚀

## 📌 Sobre o Projeto
Este projeto consiste no desenvolvimento de um pipeline de dados (ETL) robusto e de nível corporativo para a ingestão e estruturação de dados do ecossistema de e-commerce do **Mercado Livre**. O objetivo principal é consolidar informações de **Perfil do Vendedor, Vendas, Catálogo de Produtos e Distribuição Geográfica (Regiões)** em um ambiente analítico focado em tomada de decisão estratégica.

Como os dados reais de transações de terceiros são protegidos por tokens privados e regras rígidas de LGPD, o pipeline implementa adicionalmente um **mecanismo inteligente de simulação de payloads sintéticos complexos (Mock API Engine)**, garantindo um volume expressivo de dados para simular cenários reais de Big Data e volumetria de mercado.

---

## 🏗️ Arquitetura e Design do Data Lakehouse

O projeto adota o padrão de **Arquitetura Medalhão** integrado ao **Unity Catalog** do Databricks, garantindo governança, linhagem de dados e isolamento completo de ambientes.



### Camadas do Pipeline:
1. **Landing Zone (Raw Schema):** Os payloads brutos vindos da API (ou do simulador) são persistidos exatamente como foram recebidos, no formato **JSON String**. Isso isola o processo de extração do processo de transformação e garante auditoria total em caso de reprocessamento.
2. **Bronze Camada (Ingestion Schema):** O Spark consome os dados da Landing Zone, realiza um pré-processamento de compatibilidade de tipos nulos e strings complexas para ambientes Serverless, injeta metadados de auditoria (`dh_ingestao_etl`, `origem_dados`) e grava os dados em formato **Delta Lake**.
3. **Silver Camada (Em Desenvolvimento):** Lógica para desaninhar (*flattening* e *explode*) os nós complexos de JSON (como o nó `buyer` de clientes e `order_items` de produtos), aplicando chaves substitutas (Surrogate Keys) e particionamento.
4. **Gold Camada (Em Desenvolvimento):** Criação de tabelas fatos e dimensões (Modelagem Dimensional) prontas para consumo por ferramentas de BI (Power BI/Looker Studio) para responder dores de negócio (Ex: *Faturamento por Região*, *Churn de Clientes*).

---

## 🛠️ Tecnologias e Decisões Técnicas

* **Linguagem Principal:** Python 3 (padrão de mercado para scripts de engenharia).
* **Motor de Processamento:** **PySpark (SparkSession Estruturado)** otimizado especificamente para **Databricks Serverless Compute**. A arquitetura elimina dependências legadas de baixo nível como o `SparkContext (sc)`, operando de forma resiliente sob políticas rígidas de isolamento de cluster na nuvem.
* **Armazenamento:** **Delta Lake**, aproveitando recursos avançados como *Time Travel* (viagem no tempo), transações ACID e evolução automática de esquema (`mergeSchema`).
* **Governança:** Configurado via **Unity Catalog**, aplicando Tags de rastreabilidade corporativa (`env`, `project`, `data_governance`) e comentários automáticos para dicionário de dados.
* **Desacoplamento de Código (Best Practice):** O projeto foi dividido modularmente para evitar credenciais expostas (*hardcoded*):
  * `config_secrets`: Centraliza parâmetros de controle de fluxo, tokens e dicionário dinâmico de endpoints.
  * `pipeline_mercado_livre_bronze`: Contém puramente as instruções de execução do motor do Spark.

---

## 🧠 Complexidade Técnica Superada

* **Resiliência Serverless:** Tratamento avançado em PySpark utilizando loops iterativos com `spark.createDataFrame` para contornar restrições de serialização (`sc.parallelize`) comuns em runtimes Serverless modernos.
* **Abstração JSON Aninhada:** Preparação da base de dados tratando dicionários e arrays internos via conversão de tipos estruturados, mitigando problemas de quebra de esquema (*Schema Drift*) na camada Bronze.
* **Padronização Git Integration:** Versionamento do projeto utilizando o Databricks Git Folders integrado via tokens PAT (*Personal Access Tokens*) com escopo seguro de escrita no GitHub.

---

## 📁 Estrutura do Repositório

```text
├── notebooks/
│   ├── config_secrets.py                    # Configurações globais, catálogos e schemas
│   └── pipeline_mercado_livre_bronze.py     # Script principal de ETL (Landing -> Bronze)
├── README.md                                # Documentação do projeto
