# Mercado Livre ETL Data Pipeline: Lakehouse & Medallion Architecture 🚀

## 📌 Sobre o Projeto
Este projeto consiste no desenvolvimento de um pipeline de dados (ETL) robusto e de nível corporativo para a ingestão, estruturação e modelagem de dados do ecossistema de e-commerce do **Mercado Livre**. O objetivo principal é consolidar informações de **Perfil do Vendedor, Vendas, Catálogo de Produtos e Distribuição Geográfica (Regiões)** em um ambiente analítico altamente otimizado para tomada de decisão estratégica.

Como os dados reais de transações de terceiros são protegidos por tokens privados e regras rígidas de LGPD, o pipeline implementa adicionalmente um **mecanismo inteligente de simulação de payloads sintéticos complexos (Mock API Engine)**, garantindo um volume expressivo de dados para simular cenários reais de Big Data e volumetria de mercado.

---

## 🏗️ Arquitetura e Design do Data Lakehouse

O projeto adota o padrão de **Arquitetura Medallion** integrado ao **Unity Catalog** do Databricks, garantindo governança, linhagem de dados e isolamento completo de ambientes através de Schemas dedicados.