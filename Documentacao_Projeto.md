# DOCUMENTAÇÃO DO PROJETO EMBRAPA API

## VISÃO GERAL DO PROJETO

O projeto Embrapa API visa criar um serviço robusto para acessar, estruturar e disponibilizar dados vitivinícolas 
do portal VitiBrasil da Embrapa Uva e Vinho. Esta API foi desenvolvida para fornecer acesso programático aos dados 
que anteriormente estavam disponíveis apenas em formato de tabelas HTML no website, tornando esses dados valiosos 
acessíveis para análises, pesquisas e aplicações de terceiros.

## ARQUITETURA DO SISTEMA

A arquitetura do sistema foi projetada seguindo os princípios de design modular, com os seguintes componentes principais:

1. **Scraper Adaptativo**: Responsável pela coleta de dados do portal VitiBrasil, adaptando-se a mudanças na estrutura HTML.
2. **Serviço de Dados**: Gerencia a obtenção, transformação e filtragem dos dados com mecanismos de fallback.
3. **Sistema de Cache**: Armazena dados previamente recuperados para reduzir requisições e melhorar a performance.
4. **API REST**: Expõe os endpoints para acesso externo aos dados processados.
5. **Sistema de Fallback em Cascata**: Garante a disponibilidade de dados mesmo quando ocorrem falhas.

## ETAPAS DE DESENVOLVIMENTO

### ETAPA 1: ANÁLISE DO PORTAL VITIBRASIL

- Estudamos a estrutura do portal VitiBrasil para entender como os dados estão organizados
- Identificamos os diferentes tipos de dados disponíveis (produção, processamento, comercialização, etc.)
- Mapeamos os parâmetros de URL e padrões de navegação para acessar diferentes conjuntos de dados
- Analisamos a estrutura HTML das tabelas e identificamos padrões e variações

### ETAPA 2: DESENVOLVIMENTO DO SCRAPER ADAPTATIVO

- Criamos a classe `AdaptiveScraper` para extrair dados do portal
- Implementamos detecção automática de mudanças na estrutura HTML usando hashes
- Desenvolvemos estratégias múltiplas de extração para lidar com diferentes formatos de tabela
- Implementamos mecanismos de retry e exponential backoff para lidar com falhas de conexão
- Adicionamos suporte para paginação e filtros por ano
- Incorporamos armazenamento do HTML bruto para recuperação em caso de falha no parsing

### ETAPA 3: SERVIÇO DE GESTÃO DE DADOS

- Desenvolvemos o `ViniDataService` para orquestrar a obtenção e transformação de dados
- Implementamos mapeamento inteligente entre tipos de produtos e subcategorias
- Criamos lógica de filtragem por região, tipo de produto, canal, origem e destino
- Adicionamos validação de dados para detectar estruturas malformadas ou inconsistentes
- Implementamos sanitização automática para formatos JSON e suporte a exportação para CSV e Parquet
- Implementamos tratamento de formatos numéricos regionais (vírgula como separador decimal)
- Desenvolvemos conversão automática de strings especiais como "-" para valores nulos

### ETAPA 4: SISTEMA DE CACHE E FALLBACK

- Criamos um sistema de cache para armazenar e reutilizar dados já recuperados
- Implementamos geração de chaves de cache baseadas em categorias e filtros
- Desenvolvemos um mecanismo de fallback em cascata com três níveis:
  1. Cache: Tenta recuperar dados do cache
  2. Online: Se não houver cache, busca dados online
  3. Arquivos Locais: Se a busca online falhar, utiliza arquivos CSV locais 
- Implementamos recuperação inteligente a partir de HTML bruto quando o parsing normal falha
- Adicionamos geração de logs detalhados para rastreamento de falhas

### ETAPA 5: DESENVOLVIMENTO DA API REST

- Projetamos endpoints REST para cada categoria de dados
- Implementamos validação de parâmetros e tratamento de erros
- Adicionamos suporte a filtragem, paginação e ordenação
- Criamos documentação OpenAPI para todos os endpoints
- Implementamos controle de acesso e autenticação básica
- Adicionamos monitoramento com métricas Prometheus

### ETAPA 6: TRATAMENTO DE CASOS ESPECIAIS

- Implementamos processamento especial para dados com formato numérico brasileiro (ex: "1.234.567,89")
- Criamos tratamento para valores ausentes representados como traços ("-")
- Desenvolvemos recuperação de dados a partir de HTML bruto para tabelas com estruturas complexas
- Implementamos detecção e adaptação a diferentes formatos de cabeçalho de tabela
- Adicionamos suporte a múltiplas estratégias de extração para lidar com diferentes layouts HTML

### ETAPA 7: TESTES E VALIDAÇÃO

- Desenvolvemos testes automatizados para validar a funcionalidade de fallback
- Criamos cenários de teste para verificar a recuperação a partir de HTML bruto
- Implementamos testes para validar o tratamento de formatos numéricos regionais
- Verificamos o comportamento do sistema com dados ausentes ou malformados
- Validamos a hierarquia de fallback (cache → online → arquivos locais)

### ETAPA 8: OTIMIZAÇÕES

- Implementamos armazenamento seletivo de HTML bruto apenas quando necessário
- Otimizamos o processo de scraping com timeouts e retries configuráveis
- Adicionamos headers de navegador para evitar bloqueios
- Melhoramos o algoritmo de extração de tabelas para lidar com estruturas complexas
- Implementamos limpeza de texto para remover quebras de linha e espaços duplicados

## COMPONENTES DO SISTEMA

### SCRAPER ADAPTATIVO (adaptive_scraper.py)

O scraper adaptativo é responsável por coletar dados do portal VitiBrasil e transformá-los em estruturas de dados utilizáveis.
Características principais:

- Detecção de mudanças de esquema via comparação de hashes
- Configuração de retries e backoff exponencial para falhas de conexão
- Múltiplas estratégias para extração de dados de tabelas
- Armazenamento de HTML bruto para recuperação de emergência
- Suporte a paginação e filtragem por diversos parâmetros

### SERVIÇO DE DADOS (data_service.py)

O serviço de dados gerencia a obtenção, validação e transformação de dados do VitiBrasil.
Características principais:

- Hierarquia de fallback em três níveis (cache, online, arquivos locais)
- Validação e recuperação de dados de HTML bruto
- Filtragem inteligente por diversos parâmetros (região, tipo, etc.)
- Sanitização de dados para serialização JSON
- Tratamento de formatos numéricos regionais e valores especiais

### SISTEMA DE CACHE (cache_service.py)

O sistema de cache armazena resultados de consultas anteriores para melhorar performance e resiliência.
Características principais:

- Cache em memória com chaves baseadas em parâmetros de consulta
- Invalidação automática de cache após tempo configurável
- Função de callback para obtenção de dados quando não encontrados no cache

### API REST (endpoints/*.py)

A API REST expõe os dados do VitiBrasil através de endpoints organizados por categoria.
Características principais:

- Endpoints para cada categoria principal (produção, processamento, etc.)
- Validação de parâmetros com Pydantic
- Tratamento de erros centralizado
- Suporte a filtragem, paginação e ordenação
- Documentação OpenAPI interativa

## MODELO DE DADOS

Os dados do VitiBrasil são estruturados em cinco categorias principais:

1. **Produção**: Dados sobre produção de uvas, vinhos e sucos
2. **Processamento**: Informações sobre o processamento de uvas
3. **Comercialização**: Dados de comercialização no mercado interno
4. **Importação**: Estatísticas de importação de produtos vitivinícolas
5. **Exportação**: Dados sobre exportações brasileiras do setor

## MECANISMOS DE FALLBACK

Uma das características mais importantes do sistema é seu mecanismo de fallback em cascata,
que garante a disponibilidade de dados mesmo em situações adversas:

1. **Cache**: Primeiro, o sistema tenta recuperar dados do cache
2. **Online**: Se não encontrar no cache, busca diretamente do portal VitiBrasil
3. **Recuperação de HTML**: Se o parsing normal falhar, tenta recuperar dados do HTML bruto
4. **Arquivos Locais**: Se todas as tentativas online falharem, utiliza arquivos CSV locais

## TRATAMENTO DE ERROS

O sistema implementa tratamento abrangente de erros em todos os níveis:

- Retries automáticos para falhas de conexão
- Validação de dados em múltiplos estágios
- Recuperação de dados a partir de HTML bruto
- Fallback para fontes alternativas quando necessário
- Logging detalhado para diagnóstico de problemas

## CONCLUSÃO

O projeto Embrapa API representa um avanço significativo na disponibilização de dados vitivinícolas
do Brasil, tornando-os acessíveis de forma programática e estruturada. A arquitetura robusta
com múltiplos níveis de fallback garante alta disponibilidade dos dados, mesmo quando
enfrenta desafios como mudanças na estrutura do portal ou problemas de conectividade.

A implementação de técnicas avançadas como scraping adaptativo, recuperação de HTML bruto
e tratamento de formatos numéricos regionais permite que o sistema lide com as especificidades
dos dados vitivinícolas brasileiros, fornecendo uma base sólida para análises e aplicações.