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
- Implementamos controle de acesso e autenticação baseada em JWT
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
- Implementamos testes de integração para validar exportação em formatos CSV, JSON e Parquet

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
- Exportação otimizada para formatos CSV, JSON e Parquet

### SISTEMA DE CACHE (cache_service.py)

O sistema de cache armazena resultados de consultas anteriores para melhorar performance e resiliência.
Características principais:

- Cache em memória com chaves baseadas em parâmetros de consulta
- Invalidação automática de cache após tempo configurável
- Função de callback para obtenção de dados quando não encontrados no cache
- Configuração via variáveis de ambiente

### API REST (endpoints/*.py)

A API REST expõe os dados do VitiBrasil através de endpoints organizados por categoria.
Características principais:

- Endpoints para cada categoria principal (produção, processamento, etc.)
- Validação de parâmetros com Pydantic
- Tratamento de erros centralizado
- Suporte a filtragem, paginação e ordenação
- Documentação OpenAPI interativa
- Autenticação via JWT com controle de acesso por categoria

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

## IMPLANTAÇÃO E DEPLOYMENT

O projeto foi projetado para ser implantado de diversas formas, garantindo flexibilidade:

### Implantação com Docker

- Disponibilizamos um Dockerfile para criar uma imagem contendo todo o ambiente necessário
- Configurações como porta, níveis de log e parâmetros de cache são controlados via variáveis de ambiente
- O docker-compose.yml inclui configuração para um container de banco de dados (opcional)
- Suporte a volumes persistentes para armazenar cache e dados de fallback

### Implantação Serverless

- Adaptações para execução como funções AWS Lambda e Azure Functions
- Gerenciamento de estado via serviços de cache de nuvem (Redis, ElastiCache)
- Configuração para Auto-scaling baseado em demanda
- Integração com API Gateway para expor os endpoints
- Otimizações para reduzir cold start e tempo de resposta

### Implantação On-Premises

- Scripts de instalação para ambientes Linux e Windows
- Serviço systemd para Linux e serviço Windows para execução contínua
- Rotação de logs automática para gerenciamento de espaço
- Configuração para proxies corporativos
- Instruções para deployment em clusters de alta disponibilidade

## SEGURANÇA

Foram implementadas diversas medidas de segurança:

- Autenticação via API key ou JWT
- Limitação de taxa (rate limiting) por IP e por chave de API
- Validação rigorosa de parâmetros de entrada para prevenir injeções
- Sanitização de saída para prevenir XSS em contextos web
- Logs de auditoria para todas as requisições bem-sucedidas e falhas
- HTTPS obrigatório em produção
- Headers de segurança como CORS, CSP e X-Content-Type-Options

## EXEMPLOS DE USO

### Obter Dados de Produção de Vinho

```python
import requests

API_URL = "https://embrapa-api.exemplo.com/v1"
API_KEY = "sua_chave_api"

# Requisição de dados de produção de vinhos por estado
response = requests.get(
    f"{API_URL}/producao/vinho",
    params={
        "ano": 2020,
        "regiao": "Sul",
        "estado": "RS",
        "formato": "json"
    },
    headers={"X-API-Key": API_KEY}
)

dados = response.json()
print(f"Total de vinhos produzidos no RS em 2020: {dados['total']} litros")
```

### Comparar Importações e Exportações

```python
import requests
import pandas as pd
import matplotlib.pyplot as plt

API_URL = "https://embrapa-api.exemplo.com/v1"
API_KEY = "sua_chave_api"

# Requisição de dados de importação
imp_response = requests.get(
    f"{API_URL}/importacao",
    params={"ano_inicial": 2015, "ano_final": 2020, "produto": "vinho"},
    headers={"X-API-Key": API_KEY}
)

# Requisição de dados de exportação
exp_response = requests.get(
    f"{API_URL}/exportacao",
    params={"ano_inicial": 2015, "ano_final": 2020, "produto": "vinho"},
    headers={"X-API-Key": API_KEY}
)

# Criação de dataframes
imp_df = pd.DataFrame(imp_response.json()["dados"])
exp_df = pd.DataFrame(exp_response.json()["dados"])

# Visualização
plt.figure(figsize=(10, 6))
plt.plot(imp_df["ano"], imp_df["valor_total"], label="Importações")
plt.plot(exp_df["ano"], exp_df["valor_total"], label="Exportações")
plt.title("Balança Comercial de Vinhos (2015-2020)")
plt.xlabel("Ano")
plt.ylabel("Valor (USD)")
plt.legend()
plt.grid(True)
plt.show()
```

### Exportação para Formatos Analíticos

```python
import requests
import pandas as pd

API_URL = "https://embrapa-api.exemplo.com/v1"
API_KEY = "sua_chave_api"

# Requisição de dados de produção em formato Parquet
response = requests.get(
    f"{API_URL}/producao",
    params={
        "ano_inicial": 2010,
        "ano_final": 2022,
        "formato": "parquet"
    },
    headers={"X-API-Key": API_KEY}
)

# Salvar o arquivo Parquet
with open("producao_dados.parquet", "wb") as f:
    f.write(response.content)

# Carregar para análise
df = pd.read_parquet("producao_dados.parquet")
print(f"Número de registros: {len(df)}")
print(f"Colunas disponíveis: {df.columns.tolist()}")

# Análise agregada
print(df.groupby("ano").agg({"producao_total": "sum"}).reset_index())
```

## MONITORAMENTO E MÉTRICAS

O sistema implementa monitoramento abrangente utilizando o Prometheus para coleta de métricas. Conforme implementado no arquivo `main.py`, as seguintes métricas são coletadas:

- **REQUEST_COUNTER**: Contagem de requisições por endpoint e método HTTP
- **REQUEST_LATENCY**: Histograma de latência das requisições em segundos por endpoint
- **ERROR_COUNTER**: Contagem de erros por tipo de erro

O monitoramento é implementado através de:

- Um registro (registry) personalizado do Prometheus para evitar métricas duplicadas
- Middleware FastAPI que rastreia cada requisição e registra suas métricas
- Endpoint `/metrics` para expor as métricas coletadas em formato compatível com Prometheus

Exemplo de implementação no código:
```python
# Define Prometheus metrics with our custom registry
REQUEST_COUNTER = Counter('api_requests', 'Contagem de requisições', ['endpoint', 'method'], registry=CUSTOM_REGISTRY)
REQUEST_LATENCY = Histogram('api_request_latency_seconds', 'Latência das requisições em segundos', ['endpoint'], registry=CUSTOM_REGISTRY)
ERROR_COUNTER = Counter('api_errors', 'Erros por tipo', ['error_code'], registry=CUSTOM_REGISTRY)
```

Estas métricas podem ser utilizadas para:
- Monitorar o desempenho geral da API
- Identificar endpoints lentos ou com alta taxa de erros
- Acompanhar padrões de uso da API
- Alertar quando ocorrerem comportamentos anômalos

Para visualização das métricas, é recomendado o uso do Grafana com dashboards pré-configurados que incluem:
- Mapa de calor de latência por endpoint
- Taxa de requisições por minuto
- Taxa de erros por tipo
- Disponibilidade geral da API
- Uso de recursos (CPU, memória, rede)

## RESOLUÇÃO DE PROBLEMAS COMUNS

### Problemas de Dependências

```
pip install numpy==1.24.3
pip install pandas==2.1.0
pip install pyarrow==14.0.1
```

Caso ocorram problemas com incompatibilidade entre versões do NumPy, Pandas e PyArrow, recomenda-se instalar as versões específicas acima para garantir compatibilidade.

### Problemas de Acesso ao Portal VitiBrasil

Se o sistema estiver enfrentando dificuldades para acessar o portal VitiBrasil, verifique:

1. A conectividade com o domínio `vitibrasil.cnpuv.embrapa.br`
2. Os arquivos CSV locais para garantir que o fallback está funcionando
3. Os logs para identificar possíveis alterações na estrutura HTML
4. As configurações de proxy se estiver em ambiente corporativo

### Ajustes de Performance

Para melhorar a performance da API:

1. Aumente o tempo de vida do cache (TTL) na configuração
2. Aumente o número máximo de itens no cache
3. Utilize Parquet em vez de CSV para transferência de grandes volumes de dados
4. Implemente CDN para servir dados frequentemente solicitados

## ROADMAP E DESENVOLVIMENTOS FUTUROS

### Curto Prazo (3-6 meses)

- Adicionar suporte para exportação em formato Excel (.xlsx)
- Implementar endpoint de busca unificada entre todas as categorias
- Desenvolver webhooks para notificação de atualizações de dados
- Adicionar suporte a visualizações integradas via bibliotecas JavaScript
- Implementar sistema de sugestões de tendências baseado nos dados históricos

### Médio Prazo (6-12 meses)

- Criar APIs para conjuntos adicionais de dados da Embrapa
- Desenvolver integração com fontes de dados meteorológicos
- Implementar machine learning para previsão de tendências
- Adicionar suporte a GraphQL para consultas mais flexíveis
- Desenvolver SDK cliente para Python, JavaScript e R

### Longo Prazo (12+ meses)

- Expandir para outros setores agrícolas além da vitivinicultura
- Implementar análise de sentimento baseada em notícias do setor
- Desenvolver sistema de recomendação para produtores baseado em dados históricos
- Criar plataforma de visualização interativa integrada à API
- Implementar integração com sistemas IoT para dados em tempo real

## CONTRIBUIÇÕES

O projeto é aberto para contribuições da comunidade. Para contribuir:

1. Faça um fork do repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Implemente suas alterações com testes adequados
4. Envie um pull request com descrição detalhada das mudanças

Todas as contribuições passam por revisão de código e testes automatizados antes da integração.

## CONTATO E SUPORTE

- **Documentação Completa**: [https://embrapa-api.exemplo.com/docs](https://embrapa-api.exemplo.com/docs)
- **Repositório**: [https://github.com/embrapa/viti-api](https://github.com/embrapa/viti-api)
- **Suporte**: suporte@embrapa-api.exemplo.com
- **Comunidade**: [https://discourse.embrapa-api.exemplo.com](https://discourse.embrapa-api.exemplo.com)

---

© 2025 Embrapa Uva e Vinho. Todos os direitos reservados.