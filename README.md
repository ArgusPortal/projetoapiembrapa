# Embrapa Vitivinicultura API

API para acesso estruturado aos dados do portal VitiBrasil da Embrapa Vitivinicultura.

## Sobre o Projeto

Esta API permite o acesso programático aos dados vitivinícolas do portal VitiBrasil da Embrapa, cobrindo informações sobre produção, processamento industrial, comercialização no mercado interno, importação e exportação de vinhos, uvas e derivados no Brasil.

### Características Principais

- **Categorias de Dados**: Produção, Processamento, Comercialização, Importação e Exportação
- **Formatos de Resposta**: JSON, CSV, Parquet (para análises e ML)
- **Filtros Avançados**: Por região, tipo de produto, período temporal, etc.
- **Cache Inteligente**: Redução de carga no servidor original e resposta mais rápida
- **Autenticação**: Sistema JWT para controle de acesso
- **Monitoramento**: Prometheus para métricas de utilização

## Requisitos

- Python 3.8+
- FastAPI
- Pydantic 2.x
- Pandas
- PyArrow
- BeautifulSoup4
- Requests

## Instalação

### 1. Clone o repositório

```bash
git clone https://github.com/argusportal/projetoembrapaapi.git
cd projetoembrapaapi
```

### 2. Crie um ambiente virtual

```bash
python -m venv .venv
```

### 3. Ative o ambiente virtual

#### Windows
```bash
.venv\Scripts\activate
```

#### Linux/Mac
```bash
source .venv/bin/activate
```

### 4. Instale as dependências

```bash
pip install -r requirements.txt
```

## Configuração

As configurações da aplicação estão disponíveis no arquivo `app/core/config.py`. Você pode ajustar:

- Configuração da conexão com o portal VitiBrasil
- Parâmetros de cache
- Configurações de segurança
- Limites de rate limiting

## Executando a API

```bash
python main.py
```

A aplicação estará disponível em `http://localhost:8000`.

## Documentação da API

A documentação completa está disponível em:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## Autenticação

### Obter Token de Acesso

```
POST /api/auth/login
```

Credenciais disponíveis para teste:

- **Analista**: 
  - Email: `analyst@embrapa.br`
  - Senha: `password123`
  - Permissões: Produção e Comercialização

- **Pesquisador**: 
  - Email: `researcher@embrapa.br`
  - Senha: `research2023`
  - Permissões: Todas as categorias e exportação

### Renovar Token

```
POST /api/auth/refresh?token=seu_token_atual
```

## Endpoints Principais

### Produção de Vinhos e Uvas

```
GET /api/producao/
```

Parâmetros:
- `start_year` (int): Ano inicial (min: 1970)
- `end_year` (int): Ano final (max: 2025)
- `produto` (string, opcional): Filtro por tipo de produto
- `regiao` (string, opcional): Filtro por região geográfica
- `format` (string, opcional): Formato da resposta (json, csv, parquet)

### Processamento Industrial

```
GET /api/processamento/
```

Parâmetros:
- `start_year` (int): Ano inicial
- `end_year` (int): Ano final
- `tipo` (string, opcional): Tipo de processamento
- `regiao` (string, opcional): Filtro por região geográfica
- `format` (string, opcional): Formato da resposta

### Comercialização Interna

```
GET /api/comercializacao/
```

Parâmetros:
- `start_year` (int): Ano inicial
- `end_year` (int): Ano final
- `canal` (string, opcional): Canal de comercialização
- `produto` (string, opcional): Tipo de produto
- `format` (string, opcional): Formato da resposta

### Exportação

```
GET /api/exportacao/
```

Parâmetros:
- `start_year` (int): Ano inicial
- `end_year` (int): Ano final
- `produto` (string, opcional): Tipo de produto
- `destino` (string, opcional): País/região de destino
- `format` (string, opcional): Formato da resposta

### Importação

```
GET /api/importacao/
```

Parâmetros:
- `start_year` (int): Ano inicial
- `end_year` (int): Ano final
- `produto` (string, opcional): Tipo de produto
- `origem` (string, opcional): País/região de origem
- `format` (string, opcional): Formato da resposta

## Exemplos de Uso

### Obter Dados de Produção de Viníferas (2020-2023)

```bash
curl -X 'GET' \
  'http://localhost:8000/api/producao/?start_year=2020&end_year=2023&produto=vinifera' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer seu_token_aqui'
```

### Exportar Dados de Comercialização para CSV

```bash
curl -X 'GET' \
  'http://localhost:8000/api/comercializacao/?start_year=2020&end_year=2023&format=csv' \
  -H 'accept: text/csv' \
  -H 'Authorization: Bearer seu_token_aqui' \
  --output comercializacao.csv
```

## Desafios Técnicos e Soluções Implementadas

Durante o desenvolvimento da API, nos deparamos com diversos desafios técnicos relacionados ao processamento e tratamento dos dados do portal VitiBrasil. Abaixo estão os principais problemas encontrados e as soluções implementadas:

### 1. Extração de Dados do Portal Original

**Desafios:**
- Estrutura HTML inconsistente entre diferentes seções do portal
- Alterações frequentes no layout da página e na estrutura das tabelas
- Ausência de API oficial ou endpoints estruturados

**Soluções:**
- Implementação do `AdaptiveScraper` com detecção de mudanças de schema usando hash MD5
- Sistema de retentativas com backoff exponencial para lidar com instabilidades
- Múltiplas estratégias de extração de tabelas baseadas na estrutura detectada

### 2. Problemas nos Formatos de Exportação

**Desafios CSV:**
- Duplicação de registros nos dados exportados
- Presença de colunas genéricas (`column_0`) não identificadas corretamente
- Metadados (copyright, links de navegação) misturados com dados reais
- Células vazias excessivas dificultando a análise

**Soluções CSV:**
- Implementação do método `_clean_data_for_export()` para remover duplicatas
- Filtro de linhas contendo elementos de navegação ("DOWNLOAD", "TOPO", "« ‹ › »")
- Remoção automática de colunas de metadados e informações de copyright
- Limpeza de células vazias e estruturação consistente de dados

### 3. Problemas nos Dados em Formato JSON

**Desafios JSON:**
- Estrutura inconsistente de chaves e valores
- Inclusão de elementos HTML dentro dos valores de texto
- Dados numéricos codificados como strings, dificultando análises
- Presença de valores especiais (NaN, Infinity) incompatíveis com JSON

**Soluções JSON:**
- Criação do método `_sanitize_for_json()` com limpeza profunda de estrutura
- Conversão automática de strings numéricas para tipos numéricos apropriados
- Tratamento especial para valores NaN, infinito e tipos NumPy
- Remoção de chaves e valores redundantes ou irrelevantes

### 4. Otimização do Formato Parquet

**Desafios Parquet:**
- Definição inadequada de tipos de dados comprometendo eficiência de armazenamento
- Formato de números com vírgula como separador decimal (padrão brasileiro)
- Esquema inconsistente entre diferentes exportações

**Soluções Parquet:**
- Implementação da detecção inteligente de tipos numéricos
- Conversão automática de números em formato europeu (vírgula como separador decimal)
- Adição da compressão Snappy para reduzir o tamanho dos arquivos
- Otimização de esquemas para melhor desempenho em consultas analíticas

### 5. Sistema de Resilience e Fallback

**Desafios:**
- Instabilidade da fonte de dados original
- Tempos de resposta imprevisíveis do portal VitiBrasil
- Necessidade de disponibilidade contínua da API mesmo com falhas na fonte

**Soluções:**
- Implementação do `ResilientCache` com múltiplas camadas (memória, disco)
- Sistema de fallback para arquivos locais quando a fonte online falha
- Recuperação inteligente de dados a partir de HTML malformado
- Validação e sanitização de dados em cada camada do sistema

### 6. Dados Históricos Incompletos

**Desafios:**
- Dados faltantes em períodos específicos
- Inconsistências nos nomes dos produtos ao longo do tempo
- Alterações na metodologia de coleta ao longo dos anos

**Soluções:**
- Normalização de nomes de produtos e categorias
- Preenchimento inteligente de períodos faltantes com dado histórico mais próximo
- Adição de metadados para identificar a fonte e confiabilidade dos dados

Estas soluções implementadas garantem um acesso confiável e estruturado aos dados vitivinícolas, mesmo diante das inconsistências da fonte original, proporcionando uma base sólida para análises e visualizações.

## Resolução de Problemas

### Dependências

Se você encontrar erros de compatibilidade com NumPy, tente:

```bash
pip install numpy==1.24.3
pip install pandas==2.1.0
pip install pyarrow==14.0.1
```

### Email Validator

Se encontrar o erro "No module named 'email_validator'", instale:

```bash
pip install email-validator
```

## Contribuição

Contribuições são bem-vindas! Por favor, sinta-se à vontade para enviar pull requests ou abrir issues para melhorias.

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para detalhes.

## Créditos

Dados originais disponíveis em: [VitiBrasil - Embrapa](http://vitibrasil.cnpuv.embrapa.br/)
