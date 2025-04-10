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
git clone https://github.com/seu-usuario/projetoembrapaapi.git
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

Desenvolvido como parte de um projeto de integração de dados com a Embrapa Vitivinicultura.

Dados originais disponíveis em: [VitiBrasil - Embrapa](http://vitibrasil.cnpuv.embrapa.br/)
