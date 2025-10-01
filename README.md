# Tick Data Collector

Ferramenta auxiliar para Profit (Nelogica) e MetaTrader que lê dados de ticks de ativos, converte-os para o formato Renko e os armazena em um banco de dados.  

---

# Tick Data Collector

Auxiliary tool for Profit (Nelogica) and MetaTrader that reads tick data from assets, converts it to Renko format, and stores it in a database.


### Funcionalidades / Features

- Lê os dados mais recentes de ticks a partir de arquivos CSV presentes na pasta de input (input_data), obtidos através da plataforma Profit  
  Reads the latest tick data from CSV files in the input folder (input_data), obtained via the Profit platform
- Normaliza os dados em um formato padrão e ordena-os  
  Normalizes the data into a standard format and sorts it
- Processa-os para construir os tijolos Renko referentes às frequências pré-definidas  
  Processes the data to build the corresponding Renko bricks for predefined frequencies
- Concatena os novos dados ao histórico existente  
  Concatenates the new data to the existing historical dataset

## Estrutura do Projeto / Project Structure

RenkoForge/
├─ .venv/                  # Ambiente virtual (ignorado pelo Git)
├─ input_data/             # CSVs de entrada, extraídos do Profit
├─ output_data/            # Dados gerados (ignored by git)
├─ examples/               # CSVs de exemplo para testes
│   └─ SampleData_17-09-2025.csv
├─ .gitignore              # Arquivos e pastas ignorados pelo Git
├─ .env                    # Credenciais e configurações (não versionado)
├─ .env.example            # Estrutura de variáveis para outros desenvolvedores
├─ config.py               # Configurações do projeto e conexão com o banco de dados
├─ main.py                 # Script principal
├─ processing/             # Pacote com funções de processamento
│   ├─ process_data.py     # Funções de processamento e salvamento de dados
│   └─ renko.py            # Lógica Renko
├─ create_db.py            # Script para criação do banco de dados do projeto
└─ requirements.txt        # Dependências do projeto


---

## Pré-requisitos / Requirements
- Python 3.10+
- PostgreSQL
- Bibliotecas Python:
```bash
pip install -r requirements.txt
```
## Como Rodar / Usage 

### Instalar dependências 

    Certifique-se de estar no ambiente virtual do projeto.
```bash
python -m venv .venv
source .venv/bin/activate
```
    Rode:
```bash
pip install -r requirements.txt
```

Isso inclui todas as bibliotecas necessárias, como python-dotenv. 

### Configurar variáveis de ambiente Copie o arquivo .env.example para .env:

```bash
    cp .env.example .env  # ou copie manualmente
```

### Edite o arquivo .env com suas credenciais e configurações (usuario e senha do postgres) 

### Criar o banco de dados 

    Rode o script create_db.py para criar as tabelas iniciais no PostgreSQL, se ainda não existirem 

### Preparar os dados de exemplo 
    Copie os CSVs da pasta examples/ para input_data/ para testar o funcionamento do projeto.

```bash
    cp examples/*.csv input_data/  # ou copie manualmente
```

### Rodar o script principal (main.py)



## License
This project is licensed under the MIT License. See the LICENSE file for details.