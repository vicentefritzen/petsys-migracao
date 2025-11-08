# Sistema de Migração PetSys

Sistema completo de migração de dados do banco legado PetSys para o novo sistema PetSys-Web.

## 📋 Índice

- [Visão Geral](#-visão-geral)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Instalação e Configuração](#-instalação-e-configuração)
- [Como Usar](#-como-usar)
- [Migrações Disponíveis](#-migrações-disponíveis)
- [Mapeamentos de Dados](#-mapeamentos-de-dados)
- [Funcionalidades Principais](#-funcionalidades-principais)
- [Exemplos de Execução](#-exemplos-de-execução)
- [Ferramentas de Teste](#-ferramentas-de-teste)
- [Criar Novas Migrações](#-criar-novas-migrações)
- [Troubleshooting](#-troubleshooting)
- [Changelog](#-changelog)

## 🎯 Visão Geral

Este repositório contém scripts para migrar dados do sistema legado PetSys para o novo PetSys-Web, incluindo:

- **Clientes**: PET_CLIENTE → PESSOA
- **Pets**: PET_ANIMAL → PET
- **Atualização de Endereços**: Via API ViaCEP
- **Tabela de Controle**: Rastreamento de mapeamentos origem→destino

### Características Principais

- ✅ **Validação de Duplicatas**: Verifica CPF/CNPJ antes de inserir
- ✅ **Update Automático**: Atualiza dados existentes ao invés de duplicar
- ✅ **Fuzzy Matching Inteligente**: Para raças, cores e cidades
- ✅ **Idempotente**: Pode executar múltiplas vezes sem problemas
- ✅ **Menu Interativo**: Interface amigável para escolher migrações
- ✅ **Dry-Run**: Modo simulação para testar antes de executar

## 📁 Estrutura do Projeto

```
petsys-migracao/
├── src/
│   ├── common/                      # Módulos compartilhados
│   │   ├── __init__.py
│   │   ├── db_utils.py              # Utilitários de banco de dados
│   │   └── fuzzy_utils.py           # Fuzzy matching para raças, cores, etc
│   │
│   ├── migrations/                  # Migrações organizadas por entidade
│   │   ├── __init__.py
│   │   ├── migrate_template.py      # Template para novas migrações
│   │   │
│   │   ├── clientes/                # Migração de clientes
│   │   │   ├── __init__.py
│   │   │   └── migrate_clientes.py  # PET_CLIENTE -> PESSOA
│   │   │
│   │   └── pets/                    # Migração de pets
│   │       ├── __init__.py
│   │       └── migrate_pets.py      # PET_ANIMAL -> PET
│   │
│   ├── tests/                       # Testes e análises
│   │   ├── __init__.py
│   │   ├── analyze_dest_pet.py
│   │   ├── analyze_legacy_pets.py
│   │   ├── test_connection.py
│   │   └── test_fuzzy_matching.py
│   │
│   ├── main.py                      # 🎯 Menu interativo principal
│   ├── migrate.py                   # Script legado de migração
│   ├── update_cities.py             # Atualização via ViaCEP
│   ├── db.py                        # Helpers de conexão
│   ├── show_credentials.py          # Exibir credenciais
│   └── limpar_*.py                  # Scripts de limpeza
│
├── logs/                            # Logs de execução
├── .env                             # Configurações (não versionado)
├── .env.example                     # Template de configuração
├── Pipfile                          # Dependências Python
└── README.md                        # Este arquivo
```

## 🚀 Instalação e Configuração

### 1. Instalar Dependências

```bash
# Instalar pipenv se não tiver
pip install pipenv

# Instalar dependências do projeto
pipenv install
```

### 2. Configurar Variáveis de Ambiente

```bash
# Copiar arquivo de exemplo
cp .env.example .env

# Editar com suas credenciais
nano .env  # ou use seu editor preferido
```

### 3. Configurar .env

```env
# Bancos de Dados
LEGACY_DB_URL=mssql+pyodbc://usuario:senha@servidor/PetSysLegado?driver=ODBC+Driver+17+for+SQL+Server
DEST_DB_URL=mssql+pyodbc://usuario:senha@servidor/PetSysWeb?driver=ODBC+Driver+17+for+SQL+Server

# Alternativa com pymssql (Azure SQL)
# LEGACY_DB_URL=mssql+pymssql://user:pass@server:1433/legacy_db
# DEST_DB_URL=mssql+pymssql://user:pass@server:1433/dest_db

# Tenant e Cidade Padrão
DEFAULT_TENANT=dfedd5f4-f30c-45ea-bc1e-695081d8415c
DEFAULT_CITY_ID=b6099443-d5c4-5e2c-8b53-4bd1c02b9793

# Configurações ViaCEP
VIACEP_DELAY_SECONDS=10       # Delay após cada batch
VIACEP_BATCH_SIZE=10          # Registros por batch
FUZZY_MIN_SCORE=85            # Score mínimo para match (0-100)
```

⚠️ **Senhas especiais**: Use URL encoding para caracteres especiais:
- `@` → `%40`
- `#` → `%23`
- `!` → `%21`

### 4. Testar Conexão

```bash
pipenv run python src/tests/test_connection.py
```

## 💻 Como Usar

### Menu Interativo (Recomendado)

```bash
# Ativar ambiente virtual
pipenv shell

# Executar menu principal
python src/main.py
```

Você verá:

```
==============================================================
  SISTEMA DE MIGRAÇÃO PETSYS
  Legado -> Web
==============================================================

Escolha a migração que deseja executar:

  1. Clientes (PET_CLIENTE -> PESSOA)
  2. Pets (PET_ANIMAL -> PET)
  3. Vacinas [EM BREVE]
  4. Atualizar Cidades via ViaCEP [EM BREVE]

  0. Sair

Opção: _
```

### ⚠️ Ordem de Execução Obrigatória

1. **Clientes** primeiro (cria registros de PESSOA)
2. **Pets** depois (requer proprietários migrados)
3. **Vacinas** por último (requer pets migrados)

### Execução Direta (Scripts Individuais)

```bash
# Migração de Clientes
python src/migrations/clientes/migrate_clientes.py --dry-run
python src/migrations/clientes/migrate_clientes.py --batch-size 500

# Migração de Pets
python src/migrations/pets/migrate_pets.py --dry-run
python src/migrations/pets/migrate_pets.py

# Atualização de Cidades/Endereços
python src/update_cities.py --dry-run
python src/update_cities.py
```

### Parâmetros Disponíveis

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| `--dry-run` | Modo simulação, não insere dados | `--dry-run` |
| `--batch-size N` | Tamanho do lote de leitura | `--batch-size 500` |
| `--tenant UUID` | Define tenant específico | `--tenant abc-123...` |

## 📊 Migrações Disponíveis

### 1. Clientes (PET_CLIENTE → PESSOA)

Migra dados de clientes do sistema legado para a tabela PESSOA.

**Características:**
- Valida CPF/CNPJ duplicados (chave única: documento + tenant)
- Atualiza registros existentes ao invés de duplicar
- Verifica/adiciona tipo CLIENTE em PESSOA_TIPO
- Define cidade padrão (configurável no .env)

**Campos migrados:**

| Origem (PET_CLIENTE) | Destino (PESSOA) | Transformação |
|----------------------|------------------|---------------|
| Codigo | - | Salvo em CONTROLE_MIGRACAO_LEGADO |
| Nome | sNmPessoa | Direto |
| Documento | sNrDoc ⭐ | Chave única (validação) |
| Email | sDsEmail | Direto |
| Telefone1 | sNrTelefone1 | Direto |
| Telefone2 | sNrTelefone2 | Direto |
| CEP | nNrCep | Conversão para inteiro |
| Endereco | sDsEndereco | Direto |
| Bairro | sNmBairro | Direto |
| Tipo (1=F) | sIdFisicaJuridica | 'F' ou 'J' |
| Ativo | bFlAtivo | Conversão boolean |
| - | sCdCidade | DEFAULT_CITY_ID |
| - | sCdTenant | DEFAULT_TENANT |

⭐ = Campo usado para validação de duplicatas

### 2. Pets (PET_ANIMAL → PET)

Migra dados de animais do sistema legado para a tabela PET.

**Características:**
- Busca proprietário via CONTROLE_MIGRACAO_LEGADO
- Fuzzy matching de raças (score mínimo 75%)
- Fuzzy matching de cores (score mínimo 70%)
- Descobre espécie automaticamente pela raça
- Pula pets sem proprietário válido migrado

**Campos migrados:**

| Origem (PET_ANIMAL) | Destino (PET) | Transformação |
|---------------------|---------------|---------------|
| Codigo | - | Salvo em CONTROLE_MIGRACAO_LEGADO |
| Nome | sNmPet | Direto |
| DataNascimento | tDtNascimento | Conversão date |
| Proprietario (FK) | sCdPessoa | Via tabela CONTROLE |
| Raca (FK) | nCdRaca | **Fuzzy match** 75% |
| Raca → Especie | nCdEspecie | Da raça (1=CAN, 2=FEL) |
| Cor (FK) | nCdCor | **Fuzzy match** 70% |
| Sexo | nCdSexo | Mapeamento (ver abaixo) |
| Porte | nCdPorte | Direto (1-4) |
| Ativo | bFlAtivo | Conversão boolean |
| Observacoes | sDsObservacoes | Trunca 500 chars |
| - | sCdTenant | DEFAULT_TENANT |

**Mapeamento de Sexo:**

```
Legado → Destino
1 (FÊMEA)          → 1 (FÊMEA)
2 (FÊMEA CASTRADA) → 3 (FÊMEA CASTRADA)
3 (MACHO)          → 2 (MACHO)
4 (MACHO CASTRADO) → 4 (MACHO CASTRADO)
```

**Fallbacks:**
- Raça não encontrada → S.R.D. (código 7 para CANINA, 33 para FELINA)
- Cor não encontrada → CARACTERISTICA (código 5)

### 3. Atualização de Endereços (ViaCEP)

Atualiza endereços consultando a API ViaCEP.

**Características:**
- Consulta CEP na API ViaCEP
- Fuzzy matching de cidades (score mínimo 85%)
- **Preferência por Santa Catarina (SC)**
- Atualiza: cidade, logradouro, bairro, complemento
- Rate limiting: 10s de delay a cada 10 registros

**Campos atualizados em PESSOA:**

| Campo | Origem |
|-------|--------|
| sCdCidade | Fuzzy match com CIDADE |
| sDsEndereco | Logradouro da API |
| sDsBairro | Bairro da API |
| sDsComplemento | Complemento da API |

**Fuzzy Matching de Cidades:**
- Usa biblioteca `rapidfuzz`
- Score mínimo: 85% (configurável)
- Busca primeiro na UF retornada pelo ViaCEP
- Se não encontrar, busca em todo o Brasil
- **Preferência SC**: Em scores próximos, escolhe cidade de SC

## 🎯 Funcionalidades Principais

### Validação de Duplicatas

O sistema verifica se um registro já foi migrado antes de inserir:

```python
# Verifica por CPF/CNPJ + Tenant
registro_existente = session.query(Pessoa).filter(
    Pessoa.sNrDoc == documento,
    Pessoa.sCdTenant == tenant
).first()

if registro_existente:
    # ATUALIZA registro existente
    registro_existente.sNmPessoa = novo_nome
    # ... outros campos
else:
    # INSERE novo registro
    nova_pessoa = Pessoa(...)
```

### Fuzzy Matching Inteligente

#### Raças (Score mínimo: 75%)

```python
# Exemplo: "YORK SHIRE" no legado
nCdRaca, raca_matched, score = buscar_raca_por_nome(
    dest_engine, 
    "YORK SHIRE",
    especie=1,  # CANINA
    min_score=75
)
# Resultado: (15, "YORK SHIRE", 100%)
```

#### Cores (Score mínimo: 70%)

```python
# Exemplo: "PRETO" no legado
nCdCor, cor_matched, score = buscar_cor_por_nome(
    dest_engine,
    "PRETO",
    min_score=70
)
# Resultado: (1, "PRETA", 95%)
```

### Tabela de Controle

Criada automaticamente no primeiro run: `CONTROLE_MIGRACAO_LEGADO`

| Campo | Descrição |
|-------|-----------|
| sNmTabelaOrigem | Nome da tabela origem (ex: PET_CLIENTE) |
| sValorChaveOrigem | ID do registro na origem |
| sNmTabelaDestino | Nome da tabela destino (ex: PESSOA) |
| sValorChaveDestino | UUID do registro no destino |
| tDtMigracao | Data/hora da migração |

Usado para:
- Rastrear mapeamentos origem→destino
- Encontrar proprietários de pets
- Evitar duplicatas
- Auditoria

## 📝 Exemplos de Execução

### Exemplo 1: Migração de Clientes (Dry-run)

```bash
$ pipenv run python src/main.py

Opção: 1

--------------------------------------------------------------
MIGRAÇÃO DE CLIENTES
--------------------------------------------------------------

Esta migração irá:
  • Ler registros de PET_CLIENTE (banco legado)
  • Validar CPF/CNPJ duplicados
  • Atualizar registros existentes ou inserir novos
  • Inserir em PESSOA (banco destino)

Executar em modo DRY-RUN primeiro? (s/n): s

→ Executando DRY-RUN...

[1/150] Processando: João Silva (CPF: 123.456.789-00)
  [dry-run] PESSOA: João Silva (joao@email.com)

[2/150] Processando: Maria Santos (CPF: 987.654.321-00)
  ✓ Documento já existe - seria ATUALIZADO
  [dry-run] PESSOA: Maria Santos (maria@email.com)

...

✓ Migração concluída! 150 registros processados.
  • 120 seriam inseridos
  • 30 seriam atualizados
```

### Exemplo 2: Migração de Pets

```bash
$ pipenv run python src/main.py

Opção: 2

--------------------------------------------------------------
MIGRAÇÃO DE PETS
--------------------------------------------------------------

⚠ IMPORTANTE: Execute migração de CLIENTES antes!

Executar em modo DRY-RUN primeiro? (s/n): n
Tamanho do batch (padrão 500): 500

→ Executando migração real...

[1/3031] Processando: BELINHA (Código: 7)
    Raça: 'YORK SHIRE' -> 'YORK SHIRE' (score: 100%)
    Cor: 'CARACTERISTICA' -> 'CARACTERISTICA' (score: 100%)
  ✓ Inserido: BELINHA (Proprietário: abc-123...)

[2/3031] Processando: TOTÓ (Código: 8)
    Raça: 'POODLE' -> 'POODLE' (score: 100%)
    Cor: 'PRETO' -> 'PRETA' (score: 95%)
  ✓ Inserido: TOTÓ (Proprietário: def-456...)

[3/3031] Processando: REX (Código: 9)
  ⚠ Pulado: sem proprietário válido migrado

...

============================================================
Migração concluída!
Total processado: 3031
Inseridos: 2900
Atualizados: 0
Pulados (sem proprietário): 131
============================================================
```

### Exemplo 3: Atualização de Cidades

```bash
$ pipenv run python src/update_cities.py

Total de pessoas com CEP para processar: 150

[1/150] Processando pessoa 52a4f443-e259-41c7-a440-0068bd2a1085, CEP: 88010001
  ViaCEP: Florianópolis/SC
    Logradouro: Praça 15 de Novembro
    Bairro: Centro
  Match fuzzy: 'Florianópolis' -> 'Florianópolis' (SC) [score: 100%]
  ✓ Cidade atualizada: e644a337-65ef-5745-bdb3-000faeef6736
  ✓ Endereço atualizado

[2/150] Processando pessoa a8f3c221-1234-5678-9abc-def012345678, CEP: 88100260
  ViaCEP: São José/SC
    Logradouro: Rua das Flores
    Bairro: Kobrasol
  Match fuzzy: 'São José' -> 'São José' (SC) [score: 100%]
  = Cidade já está correta
  ✓ Endereço atualizado

[10/150] Processando pessoa...
  💤 Aguardando 10 segundos...

...

============================================================
Processamento concluído!
Total processado: 150
Atualizados: 142
Erros/Não encontrados: 8
============================================================
```

## 🧪 Ferramentas de Teste

### Testar Conexões

```bash
pipenv run python src/tests/test_connection.py
```

Testa conectividade com ambos os bancos de dados.

### Analisar Estrutura Legacy

```bash
pipenv run python src/tests/analyze_legacy_pets.py
```

Analisa estrutura e dados da tabela `PET_ANIMAL` no banco legado.

### Analisar Estrutura Destino

```bash
pipenv run python src/tests/analyze_dest_pet.py
```

Analisa estrutura e dados da tabela `PET` no banco destino.

### Testar Fuzzy Matching

```bash
pipenv run python src/tests/test_fuzzy_matching.py
```

Testa algoritmo de fuzzy matching isoladamente.

## 🎓 Criar Novas Migrações

### 1. Copiar Template

```bash
# Exemplo: Migração de Vacinas
cp src/migrations/migrate_template.py src/migrations/vacinas/migrate_vacinas.py
touch src/migrations/vacinas/__init__.py
```

### 2. Adaptar Funções

Edite o arquivo copiado e implemente:

- `map_origem_to_destino()` - Mapeamento de campos
- `insert_or_update_destino()` - Lógica de inserção/atualização
- `migrate_entidade()` - Fluxo principal

### 3. Adicionar ao Menu

Edite `src/main.py` e adicione nova opção:

```python
def menu_vacinas():
    from src.migrations.vacinas.migrate_vacinas import migrate_vacinas
    # ... resto da função
```

## ❓ Troubleshooting

### Erro de Conexão

```
sqlalchemy.exc.OperationalError: (pyodbc.OperationalError)
```

**Solução:**
- Verifique credenciais no `.env`
- Teste conexão: `python src/tests/test_connection.py`
- Verifique firewall/VPN

### Erro de Duplicata

```
sqlalchemy.exc.IntegrityError: UNIQUE constraint failed
```

**Solução:**
- O sistema já trata isso automaticamente (modo update)
- Se persistir, limpe tabela de controle e re-execute

### Fuzzy Match Muito Baixo

```
⚠ Raça 'XYZ' não encontrada (score < 75%)
```

**Solução:**
- Reduza `min_score` no código
- Ou adicione a raça manualmente no banco destino
- Ou use fallback (S.R.D.)

### Rate Limit ViaCEP

```
HTTP 429 Too Many Requests
```

**Solução:**
- Aumente `VIACEP_DELAY_SECONDS` no `.env`
- Reduza `VIACEP_BATCH_SIZE` no `.env`

## 📈 Changelog

### [07/11/2025] - Melhorias de Validação e Atualização

**✅ Adicionado:**
- Validação de documentos duplicados
- Update automático para registros existentes
- Atualização completa de endereços via ViaCEP (logradouro, bairro, complemento)
- Validação de PESSOA_TIPO

**🔧 Modificado:**
- `insert_pessoa()` → `insert_or_update_pessoa()`
- `atualizar_cidade_pessoa()` → `atualizar_endereco_pessoa()`
- Template atualizado com padrão INSERT/UPDATE

**📊 Benefícios:**
- Idempotência: pode executar múltiplas vezes
- Sincronização: dados do legado sempre sobrescrevem destino
- Endereços completos: não apenas cidade

### [Data Anterior] - Migração de Pets Implementada

**✅ Adicionado:**
- Migração completa de PET_ANIMAL → PET
- Fuzzy matching de raças e cores
- Menu interativo
- Estrutura modular organizada
- Template para novas migrações

## 💡 Dicas e Boas Práticas

1. ✅ **Sempre execute DRY-RUN primeiro** para verificar o que será feito
2. ✅ **Migre na ordem correta**: Clientes → Pets → Vacinas
3. ✅ **Monitore logs** para raças/cores não encontradas
4. ✅ **Faça backup** antes de rodar em produção
5. ✅ **Use batch size adequado** (padrão 500) para otimizar performance
6. ✅ **Verifique tabela de controle** após cada migração
7. ✅ **Teste fuzzy matching** antes de aplicar em massa
8. ✅ **Configure rate limiting** apropriado para ViaCEP

## 📦 Dependências

```bash
pipenv install
```

Bibliotecas utilizadas:
- **SQLAlchemy** - ORM para banco de dados
- **pymssql** / **pyodbc** - Drivers SQL Server / Azure SQL
- **python-dotenv** - Gerenciamento de variáveis de ambiente
- **rapidfuzz** - Fuzzy matching de strings
- **requests** - Cliente HTTP para ViaCEP

## 📄 Licença

Este é um projeto interno para migração de dados.

## 👥 Suporte

Para dúvidas ou problemas, consulte os logs em `logs/` ou execute os scripts de teste em `src/tests/`.

---

**Última atualização**: 08/11/2025
