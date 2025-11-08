# Sistema de Migração PetSys - Legado para Web

Sistema completo de migração de dados do banco legado PetSys para o novo sistema PetSys-Web.

## 📁 Estrutura do Projeto

```
migracao-petsys/
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
│   │   └── __init__.py
│   │
│   ├── main.py                      # 🎯 Menu interativo principal
│   ├── update_cities.py             # Atualização via ViaCEP
│   └── test_connection.py           # Teste de conexão
│
├── .env                             # Configurações (não versionado)
├── Pipfile                          # Dependências Python
└── README.md                        # Este arquivo
```

## 🚀 Como Usar

### Executar Menu Interativo

```bash
pipenv run python src/main.py
```

### Opções do Menu

```
1. Clientes (PET_CLIENTE -> PESSOA)
2. Pets (PET_ANIMAL -> PET)
3. Vacinas [EM BREVE]
4. Atualizar Cidades via ViaCEP [EM BREVE]
0. Sair
```

### ⚠️ Ordem de Execução Obrigatória

1. **Clientes** primeiro
2. **Pets** depois (requer clientes migrados)
3. **Vacinas** por último

## 🎯 Funcionalidades Principais

### ✅ Validação de Duplicatas
- Verifica CPF/CNPJ antes de inserir
- Se existir, **atualiza** ao invés de duplicar
- **Idempotente**: pode executar múltiplas vezes

### ✅ Fuzzy Matching Inteligente
- **Raças**: Score mínimo 75% (fallback: S.R.D.)
- **Cores**: Score mínimo 70% (fallback: CARACTERISTICA)
- Logs detalhados de matching

### ✅ Tabela de Controle
- Registra todos os mapeamentos origem → destino
- Usado para encontrar proprietários dos pets
- Permite rastreabilidade completa

## 📊 Mapeamentos

### Clientes (PET_CLIENTE → PESSOA)

| Origem | Destino |
|--------|---------|
| Codigo | (controle) |
| Nome | sNmPessoa |
| Documento | sNrDoc ⭐ |
| Email | sDsEmail |
| Telefone1/2 | sNrTelefone1/2 |
| CEP | nNrCep |
| Endereco | sDsEndereco |
| Bairro | sNmBairro |
| Tipo (1=F) | sIdFisicaJuridica |
| Ativo | bFlAtivo |

⭐ = Chave única com tenant (validação de duplicata)

### Pets (PET_ANIMAL → PET)

| Origem | Destino | Observação |
|--------|---------|------------|
| Codigo | (controle) | |
| Nome | sNmPet | |
| DataNascimento | tDtNascimento | |
| Proprietario | sCdPessoa | Via tabela controle |
| Raca | nCdRaca | Fuzzy match 75% |
| Cor | nCdCor | Fuzzy match 70% |
| Sexo | nCdSexo | Mapeamento direto |
| Porte | nCdPorte | Mapeamento direto |
| Especie | nCdEspecie | Da raça (1=CAN, 2=FEL) |
| Ativo | bFlAtivo | |
| Observacoes | sDsObservacoes | Max 500 chars |

### Mapeamento de Sexo

```
Legado → Destino
1 (FEMEA)          → 1 (FÊMEA)
2 (FEMEA CASTRADA) → 3 (FÊMEA CASTRADA)
3 (MACHO)          → 2 (MACHO)
4 (MACHO CASTRADO) → 4 (MACHO CASTRADO)
```

## 📝 Exemplo de Execução

```bash
$ pipenv run python src/main.py

==============================================================
  SISTEMA DE MIGRAÇÃO PETSYS
  Legado -> Web
==============================================================

Escolha a migração: 2

--------------------------------------------------------------
MIGRAÇÃO DE PETS
--------------------------------------------------------------

Esta migração irá:
  • Ler registros de PET_ANIMAL (banco legado)
  • Buscar proprietário em PESSOA (via tabela de controle)
  • Fazer fuzzy matching de raças e cores
  • Inserir em PET (banco destino)

⚠ IMPORTANTE: Execute migração de CLIENTES antes!

Executar em modo DRY-RUN primeiro? (s/n): s

→ Executando DRY-RUN...

[1] Processando: BELINHA (Código: 7)
    Raça: 'YORK SHIRE' -> 'YORK SHIRE' (score: 100%)
    Cor: 'CARACTERISTICA' -> 'CARACTERISTICA' (score: 100%)
  [dry-run] PET: BELINHA (Proprietário: abc-123...)

[2] Processando: TOTÓ (Código: 8)
    Raça: 'POODLE' -> 'POODLE' (score: 100%)
    Cor: 'PRETO' -> 'PRETA' (score: 95%)
  [dry-run] PET: TOTÓ (Proprietário: def-456...)

...

✓ Migração concluída! 3031 registros processados.
```

## 🔧 Configuração (.env)

```env
# Bancos de Dados
LEGACY_DB_URL=mssql+pymssql://user:pass@server:1433/legacy_db
DEST_DB_URL=mssql+pymssql://user:pass@server:1433/dest_db

# Tenant e Cidade Padrão
DEFAULT_TENANT=dfedd5f4-f30c-45ea-bc1e-695081d8415c
DEFAULT_CITY_ID=b6099443-d5c4-5e2c-8b53-4bd1c02b9793
```

⚠️ **Senhas especiais**: Use URL encoding (`@`→`%40`, `#`→`%23`, `!`→`%21`)

## 🧪 Testar

```bash
# Testar conexões
pipenv run python src/test_connection.py

# Analisar estrutura legado
pipenv run python src/analyze_legacy_pets.py

# Analisar estrutura destino
pipenv run python src/analyze_dest_pet.py
```

## 📦 Dependências

```bash
pipenv install
```

- SQLAlchemy (ORM)
- pymssql (Azure SQL)
- python-dotenv (configuração)
- rapidfuzz (fuzzy matching)
- requests (ViaCEP)

## 💡 Dicas

1. ✅ Sempre execute **DRY-RUN** primeiro
2. ✅ Migre **Clientes** antes de **Pets**
3. ✅ Monitore logs para raças/cores não encontradas
4. ✅ Faça **backup** antes de rodar em produção
5. ✅ Use batch size adequado (padrão 500)

## 🎓 Criar Nova Migração

```bash
# 1. Copie o template
cp src/migrations/migrate_template.py src/migrations/vacinas/migrate_vacinas.py

# 2. Crie __init__.py
touch src/migrations/vacinas/__init__.py

# 3. Adapte as funções:
#    - map_origem_to_destino()
#    - insert_or_update_destino()
#    - migrate_entidade()

# 4. Adicione ao menu em main.py
```

## 📈 Status

- [x] Migração de Clientes (validação duplicata)
- [x] Migração de Pets (fuzzy matching)
- [x] Estrutura modular organizada
- [x] Menu interativo
- [x] Template para novas migrações
- [ ] Migração de Vacinas
- [ ] Integração ViaCEP no menu
- [ ] Relatórios detalhados
