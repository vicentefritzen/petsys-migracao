# Migração PETSYS - PET_CLIENTE -> PESSOA

Este repositório contém scripts enxutos para migrar os registros da tabela legada `PET_CLIENTE` para a tabela `PESSOA` do novo sistema PetSys-Web.

## Resumo

- Lê os registros de `PET_CLIENTE` da base legada.
- Mapeia campos e insere em `PESSOA` na base destino.
- Registra o mapeamento origem->destino em uma tabela de controle (criada automaticamente): `CONTROLE_MIGRACAO_LEGADO` (ou `controle_migracao_legacy` em Postgres).
- Atualiza cidades consultando a API ViaCEP após a migração inicial.

## Preparação

1. Ative seu ambiente pipenv:

```bash
pipenv shell
pipenv install sqlalchemy python-dotenv requests rapidfuzz
```

2. Copie o arquivo de exemplo e configure suas credenciais:

```bash
cp .env.example .env
```

3. Edite o arquivo `.env` e preencha as conexões de banco de dados:

```bash
# Exemplo de configuração no .env
LEGACY_DB_URL=mssql+pyodbc://usuario:senha@servidor/PetSysLegado?driver=ODBC+Driver+17+for+SQL+Server
DEST_DB_URL=mssql+pyodbc://usuario:senha@servidor/PetSysWeb?driver=ODBC+Driver+17+for+SQL+Server
DEFAULT_TENANT=dfedd5f4-f30c-45ea-bc1e-695081d8415c
DEFAULT_CITY_ID=b6099443-d5c4-5e2c-8b53-4bd1c02b9793
```

## Como rodar

### 1. Migração inicial (PET_CLIENTE -> PESSOA)

```bash
# Dry-run (apenas mostra o que seria inserido, sem tocar o banco)
python3 src/migrate.py --dry-run

# Executar migração real
python3 src/migrate.py --batch-size 500
```

Parâmetros disponíveis:
- `--tenant UUID`: Define o tenant a ser usado (sobrescreve DEFAULT_TENANT do .env)
- `--batch-size N`: Tamanho do lote de leitura (padrão: 500)
- `--dry-run`: Modo simulação, não insere dados

### 2. Atualização de cidades via ViaCEP

Após a migração inicial, execute este comando para atualizar as cidades com base nos CEPs consultando a API ViaCEP:

```bash
# Dry-run (apenas mostra o que seria atualizado)
python3 src/update_cities.py --dry-run

# Executar atualização real
python3 src/update_cities.py
```

O script:
- Consulta todos os CEPs das pessoas migradas
- Busca informações na API ViaCEP (http://viacep.com.br/ws/{CEP}/json/)
- Localiza a cidade correspondente no banco destino usando **fuzzy matching** (nome + UF)
- Em caso de cidades com nomes similares, **prefere cidades de Santa Catarina (SC)**
- Atualiza o campo `sCdCidade` da pessoa
- **Respeita rate limiting**: delay de 10 segundos a cada 10 registros processados (configurável no `.env`)

**Fuzzy Matching de Cidades:**
- Usa a biblioteca `rapidfuzz` (ou `fuzzywuzzy` como fallback) para encontrar a cidade mais similar
- Score mínimo configurável (padrão: 85%)
- Busca primeiro na UF retornada pelo ViaCEP
- Se não encontrar boa correspondência, busca em todo o Brasil
- **Preferência por SC**: Em caso de matches com scores próximos, sempre prefere cidades de Santa Catarina

Parâmetros disponíveis:
- `--tenant UUID`: Define o tenant a ser usado
- `--dry-run`: Modo simulação, não atualiza dados

## Observações e decisões de design

- O script é intencionalmente pragmático e minimalista. Ele gera UUIDs para `sCdPessoa` e usa `DEFAULT_TENANT` se não for informado.
- Durante a migração inicial, todas as pessoas recebem `DEFAULT_CITY_ID` como cidade padrão.
- A tabela de controle é criada automaticamente. Ela guarda (origem,tabela,chave) -> (destino,tabela,chave) com data da migração.
- Campos obrigatórios do destino são preenchidos com valores razoáveis (string vazia, 0 ou UUID default) quando ausentes.
- A atualização de cidades via ViaCEP é um passo separado e opcional, executado após a migração inicial.
- O script `update_cities.py` usa **fuzzy matching** para encontrar cidades por nome, já que a tabela `CIDADE` possui apenas `sCdCidade`, `sNmCidade` e `sCdUf`.
- **Preferência por Santa Catarina**: Em caso de cidades com nomes similares em diferentes estados, o script prioriza SC.

## Configurações do .env

| Variável | Descrição | Exemplo |
|----------|-----------|---------|
| `LEGACY_DB_URL` | Connection string do banco legado | `mssql+pyodbc://...` |
| `DEST_DB_URL` | Connection string do banco destino | `mssql+pyodbc://...` |
| `DEFAULT_TENANT` | UUID do tenant padrão | `dfedd5f4-f30c-45ea-bc1e-695081d8415c` |
| `DEFAULT_CITY_ID` | UUID da cidade padrão para migração inicial | `b6099443-d5c4-5e2c-8b53-4bd1c02b9793` |
| `VIACEP_DELAY_SECONDS` | Segundos de espera após cada batch (padrão: 10) | `10` |
| `VIACEP_BATCH_SIZE` | Quantos registros processar antes do delay (padrão: 10) | `10` |
| `FUZZY_MIN_SCORE` | Score mínimo (0-100) para aceitar match de cidade (padrão: 85) | `85` |

## Próximos passos sugeridos

- Criar um mapeamento mais robusto de cidades se a tabela CIDADE tiver estrutura diferente
- Validar formatos de CPF/CNPJ e normalizar (remover pontuação) conforme regra do sistema destino
- Adicionar testes unitários e um pequeno runner que valide um subset de registros antes da migração em massa
- Implementar retry automático para falhas na API ViaCEP
- Adicionar logging estruturado em arquivo para auditoria

## Exemplo de execução do update_cities.py

```
Total de pessoas com CEP para processar: 150

[1/150] Processando pessoa 52a4f443-e259-41c7-a440-0068bd2a1085, CEP: 88010001
  ViaCEP: Florianópolis/SC
  Match fuzzy: 'Florianópolis' -> 'Florianópolis' (SC) [score: 100%]
  ✓ Cidade atualizada: e644a337-65ef-5745-bdb3-000faeef6736

[2/150] Processando pessoa a8f3c221-1234-5678-9abc-def012345678, CEP: 88100260
  ViaCEP: São José/SC
  Match fuzzy: 'São José' -> 'São José' (SC) [score: 100%]
  = Cidade já está correta

[3/150] Processando pessoa b1234567-89ab-cdef-0123-456789abcdef, CEP: 88108173
  ViaCEP: São José/SC
  Match fuzzy: 'São José' -> 'Sao Jose' (SC) [score: 95%]
  ⭐ Preferência SC: 'Sao Jose' [score: 95%]
  ✓ Cidade atualizada: e1ff3373-6bd8-5b4a-b6c1-002753b1e6e7

...

[10/150] Processando pessoa...
  💤 Aguardando 10 segundos...

============================================================
Processamento concluído!
Total processado: 150
Atualizados: 142
Erros/Não encontrados: 8
============================================================
```

