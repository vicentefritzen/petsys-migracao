# GUIA RÁPIDO - Migração PetSys

## 1. Configuração Inicial (apenas uma vez)

```bash
# Copiar arquivo de configuração
cp .env.example .env

# Editar .env com suas credenciais reais
nano .env  # ou use seu editor preferido

# Instalar dependências (se ainda não instalou)
pipenv install
```

## 2. Executar Migração

```bash
# Ativar ambiente
pipenv shell

# PASSO 1: Migrar clientes (PET_CLIENTE -> PESSOA)
python3 src/migrate.py --batch-size 500

# PASSO 2: Atualizar cidades via ViaCEP (fuzzy matching)
python3 src/update_cities.py
```

## 3. Testar antes (opcional)

```bash
# Dry-run da migração (não insere dados)
python3 src/migrate.py --dry-run

# Dry-run da atualização de cidades (não atualiza)
python3 src/update_cities.py --dry-run

# Testar algoritmo de fuzzy matching isoladamente
python3 src/test_fuzzy_matching.py
```

## Arquivos Criados

- ✅ `src/migrate.py` - Script principal de migração
- ✅ `src/update_cities.py` - Atualização de cidades via ViaCEP
- ✅ `src/db.py` - Helpers de conexão e controle
- ✅ `src/test_fuzzy_matching.py` - Teste do algoritmo fuzzy
- ✅ `.env.example` - Template de configuração
- ✅ `.gitignore` - Proteção para .env
- ✅ `README.md` - Documentação completa

## Características Principais

### Fuzzy Matching de Cidades
- Score mínimo configurável (padrão: 85%)
- **Preferência automática por Santa Catarina (SC)**
- Busca primeiro na UF do ViaCEP, depois em todo Brasil
- Tolerante a acentos e pequenos erros de digitação

### Rate Limiting ViaCEP
- Delay de 10 segundos a cada 10 registros
- Configurável via `.env`

### Tabela de Controle
- Criada automaticamente no primeiro run
- Mapeia IDs origem -> destino
- Útil para migrar tabelas relacionadas (ex: PET)

## Próximas Migrações

Para migrar outras tabelas (ex: PET), você pode:
1. Consultar a tabela `CONTROLE_MIGRACAO_LEGADO`
2. Usar o campo `sValorChaveDestino` para encontrar o novo ID da PESSOA
3. Criar um novo script seguindo o padrão de `migrate.py`
