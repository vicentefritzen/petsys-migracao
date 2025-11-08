# 🎉 Migração de Pets - IMPLEMENTADA!

## ✅ O que foi feito

### 1. **Reorganização completa do projeto**

```
src/
├── main.py                          # ⭐ Menu interativo principal
├── common/                          # Módulos compartilhados
│   ├── db_utils.py                 # Conexões e tabela de controle
│   └── fuzzy_utils.py              # Fuzzy matching (raças, cores)
├── migrations/
│   ├── migrate_template.py         # Template para novas migrações
│   ├── clientes/
│   │   └── migrate_clientes.py     # Migração de clientes
│   └── pets/
│       └── migrate_pets.py         # ⭐ Migração de pets (NOVO!)
└── tests/                          # Scripts de análise
    ├── analyze_dest_pet.py
    ├── analyze_legacy_pets.py
    ├── test_connection.py
    └── test_fuzzy_matching.py
```

### 2. **Migração de Pets completa**

✅ **Arquivo:** `src/migrations/pets/migrate_pets.py`

**Funcionalidades:**
- Lê `PET_ANIMAL` do banco legado (3031 registros)
- Busca proprietário via `CONTROLE_MIGRACAO_LEGADO`
- **Fuzzy matching de raças** (score mínimo 75%)
  - Consulta `PET_RACA` no legado
  - Faz match com `RACA` no destino
  - Usa S.R.D. (código 7/33) se não encontrar
- **Fuzzy matching de cores** (score mínimo 70%)
  - Consulta `PET_COR` no legado
  - Faz match com `COR` no destino
  - Usa CARACTERISTICA (código 5) se não encontrar
- **Mapeamento de sexo** (1→1, 2→3, 3→2, 4→4)
- **Mapeamento de porte** (direto 1-4)
- **Descobre espécie** pela raça (1=CANINA, 2=FELINA)
- Insere/atualiza em `PET` no destino
- ⚠️ Pula pets sem proprietário migrado

### 3. **Fuzzy matching inteligente**

✅ **Arquivo:** `src/common/fuzzy_utils.py`

**Funções:**
- `fuzzy_match()` - Matching genérico com rapidfuzz
- `buscar_raca_por_nome()` - Busca raça por nome + espécie
- `buscar_cor_por_nome()` - Busca cor por nome
- `mapear_sexo()` - Converte códigos de sexo legado→destino
- `mapear_porte()` - Converte códigos de porte
- `mapear_especie_por_raca()` - Descobre espécie pela raça

**Exemplo de uso:**
```python
# Raça
nCdRaca, raca_matched, score = buscar_raca_por_nome(
    dest_engine, 
    "POODLE MICRO TOY",  # Nome no legado
    1,                    # Espécie: CANINA
    min_score=75
)
# Resultado: (3, "POODLE MICRO TOY", 100)

# Cor
nCdCor, cor_matched, score = buscar_cor_por_nome(
    dest_engine,
    "PRETO",             # Nome no legado
    min_score=70
)
# Resultado: (1, "PRETA", 95)
```

### 4. **Menu interativo atualizado**

✅ **Arquivo:** `src/main.py`

```
============================================================
  SISTEMA DE MIGRAÇÃO PETSYS
  Legado -> Web
============================================================

Escolha a migração que deseja executar:

  1. Clientes (PET_CLIENTE -> PESSOA)
  2. Pets (PET_ANIMAL -> PET)              ← NOVO!
  3. Vacinas (PET_VACINA -> VACINA) [EM BREVE]
  4. Atualizar Cidades via ViaCEP [EM BREVE]

  0. Sair
```

**Fluxo da migração de pets:**
1. Mostra informações sobre o que será feito
2. Alerta para executar migração de CLIENTES antes
3. Pergunta se quer executar DRY-RUN primeiro
4. Pergunta tamanho do batch
5. Executa migração real
6. Mostra resumo final

### 5. **Melhorias gerais**

✅ **Validação de duplicados** (em clientes e pets)
- Verifica se registro já foi migrado
- Se sim: **atualiza** ao invés de duplicar
- Se não: **insere** novo

✅ **Update cities aprimorado**
- Agora atualiza: cidade, logradouro, bairro, complemento
- Não apenas cidade como antes

✅ **Logs detalhados**
```
[1/3031] Processando: ANTONIA (Código: 3)
    Raça: 'SCOTISH TERRIER' -> 'SCOTISH TERRIER' (score: 100%)
    Cor: 'PRETO' -> 'PRETA' (score: 95%)
  ✓ Inserido: ANTONIA
```

## 🚀 Como executar

### Opção 1: Menu interativo (recomendado)

```bash
cd /home/vicente/dev/migracao-petsys
pipenv run python src/main.py
```

### Opção 2: Direto

```bash
# Clientes
pipenv run python src/migrations/clientes/migrate_clientes.py --dry-run

# Pets
pipenv run python src/migrations/pets/migrate_pets.py --dry-run
pipenv run python src/migrations/pets/migrate_pets.py
```

## 📊 Mapeamento completo PET_ANIMAL → PET

| Campo Origem | Campo Destino | Transformação |
|--------------|---------------|---------------|
| Codigo | - | Salvo em CONTROLE |
| Nome | sNmPet | Direto |
| DataNascimento | tDtNascimento | Conversão date |
| Raca (FK) | nCdRaca | **Fuzzy match** via PET_RACA |
| Raca → Especie | nCdEspecie | Busca em PET_RACA |
| Sexo | nCdSexo | Mapeamento 1→1, 2→3, 3→2, 4→4 |
| Porte | nCdPorte | Direto (1-4) |
| Cor (FK) | nCdCor | **Fuzzy match** via PET_COR |
| Proprietario (FK) | sCdPessoa | Busca em CONTROLE |
| DataCadastro | tDtCadastro | Conversão datetime |
| Ativo | bFlAtivo | Conversão boolean |
| Observacoes | sDsObservacoes | Trunca 500 chars |
| - | sCdTenant | Pega do .env |
| - | nVlPeso | NULL (não existe no legado) |

## ⚠️ Pontos importantes

1. **Execute migração de CLIENTES antes de PETS**
   - Pets precisam do sCdPessoa do proprietário
   - Proprietários são buscados em CONTROLE_MIGRACAO_LEGADO

2. **Pets sem proprietário são pulados**
   ```
   ⚠ Pulado: sem proprietário válido migrado
   ```

3. **Raças/cores não encontradas usam defaults**
   ```
   ⚠ Raça 'POODLE GIGANTE' não encontrada (score < 75%)
   → Usando raça padrão: S.R.D. (código 7)
   ```

4. **Sistema é idempotente**
   - Pode executar múltiplas vezes
   - Não duplica dados
   - Atualiza registros existentes

## 📈 Estatísticas esperadas

- **Total de pets no legado:** 3031
- **Pets com proprietário:** ~2900 (estimativa)
- **Pets sem proprietário:** ~131 (serão pulados)
- **Match perfeito de raças:** ~95%
- **Match perfeito de cores:** ~90%

## 🎯 Próximos passos

- [ ] Migração de Vacinas (PET_VACINA)
- [ ] Integrar update_cities.py no menu
- [ ] Adicionar opção "Executar todas as migrações"
- [ ] Dashboard de progresso

---

✅ **Sistema pronto para uso!** Execute `pipenv run python src/main.py` e escolha a opção desejada.
