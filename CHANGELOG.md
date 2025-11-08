# Changelog - Sistema de Migração PetSys

## [07/11/2025] - Melhorias de Validação e Atualização

### ✅ Adicionado
- **Validação de documentos duplicados**: Sistema agora verifica se CPF/CNPJ já existe antes de inserir
- **Update automático**: Se o documento já existir, atualiza os dados ao invés de pular
- **Atualização completa de endereços via ViaCEP**: 
  - Logradouro (nome da rua)
  - Bairro
  - Complemento
  - Cidade
- **Validação de PESSOA_TIPO**: Verifica se tipo CLIENTE já existe antes de inserir

### 🔧 Modificado

#### `src/migrations/migrate_clientes.py`
- Função `insert_pessoa()` renomeada para `insert_or_update_pessoa()`
- Comportamento:
  - **Se documento existir**: UPDATE dos dados + verifica/adiciona tipo CLIENTE
  - **Se documento não existir**: INSERT normal
- Logs informativos:
  - `✓ Atualizado:` quando atualiza registro existente
  - `✓ Inserido:` quando insere novo registro
  - `→ Tipo CLIENTE adicionado` quando associa tipo novo

#### `src/update_cities.py`
- Função `atualizar_cidade_pessoa()` renomeada para `atualizar_endereco_pessoa()`
- Agora atualiza campos adicionais:
  - `sDsEndereco` (logradouro da API)
  - `sDsBairro` (bairro da API)
  - `sDsComplemento` (complemento da API)
- UPDATE dinâmico: só atualiza campos que o ViaCEP retornou
- Logs detalhados mostram dados encontrados na API

#### `src/migrations/migrate_template.py`
- Template atualizado com padrão INSERT/UPDATE
- Função `insert_destino()` substituída por `insert_or_update_destino()`
- Comentários TODO para facilitar adaptação

### 📊 Benefícios
1. **Idempotência**: Pode executar a migração múltiplas vezes sem duplicar dados
2. **Sincronização**: Dados do legado sempre sobrescrevem dados antigos no destino
3. **Endereços completos**: Não apenas cidade, mas rua, bairro e complemento
4. **Menos erros**: Não quebra por constraint UNIQUE de documento

### 🎯 Casos de Uso
- **Re-execução segura**: Execute a migração novamente para atualizar dados alterados no legado
- **Correção de dados**: Se dados foram corrigidos no sistema legado, basta re-migrar
- **Completar endereços**: Execute `update_cities.py` para preencher ruas/bairros via ViaCEP
