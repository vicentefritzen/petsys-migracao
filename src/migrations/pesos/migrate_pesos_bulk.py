"""
Migração de Pesos dos Pets - Bulk Insert Otimizado

Migra dados de PET_ANIMAL_PESO (legado) para PET_PESO (destino)
usando bulk insert para melhor performance.

Origem:  PET_ANIMAL_PESO (Codigo, Animal, Data, Peso)
Destino: PET_PESO (sCdPetPeso, sCdTenant, sCdPet, sCdUsuario, nVlPeso, 
                   tDtPesagem, tDtCriacao, tDtAlteracao)
"""
import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal
import uuid

# Adicionar src ao path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import text
from common.db_utils import get_engine_from_env, get_tenant_id


def get_default_vet_user_id():
    """Retorna o ID do usuário veterinário padrão do .env."""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    vet_user_id = os.getenv("DEFAULT_VET_USER_ID")
    
    if not vet_user_id:
        raise RuntimeError(
            "Variável DEFAULT_VET_USER_ID não definida no arquivo .env\n"
            "Adicione: DEFAULT_VET_USER_ID=f7cc3d41-12e9-4247-a828-69cfeeb52a74"
        )
    
    return vet_user_id


def map_origem_to_destino(row, tenant_id: str, sCdPet: str, sCdUsuario: str):
    """
    Mapeia registro de origem para formato do destino.
    
    Args:
        row: Registro da tabela PET_ANIMAL_PESO
        tenant_id: ID da tenant
        sCdPet: UUID do pet no destino
        sCdUsuario: UUID do usuário veterinário
    
    Returns:
        dict: Registro no formato da tabela PET_PESO
    """
    now = datetime.now()
    
    # Converter peso para DECIMAL(6,3) - máximo 999.999 kg
    # Usar Decimal para evitar overflow e manter precisão
    peso = Decimal(str(row.Peso)) if row.Peso else Decimal('0.000')
    
    # Validação: pesos acima de 999.999 são erros de digitação
    # Provavelmente gramas digitadas como quilos (dividir por 1000)
    if peso >= Decimal('1000'):
        peso = peso / Decimal('1000')
    
    # Garantir que não excede DECIMAL(6,3)
    if peso > Decimal('999.999'):
        peso = Decimal('999.999')
    
    return {
        'sCdPetPeso': str(uuid.uuid4()),
        'sCdTenant': tenant_id,
        'sCdPet': sCdPet,
        'sCdUsuario': sCdUsuario,
        'nVlPeso': peso,
        'nVlMedida': None,  # Não existe no legado
        'tDtPesagem': row.Data,
        'sDsObservacoes': None,
        'tDtCriacao': row.Data if row.Data else now,
        'tDtAlteracao': None
    }


def migrate_pesos_bulk(batch_size: int = 1000, dry_run: bool = False):
    """
    Migração BULK de pesos dos pets.
    
    Estratégia de otimização:
    1. Carregar TODOS os mapeamentos de pets (1 query)
    2. Carregar TODOS os pesos já migrados (1 query)
    3. Processar TODOS os registros em memória
    4. Bulk INSERT de todos os novos registros (1 query)
    5. UPDATE de registros existentes (se houver)
    6. Registrar controle em lotes
    
    Args:
        batch_size: Tamanho do lote para controle (padrão: 1000)
        dry_run: Se True, apenas simula (não insere dados)
    
    Returns:
        int: Total de registros processados
    """
    print("\n" + "="*80)
    print("MIGRAÇÃO DE PESOS DOS PETS - BULK INSERT")
    print("="*80 + "\n")
    
    if dry_run:
        print("🔍 MODO DRY-RUN (simulação)")
        print("   Nenhum dado será inserido no banco de dados\n")
    
    # Conectar aos bancos
    origem_engine = get_engine_from_env("LEGACY_DB_URL")
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()
    vet_user_id = get_default_vet_user_id()
    
    print(f"🔑 Tenant ID: {tenant_id}")
    print(f"👨‍⚕️  Veterinário ID: {vet_user_id}\n")
    
    # ==================================================================
    # FASE 1: PRE-CARREGAR MAPEAMENTOS (otimização)
    # ==================================================================
    print("📊 Carregando dados de referência...")
    
    # Mapeamento de pets (Animal -> sCdPet)
    print("  - Mapeamento de pets...", end=" ", flush=True)
    pets_map = {}
    with dest_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT sValorChaveOrigem, sValorChaveDestino
            FROM CONTROLE_MIGRACAO_LEGADO
            WHERE sCdTenant = '{tenant_id}'
              AND sTabelaOrigem = 'PET_ANIMAL'
              AND sTabelaDestino = 'PET'
        """))
        
        for row in result:
            pets_map[int(row.sValorChaveOrigem)] = row.sValorChaveDestino
    
    print(f"✓ {len(pets_map):,} pets mapeados")
    
    # Pesos já migrados (para update)
    print("  - Pesos já migrados...", end=" ", flush=True)
    pesos_migrados = {}
    with dest_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT sValorChaveOrigem, sValorChaveDestino
            FROM CONTROLE_MIGRACAO_LEGADO
            WHERE sCdTenant = '{tenant_id}'
              AND sTabelaOrigem = 'PET_ANIMAL_PESO'
              AND sTabelaDestino = 'PET_PESO'
        """))
        
        for row in result:
            pesos_migrados[int(row.sValorChaveOrigem)] = row.sValorChaveDestino
    
    print(f"✓ {len(pesos_migrados):,} pesos")
    
    # ==================================================================
    # FASE 2: CARREGAR TODOS OS REGISTROS DA ORIGEM
    # ==================================================================
    print("\n🔄 Carregando registros da origem...")
    
    with origem_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT Codigo, Animal, Data, Peso
            FROM PET_ANIMAL_PESO
            ORDER BY Codigo
        """))
        
        all_rows = result.fetchall()
    
    total = len(all_rows)
    print(f"  Total de pesos no legado: {total:,}\n")
    
    if dry_run:
        print(f"[DRY-RUN] Seriam processados {total:,} registros")
        print(f"[DRY-RUN] Pets disponíveis: {len(pets_map):,}")
        print(f"[DRY-RUN] Pesos já migrados: {len(pesos_migrados):,}")
        return total
    
    # ==================================================================
    # FASE 3: PROCESSAR TODOS OS REGISTROS EM MEMÓRIA
    # ==================================================================
    print("⚙️  Processando registros em memória...")
    
    pesos_para_inserir = []
    pesos_para_atualizar = []
    controle_para_inserir = []
    
    stats = {
        'total': 0,
        'inseridos': 0,
        'atualizados': 0,
        'sem_pet': 0
    }
    
    for i, row in enumerate(all_rows, 1):
        stats['total'] += 1
        
        # Progresso
        if i % 1000 == 0:
            print(f"  Processando: {i:,}/{total:,} registros...")
        
        codigo_origem = int(row.Codigo)
        animal_id = int(row.Animal)
        
        # Verificar se pet foi migrado
        if animal_id not in pets_map:
            stats['sem_pet'] += 1
            continue
        
        sCdPet = pets_map[animal_id]
        
        # Mapear para destino
        peso = map_origem_to_destino(row, tenant_id, sCdPet, vet_user_id)
        
        # Verificar se já foi migrado
        if codigo_origem in pesos_migrados:
            # Atualizar
            peso['sCdPetPeso'] = pesos_migrados[codigo_origem]
            pesos_para_atualizar.append(peso)
            stats['atualizados'] += 1
        else:
            # Inserir
            pesos_para_inserir.append(peso)
            stats['inseridos'] += 1
            
            # Registro de controle
            controle_para_inserir.append({
                'sCdTenant': tenant_id,
                'sTabelaOrigem': 'PET_ANIMAL_PESO',
                'sCampoChaveOrigem': 'Codigo',
                'sValorChaveOrigem': str(codigo_origem),
                'sTabelaDestino': 'PET_PESO',
                'sCampoChaveDestino': 'sCdPetPeso',
                'sValorChaveDestino': peso['sCdPetPeso'],
                'dtMigracao': datetime.now()
            })
    
    print(f"  ✓ Processamento concluído!")
    print(f"    - Para inserir: {len(pesos_para_inserir):,}")
    print(f"    - Para atualizar: {len(pesos_para_atualizar):,}")
    print(f"    - Sem pet migrado: {stats['sem_pet']:,}\n")
    
    # ==================================================================
    # FASE 4: BULK INSERT/UPDATE
    # ==================================================================
    print("💾 Salvando no banco de dados...")
    
    with dest_engine.begin() as conn:
        # Inserir novos pesos
        if pesos_para_inserir:
            print(f"  - Inserindo {len(pesos_para_inserir):,} pesos novos...", end=" ", flush=True)
            
            insert_sql = text("""
                INSERT INTO PET_PESO (
                    sCdPetPeso, sCdTenant, sCdPet, sCdUsuario,
                    nVlPeso, nVlMedida, tDtPesagem, sDsObservacoes,
                    tDtCriacao, tDtAlteracao
                )
                VALUES (
                    :sCdPetPeso, :sCdTenant, :sCdPet, :sCdUsuario,
                    :nVlPeso, :nVlMedida, :tDtPesagem, :sDsObservacoes,
                    :tDtCriacao, :tDtAlteracao
                )
            """)
            
            conn.execute(insert_sql, pesos_para_inserir)
            print("✓")
        
        # Atualizar pesos existentes
        if pesos_para_atualizar:
            print(f"  - Atualizando {len(pesos_para_atualizar):,} pesos existentes...", end=" ", flush=True)
            
            update_sql = text("""
                UPDATE PET_PESO
                SET sCdPet = :sCdPet,
                    sCdUsuario = :sCdUsuario,
                    nVlPeso = :nVlPeso,
                    tDtPesagem = :tDtPesagem,
                    tDtAlteracao = :tDtAlteracao
                WHERE sCdPetPeso = :sCdPetPeso
                  AND sCdTenant = :sCdTenant
            """)
            
            for peso in pesos_para_atualizar:
                peso['tDtAlteracao'] = datetime.now()
                conn.execute(update_sql, peso)
            
            print("✓")
        
        # Registrar controle em lotes (evitar limite de placeholders)
        if controle_para_inserir:
            print(f"  - Registrando {len(controle_para_inserir):,} mapeamentos...", end=" ", flush=True)
            
            # Deletar registros antigos primeiro (em chunks)
            codigos_origem = [c['sValorChaveOrigem'] for c in controle_para_inserir]
            
            for i in range(0, len(codigos_origem), 1000):
                chunk = codigos_origem[i:i+1000]
                placeholders = ','.join([f"'{c}'" for c in chunk])
                
                delete_sql = text(f"""
                    DELETE FROM CONTROLE_MIGRACAO_LEGADO
                    WHERE sCdTenant = '{tenant_id}'
                      AND sTabelaOrigem = 'PET_ANIMAL_PESO'
                      AND sTabelaDestino = 'PET_PESO'
                      AND sValorChaveOrigem IN ({placeholders})
                """)
                
                conn.execute(delete_sql)
            
            # Inserir novos registros (em chunks)
            insert_controle_sql = text("""
                INSERT INTO CONTROLE_MIGRACAO_LEGADO (
                    sCdTenant, sTabelaOrigem, sCampoChaveOrigem, sValorChaveOrigem,
                    sTabelaDestino, sCampoChaveDestino, sValorChaveDestino, dtMigracao
                )
                VALUES (
                    :sCdTenant, :sTabelaOrigem, :sCampoChaveOrigem, :sValorChaveOrigem,
                    :sTabelaDestino, :sCampoChaveDestino, :sValorChaveDestino, :dtMigracao
                )
            """)
            
            for i in range(0, len(controle_para_inserir), 1000):
                chunk = controle_para_inserir[i:i+1000]
                conn.execute(insert_controle_sql, chunk)
            
            print("✓")
    
    # ==================================================================
    # ESTATÍSTICAS FINAIS
    # ==================================================================
    print("\n" + "="*80)
    print("✓ Migração finalizada!")
    print("="*80)
    print(f"  Total processado: {stats['total']:,}")
    print(f"  Inseridos: {stats['inseridos']:,}")
    print(f"  Atualizados: {stats['atualizados']:,}")
    print(f"  Sem pet migrado: {stats['sem_pet']:,}")
    print("="*80 + "\n")
    
    return stats['total']


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migração de Pesos dos Pets (Bulk)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Tamanho do lote para controle")
    parser.add_argument("--dry-run", action="store_true", help="Simula migração sem inserir dados")
    
    args = parser.parse_args()
    
    migrate_pesos_bulk(batch_size=args.batch_size, dry_run=args.dry_run)
