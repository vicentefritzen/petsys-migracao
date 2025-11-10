"""
Script para exclusão de dados migrados

Exclui dados migrados na ordem correta para evitar problemas de foreign key:
1. Aplicações de Vacinas (PET_VACINA)
2. Pesos (PET_PESO)
3. Receitas Médicas (RECEITA_MEDICA)
4. Prontuários (PRONTUARIO)
5. Vacinas (VACINA)
6. Pets (PET)
7. Clientes (PESSOA_TIPO + PESSOA)
8. Controle (CONTROLE_MIGRACAO_LEGADO)
"""
import sys
from pathlib import Path

# Adicionar src ao path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent))

from sqlalchemy import text
from common.db_utils import get_engine_from_env, get_tenant_id
import time


def get_fresh_connection():
    """Obtém um novo engine para conexão com o banco de dados."""
    return get_engine_from_env("DEST_DB_URL")


def execute_delete_with_retry(table_name: str, tenant_id: str, step_number: str, dry_run: bool = False, retry_count: int = 3, batch_size: int = 1000):
    """
    Executa DELETE em lotes com retry automático em caso de timeout.
    
    Args:
        table_name: Nome da tabela
        tenant_id: ID do tenant
        step_number: Número do passo (ex: "1️⃣ ")
        dry_run: Se True, apenas simula
        retry_count: Número de tentativas por lote
        batch_size: Quantidade de registros por lote
    
    Returns:
        int: Número de registros excluídos
    """
    table_display_names = {
        'PET_VACINA': 'APLICAÇÕES DE VACINAS (PET_VACINA)',
        'PET_PESO': 'PESOS (PET_PESO)',
        'RECEITA_MEDICA': 'RECEITAS MÉDICAS (RECEITA_MEDICA)',
        'PRONTUARIO': 'PRONTUÁRIOS (PRONTUARIO)',
        'VACINA': 'VACINAS (VACINA)',
        'PET': 'PETS (PET)',
        'PESSOA': 'CLIENTES (PESSOA_TIPO + PESSOA)',
        'CONTROLE_MIGRACAO_LEGADO': 'CONTROLE DE MIGRAÇÃO (CONTROLE_MIGRACAO_LEGADO)'
    }
    
    print(f"\n{step_number} Excluindo {table_display_names.get(table_name, table_name)}...", end=" ", flush=True)
    
    if dry_run:
        print("[DRY-RUN]")
        return 0
    
    total_deleted = 0
    
    # Loop até não ter mais registros para excluir
    while True:
        deleted_in_batch = 0
        
        for attempt in range(retry_count):
            try:
                engine = get_fresh_connection()
                
                # Excluir em lotes (TOP N)
                delete_sql = text(f"""
                    DELETE TOP ({batch_size}) FROM {table_name} 
                    WHERE sCdTenant = '{tenant_id}'
                """)
                
                with engine.begin() as conn:
                    result = conn.execute(delete_sql)
                    deleted_in_batch = result.rowcount
                
                total_deleted += deleted_in_batch
                
                # Se deletou registros, mostrar progresso
                if deleted_in_batch > 0:
                    print(f"{total_deleted:,}...", end=" ", flush=True)
                
                break  # Sucesso, sair do retry
                
            except Exception as e:
                if attempt < retry_count - 1:
                    print(f"\n⚠ Tentativa {attempt + 1} falhou. Tentando novamente em 2 segundos...")
                    time.sleep(2)
                else:
                    print(f"\n✗ Erro após {retry_count} tentativas: {e}")
                    raise
        
        # Se não deletou nada nesse lote, terminou
        if deleted_in_batch == 0:
            break
    
    print(f"✓ Total: {total_deleted:,} registros excluídos")
    return total_deleted


def get_counts(dest_engine, tenant_id: str, retry_count: int = 3):
    """
    Retorna a quantidade de registros de cada tabela.
    
    Args:
        dest_engine: Engine do banco de dados
        tenant_id: ID do tenant
        retry_count: Número de tentativas em caso de timeout
    
    Returns:
        dict: Contagens de cada tabela
    """
    counts = {}
    
    for attempt in range(retry_count):
        try:
            # Criar nova conexão para evitar timeout
            engine = get_fresh_connection()
            
            with engine.connect() as conn:
                # Aplicações de Vacinas
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM PET_VACINA 
                    WHERE sCdTenant = '{tenant_id}'
                """))
                counts['aplicacoes_vacinas'] = result.fetchone()[0]
                
                # Pesos
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM PET_PESO 
                    WHERE sCdTenant = '{tenant_id}'
                """))
                counts['pesos'] = result.fetchone()[0]
                
                # Receitas Médicas
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM RECEITA_MEDICA 
                    WHERE sCdTenant = '{tenant_id}'
                """))
                counts['receitas'] = result.fetchone()[0]
                
                # Prontuários
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM PRONTUARIO 
                    WHERE sCdTenant = '{tenant_id}'
                """))
                counts['prontuarios'] = result.fetchone()[0]
                
                # Vacinas
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM VACINA 
                    WHERE sCdTenant = '{tenant_id}'
                """))
                counts['vacinas'] = result.fetchone()[0]
                
                # Pets
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM PET 
                    WHERE sCdTenant = '{tenant_id}'
                """))
                counts['pets'] = result.fetchone()[0]
                
                # Clientes (PESSOA + PESSOA_TIPO onde nCdTipo=2)
                result = conn.execute(text(f"""
                    SELECT COUNT(DISTINCT p.sCdPessoa)
                    FROM PESSOA p
                    INNER JOIN PESSOA_TIPO pt ON pt.sCdPessoa = p.sCdPessoa
                    WHERE p.sCdTenant = '{tenant_id}' AND pt.nCdTipo = 2
                """))
                counts['clientes'] = result.fetchone()[0]
                
                # Controle de migração
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM CONTROLE_MIGRACAO_LEGADO 
                    WHERE sCdTenant = '{tenant_id}'
                """))
                counts['controle'] = result.fetchone()[0]
            
            # Se chegou aqui, sucesso!
            return counts
            
        except Exception as e:
            if attempt < retry_count - 1:
                print(f"\n⚠ Tentativa {attempt + 1} falhou:")
                print(f"   Erro: {e}")
                print(f"   Tentando novamente em 2 segundos...")
                time.sleep(2)
            else:
                print(f"\n✗ Erro ao obter contagens após {retry_count} tentativas:")
                print(f"   {e}")
                import traceback
                traceback.print_exc()
                raise
    
    return counts


def clear_aplicacoes_vacinas(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui aplicações de vacinas."""
    return execute_delete_with_retry('PET_VACINA', tenant_id, '1️⃣ ', dry_run)


def clear_pesos(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui pesos dos pets."""
    return execute_delete_with_retry('PET_PESO', tenant_id, '2️⃣ ', dry_run)


def clear_receitas(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui receitas médicas."""
    return execute_delete_with_retry('RECEITA_MEDICA', tenant_id, '3️⃣ ', dry_run)


def clear_prontuarios(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui prontuários."""
    return execute_delete_with_retry('PRONTUARIO', tenant_id, '4️⃣ ', dry_run)


def clear_vacinas(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui vacinas."""
    return execute_delete_with_retry('VACINA', tenant_id, '5️⃣ ', dry_run)


def clear_pets(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui pets."""
    return execute_delete_with_retry('PET', tenant_id, '6️⃣ ', dry_run)


def clear_clientes(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui clientes (PESSOA_TIPO e PESSOA)."""
    print(f"\n7️⃣  Excluindo CLIENTES (PESSOA_TIPO + PESSOA)... ", end="", flush=True)
    
    if dry_run:
        engine = get_fresh_connection()
        with engine.connect() as conn:
            # Contar PESSOA_TIPO de clientes (nCdTipo=2)
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM PESSOA_TIPO pt
                INNER JOIN PESSOA p ON pt.sCdPessoa = p.sCdPessoa
                WHERE p.sCdTenant = :tenant AND pt.nCdTipo = 2
            """), {"tenant": tenant_id})
            count_tipo = result.fetchone()[0]
            
            # Contar PESSOA de clientes
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM PESSOA p
                WHERE p.sCdTenant = :tenant 
                AND EXISTS (
                    SELECT 1 FROM PESSOA_TIPO pt 
                    WHERE pt.sCdPessoa = p.sCdPessoa AND pt.nCdTipo = 2
                )
            """), {"tenant": tenant_id})
            count_pessoa = result.fetchone()[0]
            
            print(f"[dry-run] PESSOA_TIPO: {count_tipo}, PESSOA: {count_pessoa}")
            return count_tipo + count_pessoa
    
    total_deleted = 0
    
    # 1. Deletar PESSOA_TIPO (nCdTipo=2 - CLIENTE) em lotes
    print("\n   → PESSOA_TIPO (clientes)... ", end="", flush=True)
    retry_count = 0
    max_retries = 3
    batch_size = 1000
    
    while retry_count < max_retries:
        try:
            engine = get_fresh_connection()
            with engine.connect() as conn:
                deleted_in_batch = 1
                while deleted_in_batch > 0:
                    result = conn.execute(text(f"""
                        DELETE TOP ({batch_size}) FROM PESSOA_TIPO
                        WHERE sCdPessoa IN (
                            SELECT sCdPessoa FROM PESSOA WHERE sCdTenant = :tenant
                        ) AND nCdTipo = 2
                    """), {"tenant": tenant_id})
                    deleted_in_batch = result.rowcount
                    conn.commit()
                    
                    if deleted_in_batch > 0:
                        total_deleted += deleted_in_batch
                        print(f"{total_deleted:,}...", end=" ", flush=True)
                
                print(f"✓ Total: {total_deleted:,}")
                break
        except Exception as e:
            retry_count += 1
            if retry_count < max_retries:
                print(f"\n⚠ Tentativa {retry_count} falhou:")
                print(f"   Erro: {e}")
                print(f"   Tentando novamente em 2 segundos...")
                time.sleep(2)
            else:
                print(f"\n✗ Erro após {max_retries} tentativas:")
                print(f"   {e}")
                import traceback
                traceback.print_exc()
                raise
    
    # 2. Deletar PESSOA em lotes (apenas pessoas que não têm mais PESSOA_TIPO)
    print("   → PESSOA (sem tipos associados)... ", end="", flush=True)
    pessoa_deleted = 0
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            engine = get_fresh_connection()
            with engine.connect() as conn:
                deleted_in_batch = 1
                while deleted_in_batch > 0:
                    result = conn.execute(text(f"""
                        DELETE TOP ({batch_size}) FROM PESSOA
                        WHERE sCdTenant = :tenant
                        AND NOT EXISTS (
                            SELECT 1 FROM PESSOA_TIPO pt 
                            WHERE pt.sCdPessoa = PESSOA.sCdPessoa
                        )
                    """), {"tenant": tenant_id})
                    deleted_in_batch = result.rowcount
                    conn.commit()
                    
                    if deleted_in_batch > 0:
                        pessoa_deleted += deleted_in_batch
                        print(f"{pessoa_deleted:,}...", end=" ", flush=True)
                
                if pessoa_deleted > 0:
                    print(f"✓ Total: {pessoa_deleted:,}")
                else:
                    print("✓ Nenhum registro (pessoas ainda têm outros tipos)")
                
                total_deleted += pessoa_deleted
                break
        except Exception as e:
            retry_count += 1
            if retry_count < max_retries:
                print(f"\n⚠ Tentativa {retry_count} falhou:")
                print(f"   Erro: {e}")
                print(f"   Tentando novamente em 2 segundos...")
                time.sleep(2)
            else:
                print(f"\n✗ Erro após {max_retries} tentativas:")
                print(f"   {e}")
                import traceback
                traceback.print_exc()
                raise
    
    return total_deleted


def clear_controle(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui registros de controle de migração."""
    return execute_delete_with_retry('CONTROLE_MIGRACAO_LEGADO', tenant_id, '8️⃣ ', dry_run)


def clear_all_data(dry_run: bool = False):
    """
    Exclui todos os dados migrados na ordem correta.
    
    Args:
        dry_run: Se True, apenas simula (não exclui)
    
    Returns:
        dict: Estatísticas da exclusão
    """
    print("\n" + "="*80)
    print("EXCLUSÃO DE DADOS MIGRADOS")
    print("="*80 + "\n")
    
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()
    
    print(f"🔑 Tenant ID: {tenant_id}")
    
    # Mostrar contagens antes
    print("\n📊 Contagem ANTES da exclusão:")
    counts_before = get_counts(dest_engine, tenant_id)
    print(f"  • Aplicações de Vacinas: {counts_before['aplicacoes_vacinas']:,}")
    print(f"  • Pesos: {counts_before['pesos']:,}")
    print(f"  • Receitas Médicas: {counts_before['receitas']:,}")
    print(f"  • Prontuários: {counts_before['prontuarios']:,}")
    print(f"  • Vacinas: {counts_before['vacinas']:,}")
    print(f"  • Pets: {counts_before['pets']:,}")
    print(f"  • Clientes: {counts_before['clientes']:,}")
    print(f"  • Registros de Controle: {counts_before['controle']:,}")
    
    if dry_run:
        print("\n[DRY-RUN] Simulando exclusão...\n")
    else:
        print("\n⚠️  ATENÇÃO: Esta operação é IRREVERSÍVEL!\n")
        print("Excluindo dados na ordem correta (respeitando foreign keys):\n")
    
    # Executar exclusões na ordem correta
    stats = {}
    
    try:
        stats['aplicacoes_vacinas'] = clear_aplicacoes_vacinas(dest_engine, tenant_id, dry_run)
        stats['pesos'] = clear_pesos(dest_engine, tenant_id, dry_run)
        stats['receitas'] = clear_receitas(dest_engine, tenant_id, dry_run)
        stats['prontuarios'] = clear_prontuarios(dest_engine, tenant_id, dry_run)
        stats['vacinas'] = clear_vacinas(dest_engine, tenant_id, dry_run)
        stats['pets'] = clear_pets(dest_engine, tenant_id, dry_run)
        stats['clientes'] = clear_clientes(dest_engine, tenant_id, dry_run)
        stats['controle'] = clear_controle(dest_engine, tenant_id, dry_run)
        
    except Exception as e:
        print(f"\n✗ Erro durante exclusão: {e}")
        return None
    
    # Mostrar contagens depois
    if not dry_run:
        print("\n📊 Contagem APÓS a exclusão:")
        counts_after = get_counts(dest_engine, tenant_id)
        print(f"  • Aplicações de Vacinas: {counts_after['aplicacoes_vacinas']:,}")
        print(f"  • Pesos: {counts_after['pesos']:,}")
        print(f"  • Receitas Médicas: {counts_after['receitas']:,}")
        print(f"  • Prontuários: {counts_after['prontuarios']:,}")
        print(f"  • Vacinas: {counts_after['vacinas']:,}")
        print(f"  • Pets: {counts_after['pets']:,}")
        print(f"  • Clientes: {counts_after['clientes']:,}")
        print(f"  • Registros de Controle: {counts_after['controle']:,}")
    
    print("\n" + "="*80)
    if dry_run:
        print("✓ Simulação concluída!")
    else:
        print("✓ Exclusão concluída!")
    
    total_deleted = sum(stats.values())
    print(f"  Total de registros excluídos: {total_deleted:,}")
    print("="*80 + "\n")
    
    return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Exclusão de Dados Migrados")
    parser.add_argument("--dry-run", action="store_true", help="Simula exclusão sem deletar dados")
    parser.add_argument("--confirm", action="store_true", help="Confirma exclusão (obrigatório para executar)")
    
    args = parser.parse_args()
    
    if not args.dry_run and not args.confirm:
        print("\n⚠️  ATENÇÃO: Esta operação é IRREVERSÍVEL!")
        print("\nPara executar a exclusão, use: --confirm")
        print("Para simular, use: --dry-run")
        print("\nExemplo: python src/clear_migrated_data.py --dry-run")
        print("         python src/clear_migrated_data.py --confirm\n")
        sys.exit(1)
    
    clear_all_data(dry_run=args.dry_run)
