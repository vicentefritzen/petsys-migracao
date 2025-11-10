"""
Script de teste para verificar a lógica de exclusão de clientes
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from sqlalchemy import text
from common.db_utils import get_engine_from_env, get_tenant_id

def test_query():
    """Testa as queries de exclusão sem executar"""
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()
    
    print(f"Tenant ID: {tenant_id}\n")
    print("="*60)
    
    with dest_engine.connect() as conn:
        # 1. Contar PESSOA_TIPO de clientes (nCdTipo=2)
        print("1. PESSOA_TIPO (clientes) a deletar:")
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM PESSOA_TIPO pt
            INNER JOIN PESSOA p ON pt.sCdPessoa = p.sCdPessoa
            WHERE p.sCdTenant = :tenant AND pt.nCdTipo = 2
        """), {"tenant": tenant_id})
        count_tipo = result.fetchone()[0]
        print(f"   Total: {count_tipo:,}")
        
        # 2. Contar PESSOA que serão deletadas (sem PESSOA_TIPO após deletar clientes)
        print("\n2. PESSOA (sem outros tipos após deletar PESSOA_TIPO cliente):")
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT p.sCdPessoa)
            FROM PESSOA p
            WHERE p.sCdTenant = :tenant
            AND NOT EXISTS (
                SELECT 1 FROM PESSOA_TIPO pt 
                WHERE pt.sCdPessoa = p.sCdPessoa 
                AND pt.nCdTipo != 2
            )
        """), {"tenant": tenant_id})
        count_pessoa = result.fetchone()[0]
        print(f"   Total: {count_pessoa:,}")
        
        # 3. Contar PESSOA que têm outros tipos além de cliente
        print("\n3. PESSOA que NÃO serão deletadas (têm outros tipos):")
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT p.sCdPessoa)
            FROM PESSOA p
            WHERE p.sCdTenant = :tenant
            AND EXISTS (
                SELECT 1 FROM PESSOA_TIPO pt 
                WHERE pt.sCdPessoa = p.sCdPessoa 
                AND pt.nCdTipo != 2
            )
        """), {"tenant": tenant_id})
        count_outros = result.fetchone()[0]
        print(f"   Total: {count_outros:,}")
        
        # 4. Verificar se há PESSOA sem nenhum tipo
        print("\n4. PESSOA sem NENHUM tipo associado:")
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM PESSOA p
            WHERE p.sCdTenant = :tenant
            AND NOT EXISTS (
                SELECT 1 FROM PESSOA_TIPO pt 
                WHERE pt.sCdPessoa = p.sCdPessoa
            )
        """), {"tenant": tenant_id})
        count_sem_tipo = result.fetchone()[0]
        print(f"   Total: {count_sem_tipo:,}")
        
    print("\n" + "="*60)
    print("RESUMO:")
    print(f"  - PESSOA_TIPO (clientes) a deletar: {count_tipo:,}")
    print(f"  - PESSOA a deletar: {count_pessoa:,}")
    print(f"  - PESSOA que permanecerão (outros tipos): {count_outros:,}")
    print(f"  - PESSOA sem tipo (órfãs): {count_sem_tipo:,}")
    print("="*60)

if __name__ == "__main__":
    test_query()
