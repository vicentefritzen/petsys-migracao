"""
Script para limpar pets duplicados e reorganizar a tabela de controle
"""
from common.db_utils import get_engine_from_env, get_tenant_id
from sqlalchemy import text

def limpar_duplicatas():
    dest = get_engine_from_env('DEST_DB_URL')
    tenant_id = get_tenant_id()
    
    print("="*60)
    print("LIMPEZA DE DUPLICATAS - PETS")
    print("="*60)
    
    with dest.connect() as conn:
        # 1. Verificar situação atual
        result = conn.execute(text(f"SELECT COUNT(*) FROM PET WHERE sCdTenant = '{tenant_id}'"))
        total_pets = result.scalar()
        
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM CONTROLE_MIGRACAO_LEGADO 
            WHERE sCdTenant = '{tenant_id}' AND sTabelaOrigem = 'PET_ANIMAL'
        """))
        total_controle = result.scalar()
        
        print(f"\nSituação ANTES da limpeza:")
        print(f"  Pets no destino: {total_pets}")
        print(f"  Registros no controle: {total_controle}")
    
    with dest.begin() as conn:
        print("\n🗑️  Removendo duplicatas do controle...")
        
        # 2. Deletar duplicatas, mantendo apenas o mais recente
        conn.execute(text(f"""
            DELETE FROM CONTROLE_MIGRACAO_LEGADO
            WHERE Id NOT IN (
                SELECT MAX(Id)
                FROM CONTROLE_MIGRACAO_LEGADO
                WHERE sCdTenant = '{tenant_id}'
                AND sTabelaOrigem = 'PET_ANIMAL'
                GROUP BY sValorChaveOrigem
            )
            AND sCdTenant = '{tenant_id}'
            AND sTabelaOrigem = 'PET_ANIMAL'
        """))
        print("  ✓ Duplicatas removidas do controle")
        
        print("\n⚠️  AVISO: Não vou deletar pets órfãos pois há dependências (PRONTUARIO, PET_PESO, etc)")
        print("  Se necessário, faça a limpeza manual no banco")
    
    # 3. Verificar situação final
    with dest.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM PET WHERE sCdTenant = '{tenant_id}'"))
        total_pets_final = result.scalar()
        
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM CONTROLE_MIGRACAO_LEGADO 
            WHERE sCdTenant = '{tenant_id}' AND sTabelaOrigem = 'PET_ANIMAL'
        """))
        total_controle_final = result.scalar()
        
        # Verificar se ainda há duplicatas
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM (
                SELECT sValorChaveOrigem
                FROM CONTROLE_MIGRACAO_LEGADO
                WHERE sCdTenant = '{tenant_id}' AND sTabelaOrigem = 'PET_ANIMAL'
                GROUP BY sValorChaveOrigem
                HAVING COUNT(*) > 1
            ) AS Dups
        """))
        duplicatas_restantes = result.scalar()
        
        print(f"\nSituação DEPOIS da limpeza:")
        print(f"  Pets no destino: {total_pets_final} (era {total_pets})")
        print(f"  Registros no controle: {total_controle_final} (era {total_controle})")
        print(f"  Duplicatas no controle: {duplicatas_restantes}")
        
        if duplicatas_restantes == 0 and total_pets_final == total_controle_final:
            print("\n✅ Limpeza concluída com sucesso!")
            print(f"   {total_pets_final} pets únicos no sistema")
        else:
            print("\n⚠️  Ainda há inconsistências!")

if __name__ == "__main__":
    limpar_duplicatas()
