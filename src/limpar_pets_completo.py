"""
Script para limpar COMPLETAMENTE os pets duplicados
Remove todas as dependências em cascata
"""
from common.db_utils import get_engine_from_env, get_tenant_id
from sqlalchemy import text

def limpar_pets_completo():
    dest = get_engine_from_env('DEST_DB_URL')
    tenant_id = get_tenant_id()
    
    print("="*60)
    print("LIMPEZA COMPLETA - PETS E DEPENDÊNCIAS")
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
        print(f"  Diferença (pets órfãos): {total_pets - total_controle}")
    
    # Perguntar confirmação
    print("\n⚠️  ATENÇÃO: Esta operação vai:")
    print("  1. Deletar TODOS os pets duplicados")
    print("  2. Deletar TODAS as dependências (PRONTUARIO, PET_PESO, etc)")
    print("  3. Manter apenas 1 pet por código do legado (o mais recente)")
    print("\n❌ Esta operação NÃO pode ser desfeita!")
    
    resposta = input("\nDeseja continuar? (digite 'SIM' para confirmar): ")
    if resposta != 'SIM':
        print("Operação cancelada.")
        return
    
    with dest.begin() as conn:
        print("\n🗑️  Etapa 1: Removendo duplicatas do controle...")
        
        # Deletar duplicatas, mantendo apenas o mais recente
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
        
        # Identificar pets órfãos (que não estão no controle)
        print("\n🗑️  Etapa 2: Deletando dependências dos pets órfãos...")
        
        # 2.1 Deletar PRONTUARIO
        result = conn.execute(text(f"""
            DELETE FROM PRONTUARIO
            WHERE sCdPet IN (
                SELECT sCdPet FROM PET
                WHERE sCdTenant = '{tenant_id}'
                AND sCdPet NOT IN (
                    SELECT sValorChaveDestino
                    FROM CONTROLE_MIGRACAO_LEGADO
                    WHERE sCdTenant = '{tenant_id}'
                    AND sTabelaOrigem = 'PET_ANIMAL'
                )
            )
        """))
        print(f"  ✓ PRONTUARIO: {result.rowcount} registros deletados")
        
        # 2.2 Deletar PET_PESO
        result = conn.execute(text(f"""
            DELETE FROM PET_PESO
            WHERE sCdPet IN (
                SELECT sCdPet FROM PET
                WHERE sCdTenant = '{tenant_id}'
                AND sCdPet NOT IN (
                    SELECT sValorChaveDestino
                    FROM CONTROLE_MIGRACAO_LEGADO
                    WHERE sCdTenant = '{tenant_id}'
                    AND sTabelaOrigem = 'PET_ANIMAL'
                )
            )
        """))
        print(f"  ✓ PET_PESO: {result.rowcount} registros deletados")
        
        # 2.3 Verificar se há outras tabelas dependentes
        try:
            result = conn.execute(text(f"""
                DELETE FROM PET_VACINA
                WHERE sCdPet IN (
                    SELECT sCdPet FROM PET
                    WHERE sCdTenant = '{tenant_id}'
                    AND sCdPet NOT IN (
                        SELECT sValorChaveDestino
                        FROM CONTROLE_MIGRACAO_LEGADO
                        WHERE sCdTenant = '{tenant_id}'
                        AND sTabelaOrigem = 'PET_ANIMAL'
                    )
                )
            """))
            print(f"  ✓ PET_VACINA: {result.rowcount} registros deletados")
        except:
            pass  # Tabela pode não existir
        
        # 2.4 Deletar outras dependências possíveis
        tabelas_dependentes = [
            'RECEITA_MEDICA',
            'AGENDAMENTO',
            'SERVICO_ITEM',
            'VENDA_ITEM',
            'PET_ALERGIA',
            'PET_DOENCA'
        ]
        
        for tabela in tabelas_dependentes:
            try:
                result = conn.execute(text(f"""
                    DELETE FROM {tabela}
                    WHERE sCdPet IN (
                        SELECT sCdPet FROM PET
                        WHERE sCdTenant = '{tenant_id}'
                        AND sCdPet NOT IN (
                            SELECT sValorChaveDestino
                            FROM CONTROLE_MIGRACAO_LEGADO
                            WHERE sCdTenant = '{tenant_id}'
                            AND sTabelaOrigem = 'PET_ANIMAL'
                        )
                    )
                """))
                if result.rowcount > 0:
                    print(f"  ✓ {tabela}: {result.rowcount} registros deletados")
            except Exception as e:
                # Tabela pode não existir ou não ter coluna sCdPet
                pass
        
        # 3. Deletar os pets órfãos
        print("\n🗑️  Etapa 3: Deletando pets órfãos...")
        result = conn.execute(text(f"""
            DELETE FROM PET
            WHERE sCdTenant = '{tenant_id}'
            AND sCdPet NOT IN (
                SELECT sValorChaveDestino
                FROM CONTROLE_MIGRACAO_LEGADO
                WHERE sCdTenant = '{tenant_id}'
                AND sTabelaOrigem = 'PET_ANIMAL'
            )
        """))
        print(f"  ✓ {result.rowcount} pets órfãos deletados")
    
    # 4. Verificar situação final
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
        
        print(f"\n{'='*60}")
        print("RESULTADO FINAL")
        print(f"{'='*60}")
        print(f"  Pets no destino: {total_pets_final} (era {total_pets})")
        print(f"  Registros no controle: {total_controle_final} (era {total_controle})")
        print(f"  Pets deletados: {total_pets - total_pets_final}")
        print(f"  Duplicatas no controle: {duplicatas_restantes}")
        
        if duplicatas_restantes == 0 and total_pets_final == total_controle_final:
            print("\n✅ LIMPEZA CONCLUÍDA COM SUCESSO!")
            print(f"   Sistema limpo: {total_pets_final} pets únicos")
        else:
            print("\n⚠️  Ainda há inconsistências!")
            if total_pets_final != total_controle_final:
                print(f"   Diferença entre pets ({total_pets_final}) e controle ({total_controle_final})")

if __name__ == "__main__":
    limpar_pets_completo()
