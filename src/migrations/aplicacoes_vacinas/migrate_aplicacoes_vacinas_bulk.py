"""
Migração de Aplicações de Vacinas (Carteira de Vacinas) - VERSÃO BULK
PET_ANIMAL_VACINA (origem) -> PET_VACINA (destino)

Migra o histórico de vacinas aplicadas e previstas dos pets usando BULK INSERT.
Performance otimizada para grandes volumes de dados.
"""
import sys
from pathlib import Path

# Adicionar src ao path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import uuid
from datetime import datetime, date
from sqlalchemy import text
from common.db_utils import get_engine_from_env, ensure_controle_table, get_tenant_id


def map_origem_to_destino(row, tenant_id: str, sCdPet: str, sCdVacina: str):
    """
    Mapeia um registro da tabela PET_ANIMAL_VACINA (origem) para PET_VACINA (destino).
    
    Versão otimizada - recebe IDs já mapeados.
    
    Args:
        row: Row da consulta SQL
        tenant_id: ID do tenant
        sCdPet: UUID do pet no destino
        sCdVacina: UUID da vacina no destino
    
    Returns:
        dict: Dados mapeados para inserção na tabela destino
    """
    def safe(val, default=None):
        if val is None:
            return default
        if isinstance(val, str):
            val = val.strip()
            return val if val else default
        return val
    
    def safe_date(val):
        if val is None:
            return None
        if isinstance(val, (datetime, date)):
            return val if isinstance(val, datetime) else datetime.combine(val, datetime.min.time())
        try:
            return datetime.fromisoformat(str(val))
        except:
            return None
    
    def safe_bool(val, default=False):
        if val is None:
            return default
        try:
            return bool(int(val))
        except (ValueError, TypeError):
            return default
    
    # Campos diretos
    sDsPartida = safe(row.Partida)
    sDsLaboratorio = safe(row.Laboratorio)
    
    # Datas
    tDtPrevista = safe_date(row.DataPrevista)
    tDtAplicacao = safe_date(row.DataAplicacao)
    
    # Flag Aplicada - determinada pela existência de DataAplicacao
    bFlAplicada = tDtAplicacao is not None
    
    # Timestamps
    tDtCriacao = datetime.now()
    tDtAlteracao = datetime.now() if tDtAplicacao else None
    
    return {
        "sCdTenant": tenant_id,
        "sCdPet": sCdPet,
        "sCdVacina": sCdVacina,
        "sCdUsuario": None,
        "sDsPartida": sDsPartida,
        "tDtPrevista": tDtPrevista,
        "tDtAplicacao": tDtAplicacao,
        "sDsLaboratorio": sDsLaboratorio,
        "sDsLocalAplicacao": None,
        "bFlPreAutorizado": False,
        "tDtCriacao": tDtCriacao,
        "tDtAlteracao": tDtAlteracao,
    }


def migrate_aplicacoes_vacinas_bulk(batch_size=1000, dry_run=False):
    """Executa a migração de aplicações de vacinas usando BULK INSERT."""
    print("\n" + "="*80)
    print("MIGRAÇÃO: PET_ANIMAL_VACINA -> PET_VACINA (BULK INSERT)")
    print("="*80 + "\n")
    
    legacy_engine = get_engine_from_env("LEGACY_DB_URL")
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()
    
    # Garantir que a tabela de controle exista
    if not dry_run:
        ensure_controle_table(dest_engine, tenant_id)
    
    print("📊 Carregando dados de referência...")
    
    # 1. Carregar TODOS os mapeamentos de pets (1 query)
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
            pets_map[int(row[0])] = row[1]
    print(f"✓ {len(pets_map)} pets mapeados")
    
    # 2. Carregar TODOS os mapeamentos de vacinas (1 query)
    print("  - Mapeamento de vacinas...", end=" ", flush=True)
    vacinas_map = {}
    with dest_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT sValorChaveOrigem, sValorChaveDestino 
            FROM CONTROLE_MIGRACAO_LEGADO
            WHERE sCdTenant = '{tenant_id}'
              AND sTabelaOrigem = 'PET_VACINA'
              AND sTabelaDestino = 'VACINA'
        """))
        for row in result:
            vacinas_map[int(row[0])] = str(row[1])  # UUID da vacina no destino
    print(f"✓ {len(vacinas_map)} vacinas mapeadas")
    
    # 3. Carregar aplicações já migradas (1 query)
    print("  - Aplicações já migradas...", end=" ", flush=True)
    aplicacoes_migradas = {}
    with dest_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT sValorChaveOrigem, sValorChaveDestino 
            FROM CONTROLE_MIGRACAO_LEGADO
            WHERE sCdTenant = '{tenant_id}'
              AND sTabelaOrigem = 'PET_ANIMAL_VACINA'
              AND sTabelaDestino = 'PET_VACINA'
        """))
        for row in result:
            aplicacoes_migradas[int(row[0])] = row[1]
    print(f"✓ {len(aplicacoes_migradas)} aplicações")
    
    print("\n🔄 Carregando registros da origem...")
    
    # Consulta principal - pegar todas as aplicações
    select_sql = text("""
        SELECT 
            Codigo,
            Animal,
            Vacina,
            DataAplicacao,
            DataPrevista,
            Partida,
            Laboratorio
        FROM PET_ANIMAL_VACINA
        ORDER BY Codigo
    """)
    
    # Estatísticas
    total = 0
    sem_pet = 0
    sem_vacina = 0
    
    aplicacoes_para_inserir = []
    aplicacoes_para_atualizar = []
    controle_para_inserir = []
    
    with legacy_engine.connect() as conn:
        result = conn.execute(select_sql)
        all_rows = result.fetchall()
        
        print(f"  Total de aplicações no legado: {len(all_rows)}\n")
        
        for row in all_rows:
            total += 1
            
            if total % 1000 == 0:
                print(f"  [{total}] Processando...", flush=True)
            
            codigo_aplicacao = int(row.Codigo)
            codigo_animal = int(row.Animal) if row.Animal else None
            codigo_vacina = int(row.Vacina) if row.Vacina else None
            
            # Validar dependências (usando dados em memória)
            if not codigo_animal or codigo_animal not in pets_map:
                sem_pet += 1
                continue
            
            if not codigo_vacina or codigo_vacina not in vacinas_map:
                sem_vacina += 1
                continue
            
            # Buscar IDs mapeados
            sCdPet = pets_map[codigo_animal]
            sCdVacina = vacinas_map[codigo_vacina]
            
            # Mapear registro
            aplicacao = map_origem_to_destino(row, tenant_id, sCdPet, sCdVacina)
            
            if dry_run:
                aplicacoes_para_inserir.append(aplicacao)
                continue
            
            # Verificar se já foi migrado
            if codigo_aplicacao in aplicacoes_migradas:
                # Atualizar
                aplicacao['sCdPetVacina'] = aplicacoes_migradas[codigo_aplicacao]
                aplicacoes_para_atualizar.append(aplicacao)
            else:
                # Inserir novo
                sCdPetVacina = str(uuid.uuid4())
                aplicacao['sCdPetVacina'] = sCdPetVacina
                aplicacoes_para_inserir.append(aplicacao)
                
                # Preparar registro de controle
                controle_para_inserir.append({
                    'sCdTenant': tenant_id,
                    'sTabelaOrigem': 'PET_ANIMAL_VACINA',
                    'sCampoChaveOrigem': 'Codigo',
                    'sValorChaveOrigem': str(codigo_aplicacao),
                    'sTabelaDestino': 'PET_VACINA',
                    'sCampoChaveDestino': 'sCdPetVacina',
                    'sValorChaveDestino': sCdPetVacina,
                    'dtMigracao': datetime.now()
                })
    
    if dry_run:
        print(f"\n[DRY-RUN] Simulação concluída!")
        print(f"  Total processado: {total}")
        print(f"  Seriam inseridos: {len(aplicacoes_para_inserir)}")
        print(f"  Seriam atualizados: {len(aplicacoes_para_atualizar)}")
        print(f"  Sem pet migrado: {sem_pet}")
        print(f"  Sem vacina migrada: {sem_vacina}")
        return total
    
    print(f"\n💾 Salvando no banco de dados...")
    
    # BULK INSERT de aplicações novas
    if aplicacoes_para_inserir:
        print(f"  - Inserindo {len(aplicacoes_para_inserir)} aplicações novas...", end=" ", flush=True)
        with dest_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO PET_VACINA (
                    sCdPetVacina, sCdTenant, sCdPet, sCdVacina, sCdUsuario,
                    sDsPartida, tDtPrevista, tDtAplicacao, sDsLaboratorio,
                    sDsLocalAplicacao, bFlPreAutorizado, tDtCriacao, tDtAlteracao
                )
                VALUES (
                    :sCdPetVacina, :sCdTenant, :sCdPet, :sCdVacina, :sCdUsuario,
                    :sDsPartida, :tDtPrevista, :tDtAplicacao, :sDsLaboratorio,
                    :sDsLocalAplicacao, :bFlPreAutorizado, :tDtCriacao, :tDtAlteracao
                )
            """), aplicacoes_para_inserir)
        print("✓")
    
    # BULK UPDATE de aplicações existentes
    if aplicacoes_para_atualizar:
        print(f"  - Atualizando {len(aplicacoes_para_atualizar)} aplicações existentes...", end=" ", flush=True)
        with dest_engine.begin() as conn:
            for aplicacao in aplicacoes_para_atualizar:
                conn.execute(text("""
                    UPDATE PET_VACINA SET
                        sCdPet = :sCdPet,
                        sCdVacina = :sCdVacina,
                        sCdUsuario = :sCdUsuario,
                        sDsPartida = :sDsPartida,
                        tDtPrevista = :tDtPrevista,
                        tDtAplicacao = :tDtAplicacao,
                        sDsLaboratorio = :sDsLaboratorio,
                        sDsLocalAplicacao = :sDsLocalAplicacao,
                        bFlPreAutorizado = :bFlPreAutorizado,
                        tDtAlteracao = :tDtAlteracao
                    WHERE sCdPetVacina = :sCdPetVacina
                """), aplicacao)
        print("✓")
    
    # BULK INSERT na tabela de controle
    if controle_para_inserir:
        print(f"  - Registrando {len(controle_para_inserir)} mapeamentos...", end=" ", flush=True)
        with dest_engine.begin() as conn:
            # Deletar registros antigos antes de inserir (evitar duplicatas)
            # Fazer em chunks de 1000 para evitar limite de SQL
            codigos_origem = [c['sValorChaveOrigem'] for c in controle_para_inserir]
            if codigos_origem:
                for i in range(0, len(codigos_origem), 1000):
                    chunk = codigos_origem[i:i+1000]
                    placeholders = ','.join([f"'{c}'" for c in chunk])
                    conn.execute(text(f"""
                        DELETE FROM CONTROLE_MIGRACAO_LEGADO
                        WHERE sCdTenant = '{tenant_id}'
                        AND sTabelaOrigem = 'PET_ANIMAL_VACINA'
                        AND sValorChaveOrigem IN ({placeholders})
                    """))
            
            # Inserir novos registros (também em chunks de 1000)
            for i in range(0, len(controle_para_inserir), 1000):
                chunk = controle_para_inserir[i:i+1000]
                conn.execute(text("""
                    INSERT INTO CONTROLE_MIGRACAO_LEGADO (
                        sCdTenant, sTabelaOrigem, sCampoChaveOrigem, sValorChaveOrigem,
                        sTabelaDestino, sCampoChaveDestino, sValorChaveDestino, dtMigracao
                    )
                    VALUES (
                        :sCdTenant, :sTabelaOrigem, :sCampoChaveOrigem, :sValorChaveOrigem,
                        :sTabelaDestino, :sCampoChaveDestino, :sValorChaveDestino, :dtMigracao
                    )
                """), chunk)
        print("✓")
    
    print("\n" + "="*80)
    print("✓ Migração finalizada!")
    print(f"  Total processado: {total}")
    print(f"  Inseridos: {len(aplicacoes_para_inserir)}")
    print(f"  Atualizados: {len(aplicacoes_para_atualizar)}")
    print(f"  Sem pet migrado: {sem_pet}")
    print(f"  Sem vacina migrada: {sem_vacina}")
    print("="*80 + "\n")
    
    return total


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migração de Aplicações de Vacinas (BULK)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Tamanho do batch")
    parser.add_argument("--dry-run", action="store_true", help="Simula migração sem inserir dados")
    
    args = parser.parse_args()
    
    migrate_aplicacoes_vacinas_bulk(batch_size=args.batch_size, dry_run=args.dry_run)
