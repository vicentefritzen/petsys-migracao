"""
Migração de Aplicações de Vacinas (Carteira de Vacinas)
PET_ANIMAL_VACINA (origem) -> PET_VACINA (destino)

Migra o histórico de vacinas aplicadas e previstas dos pets.
"""
import sys
from pathlib import Path

# Adicionar src ao path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import uuid
from datetime import datetime
from sqlalchemy import text
from common.db_utils import (
    get_engine_from_env, 
    ensure_controle_table, 
    insert_controle, 
    get_tenant_id,
    get_default_vet_user_id
)


def buscar_pet_migrado(dest_engine, tenant_id: str, codigo_animal_origem: str):
    """
    Busca o UUID do pet migrado na tabela de controle.
    
    Args:
        dest_engine: Engine do banco destino
        tenant_id: ID do tenant
        codigo_animal_origem: Código do animal na origem
    
    Returns:
        str: UUID do pet no destino, ou None se não encontrado
    """
    sql = text("""
        SELECT sValorChaveDestino 
        FROM CONTROLE_MIGRACAO_LEGADO
        WHERE sCdTenant = :tenant
          AND sTabelaOrigem = 'PET_ANIMAL'
          AND sValorChaveOrigem = :codigo
    """)
    
    with dest_engine.connect() as conn:
        result = conn.execute(sql, {"tenant": tenant_id, "codigo": codigo_animal_origem})
        row = result.fetchone()
        return str(row[0]) if row else None


def buscar_vacina_migrada(dest_engine, tenant_id: str, codigo_vacina_origem: str):
    """
    Busca o UUID da vacina migrada na tabela de controle.
    
    Args:
        dest_engine: Engine do banco destino
        tenant_id: ID do tenant
        codigo_vacina_origem: Código da vacina na origem
    
    Returns:
        str: UUID da vacina no destino, ou None se não encontrado
    """
    sql = text("""
        SELECT sValorChaveDestino 
        FROM CONTROLE_MIGRACAO_LEGADO
        WHERE sCdTenant = :tenant
          AND sTabelaOrigem = 'PET_VACINA'
          AND sValorChaveOrigem = :codigo
    """)
    
    with dest_engine.connect() as conn:
        result = conn.execute(sql, {"tenant": tenant_id, "codigo": codigo_vacina_origem})
        row = result.fetchone()
        return str(row[0]) if row else None


def map_origem_to_destino(row, tenant_id: str, dest_engine):
    """
    Mapeia um registro da tabela PET_ANIMAL_VACINA (origem) para PET_VACINA (destino).
    
    Mapeamento:
    - Codigo -> (controle)
    - Animal -> sCdPet (via tabela de controle)
    - Vacina -> sCdVacina (via tabela de controle)
    - Partida -> sDsPartida
    - DataPrevista -> tDtPrevista
    - DataAplicacao -> tDtAplicacao
    - Laboratorio -> sDsLaboratorio
    - LocalAplicacao -> sDsLocalAplicacao
    - PreAutorizado -> bFlPreAutorizado
    - (padrão .env) -> sCdUsuario (veterinário padrão)
    
    Args:
        row: Dicionário com os dados da tabela origem
        tenant_id: ID do tenant
        dest_engine: Engine do banco destino
    
    Returns:
        dict: Dados mapeados para inserção na tabela destino, ou None se pet/vacina não encontrados
    """
    def safe(val, default=""):
        if val is None:
            return default
        # Remover espaços extras
        if isinstance(val, str):
            return val.strip() if val.strip() else default
        return val
    
    def safe_bool(val, default=False):
        if val is None:
            return default
        try:
            return bool(int(val))
        except (ValueError, TypeError):
            return default
    
    # Buscar pet e vacina migrados
    codigo_animal = str(row.get("Animal"))
    codigo_vacina = str(row.get("Vacina"))
    
    sCdPet = buscar_pet_migrado(dest_engine, tenant_id, codigo_animal)
    sCdVacina = buscar_vacina_migrada(dest_engine, tenant_id, codigo_vacina)
    
    # Se não encontrar pet ou vacina, retornar None (será pulado)
    if not sCdPet or not sCdVacina:
        return None
    
    # Gerar UUID para o registro
    sCdPetVacina = str(uuid.uuid4())
    
    # Campos diretos
    sDsPartida = safe(row.get("Partida"), None)
    sDsLaboratorio = safe(row.get("Laboratorio"), None)
    sDsLocalAplicacao = safe(row.get("LocalAplicacao"), None)
    
    # Datas
    tDtPrevista = row.get("DataPrevista")
    tDtAplicacao = row.get("DataAplicacao")
    
    # PreAutorizado
    bFlPreAutorizado = safe_bool(row.get("PreAutorizado"), False)
    
    # Timestamps
    tDtCriacao = datetime.now()
    tDtAlteracao = datetime.now() if tDtAplicacao else None
    
    # Usuário veterinário padrão
    sCdUsuario = get_default_vet_user_id()
    
    return {
        "sCdPetVacina": sCdPetVacina,
        "sCdTenant": tenant_id,
        "sCdPet": sCdPet,
        "sCdVacina": sCdVacina,
        "sCdUsuario": sCdUsuario,
        "sDsPartida": sDsPartida,
        "tDtPrevista": tDtPrevista,
        "tDtAplicacao": tDtAplicacao,
        "sDsLaboratorio": sDsLaboratorio,
        "sDsLocalAplicacao": sDsLocalAplicacao,
        "bFlPreAutorizado": bFlPreAutorizado,
        "tDtCriacao": tDtCriacao,
        "tDtAlteracao": tDtAlteracao,
    }


def insert_or_update_pet_vacina(dest_engine, registro: dict, dry_run: bool = False):
    """
    Insere ou atualiza registro na tabela PET_VACINA.
    
    Verifica se a aplicação já existe (por pet + vacina + data prevista).
    Se existir, atualiza. Caso contrário, insere novo.
    
    Args:
        dest_engine: Engine do banco destino
        registro: Dicionário com dados do registro
        dry_run: Se True, apenas simula (não insere/atualiza)
    
    Returns:
        str: ID do registro inserido/atualizado
    """
    check_sql = text("""
        SELECT sCdPetVacina FROM PET_VACINA 
        WHERE sCdTenant = :sCdTenant 
          AND sCdPet = :sCdPet
          AND sCdVacina = :sCdVacina
          AND tDtPrevista = :tDtPrevista
    """)
    
    update_sql = text("""
        UPDATE PET_VACINA SET
            sCdUsuario = :sCdUsuario,
            sDsPartida = :sDsPartida,
            tDtAplicacao = :tDtAplicacao,
            sDsLaboratorio = :sDsLaboratorio,
            sDsLocalAplicacao = :sDsLocalAplicacao,
            bFlPreAutorizado = :bFlPreAutorizado,
            tDtAlteracao = :tDtAlteracao
        WHERE sCdPetVacina = :sCdPetVacina
    """)
    
    insert_sql = text("""
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
    """)

    if dry_run:
        status = "APLICADA" if registro.get("tDtAplicacao") else "PREVISTA"
        print(f"  [dry-run] {status}")
        return registro["sCdPetVacina"]

    with dest_engine.begin() as conn:
        # Verificar se já existe
        result = conn.execute(check_sql, {
            "sCdTenant": registro["sCdTenant"],
            "sCdPet": registro["sCdPet"],
            "sCdVacina": registro["sCdVacina"],
            "tDtPrevista": registro["tDtPrevista"]
        })
        existing = result.fetchone()
        
        if existing:
            # Atualizar registro existente
            scd_existente = str(existing[0])
            registro_update = registro.copy()
            registro_update["sCdPetVacina"] = scd_existente
            
            conn.execute(update_sql, registro_update)
            status = "Aplicada" if registro.get("tDtAplicacao") else "Prevista"
            print(f"  ✓ Atualizado: {status}")
            
            return scd_existente
        else:
            # Inserir novo registro
            conn.execute(insert_sql, registro)
            status = "Aplicada" if registro.get("tDtAplicacao") else "Prevista"
            print(f"  ✓ Inserido: {status}")
    
    return registro["sCdPetVacina"]


def migrate_aplicacoes_vacinas(batch_size=500, dry_run=False):
    """
    Executa a migração de aplicações de vacinas.
    
    Args:
        batch_size: Quantidade de registros por batch
        dry_run: Se True, apenas simula (não insere)
    
    Returns:
        dict: Estatísticas da migração
    """
    print("\n" + "="*70)
    print("MIGRAÇÃO: PET_ANIMAL_VACINA (origem) -> PET_VACINA (destino)")
    print("="*70 + "\n")
    
    legacy_engine = get_engine_from_env("LEGACY_DB_URL")
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()

    # Garantir que a tabela de controle exista
    ensure_controle_table(dest_engine, tenant_id)

    # Ler aplicações de vacinas da origem
    select_sql = text("""
        SELECT * FROM PET_ANIMAL_VACINA 
        ORDER BY Codigo
    """)

    total = 0
    inseridos = 0
    atualizados = 0
    pulados_pet = 0
    pulados_vacina = 0

    with legacy_engine.connect() as src_conn:
        result = src_conn.execution_options(stream_results=True).execute(select_sql)
        
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            
            for r in rows:
                row = dict(r._mapping)
                codigo_origem = str(row.get("Codigo"))
                codigo_animal = str(row.get("Animal"))
                codigo_vacina = str(row.get("Vacina"))
                data_prevista = row.get("DataPrevista")
                data_aplicacao = row.get("DataAplicacao")
                
                status = f"Aplicada em {data_aplicacao.strftime('%d/%m/%Y')}" if data_aplicacao else f"Prevista para {data_prevista.strftime('%d/%m/%Y')}"
                
                print(f"[{total + 1}] Processando: Animal={codigo_animal}, Vacina={codigo_vacina} ({status})")
                
                # Mapear
                registro = map_origem_to_destino(row, tenant_id, dest_engine)
                
                # Se não encontrou pet ou vacina, pular
                if registro is None:
                    # Verificar qual não foi encontrado
                    pet_exists = buscar_pet_migrado(dest_engine, tenant_id, codigo_animal)
                    vacina_exists = buscar_vacina_migrada(dest_engine, tenant_id, codigo_vacina)
                    
                    if not pet_exists:
                        print(f"  ⚠ Pulado: Pet (Animal={codigo_animal}) não encontrado")
                        pulados_pet += 1
                    elif not vacina_exists:
                        print(f"  ⚠ Pulado: Vacina (Codigo={codigo_vacina}) não encontrada")
                        pulados_vacina += 1
                    
                    total += 1
                    continue
                
                # Verificar se já existe (para estatísticas)
                if not dry_run:
                    with dest_engine.begin() as conn:
                        check = conn.execute(
                            text("""
                                SELECT sCdPetVacina FROM PET_VACINA 
                                WHERE sCdTenant = :t 
                                  AND sCdPet = :p 
                                  AND sCdVacina = :v
                                  AND tDtPrevista = :d
                            """),
                            {
                                "t": tenant_id, 
                                "p": registro["sCdPet"],
                                "v": registro["sCdVacina"],
                                "d": registro["tDtPrevista"]
                            }
                        )
                        exists = check.fetchone() is not None
                
                # Inserir ou atualizar
                sCdPetVacina = insert_or_update_pet_vacina(dest_engine, registro, dry_run=dry_run)
                
                # Atualizar estatísticas
                if not dry_run:
                    if exists:
                        atualizados += 1
                    else:
                        inseridos += 1
                
                # Registrar mapeamento na tabela de controle (apenas se não for dry-run)
                if not dry_run:
                    insert_controle(
                        dest_engine, tenant_id,
                        "PET_ANIMAL_VACINA", "Codigo", codigo_origem,
                        "PET_VACINA", "sCdPetVacina", sCdPetVacina
                    )
                
                total += 1

    print("\n" + "="*70)
    print("✓ Migração finalizada!")
    print(f"  Total processado: {total}")
    if not dry_run:
        print(f"  Inseridos: {inseridos}")
        print(f"  Atualizados: {atualizados}")
    print(f"  Pulados (pet não encontrado): {pulados_pet}")
    print(f"  Pulados (vacina não encontrada): {pulados_vacina}")
    print("="*70 + "\n")
    
    return {
        "total": total,
        "inseridos": inseridos,
        "atualizados": atualizados,
        "pulados_pet": pulados_pet,
        "pulados_vacina": pulados_vacina
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migração de Aplicações de Vacinas")
    parser.add_argument("--dry-run", action="store_true", help="Executar em modo simulação")
    parser.add_argument("--batch-size", type=int, default=500, help="Tamanho do batch")
    
    args = parser.parse_args()
    
    migrate_aplicacoes_vacinas(batch_size=args.batch_size, dry_run=args.dry_run)
