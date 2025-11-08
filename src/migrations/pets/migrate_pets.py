"""
Migração de Pets (PET_ANIMAL -> PET)
"""
import uuid
import os
from datetime import datetime, date
from sqlalchemy import text
from common.db_utils import get_engine_from_env, ensure_controle_table, insert_controle, get_tenant_id
from common.fuzzy_utils import (
    buscar_raca_por_nome, 
    buscar_cor_por_nome, 
    mapear_sexo, 
    mapear_porte,
    mapear_especie_por_raca
)


def get_raca_info_from_legacy(legacy_engine, codigo_raca: int):
    """Busca informações da raça no banco legado."""
    select_sql = text("""
SELECT Descricao, Especie FROM PET_RACA WHERE Codigo = :codigo
""")
    
    with legacy_engine.connect() as conn:
        result = conn.execute(select_sql, {"codigo": codigo_raca})
        row = result.fetchone()
        if row:
            return {"descricao": row[0], "especie": int(row[1])}
    return None


def get_cor_info_from_legacy(legacy_engine, codigo_cor: int):
    """Busca descrição da cor no banco legado."""
    select_sql = text("""
SELECT Descricao FROM PET_COR WHERE Codigo = :codigo
""")
    
    with legacy_engine.connect() as conn:
        result = conn.execute(select_sql, {"codigo": codigo_cor})
        row = result.fetchone()
        if row:
            return row[0]
    return None


def get_pessoa_id_by_cliente_codigo(dest_engine, codigo_cliente: int, tenant_id: str):
    """
    Busca o sCdPessoa no destino pelo código do cliente no legado.
    Usa a tabela CONTROLE_MIGRACAO_LEGADO para encontrar o mapeamento.
    Valida se a pessoa realmente existe na tabela PESSOA.
    """
    # Buscar na tabela de controle
    select_controle_sql = text("""
SELECT sValorChaveDestino 
FROM CONTROLE_MIGRACAO_LEGADO
WHERE sCdTenant = :tenant
  AND sTabelaOrigem = 'PET_CLIENTE'
  AND sCampoChaveOrigem = 'Codigo'
  AND sValorChaveOrigem = :codigo
  AND sTabelaDestino = 'PESSOA'
""")
    
    # Validar se pessoa existe
    validate_pessoa_sql = text("""
SELECT sCdPessoa FROM PESSOA
WHERE sCdPessoa = :pessoa_id AND sCdTenant = :tenant
""")
    
    with dest_engine.connect() as conn:
        # Buscar ID na tabela de controle
        result = conn.execute(select_controle_sql, {
            "tenant": tenant_id,
            "codigo": str(codigo_cliente)
        })
        row = result.fetchone()
        
        if row:
            pessoa_id = str(row[0])
            
            # Validar se pessoa realmente existe
            result_validate = conn.execute(validate_pessoa_sql, {
                "pessoa_id": pessoa_id,
                "tenant": tenant_id
            })
            
            if result_validate.fetchone():
                return pessoa_id
    
    return None


def map_animal_to_pet_optimized(row, tenant_id: str, 
                                  racas_legado: dict, racas_destino: dict,
                                  cores_legado: dict, cores_destino: dict,
                                  sCdPessoa: str):
    """
    Versão otimizada que usa dados já carregados em memória.
    Não faz queries no banco - usa apenas dicionários.
    """
    def safe(val, default=""):
        return default if val is None else val
    
    sNmPet = safe(row.get("Nome"), "SEM NOME")
    
    # Data de nascimento
    dt_nasc = row.get("DataNascimento")
    if isinstance(dt_nasc, (datetime, date)):
        tDtNascimento = dt_nasc if isinstance(dt_nasc, date) else dt_nasc.date()
    else:
        try:
            tDtNascimento = datetime.fromisoformat(str(dt_nasc)).date()
        except:
            tDtNascimento = None
    
    # Data de cadastro
    dt_cad = row.get("DataCadastro")
    if isinstance(dt_cad, datetime):
        tDtCadastro = dt_cad
    else:
        try:
            tDtCadastro = datetime.fromisoformat(str(dt_cad))
        except:
            tDtCadastro = datetime.utcnow()
    
    # Ativo
    ativo = row.get("Ativo")
    try:
        bFlAtivo = bool(int(ativo))
    except:
        bFlAtivo = True
    
    # Observações
    obs = safe(row.get("Observacoes"), None)
    if obs and len(obs) > 500:
        obs = obs[:497] + "..."
    sDsObservacoes = obs
    
    # Raça e Espécie (usando dados em memória)
    codigo_raca = row.get("Raca")
    if codigo_raca is not None:
        codigo_raca = int(codigo_raca)
    raca_info = racas_legado.get(codigo_raca, {})
    nome_raca = raca_info.get('descricao', '')
    especie_legado = raca_info.get('especie')
    
    # Espécie (já temos do dicionário)
    nCdEspecie = int(especie_legado) if especie_legado else 1  # Default CANINA
    
    # Fuzzy match da raça (versão otimizada - só dicionário)
    nCdRaca = None
    if nome_raca and racas_destino:
        try:
            from rapidfuzz import fuzz, process
            result = process.extractOne(
                nome_raca, 
                racas_destino.keys(), 
                scorer=fuzz.ratio,
                score_cutoff=75
            )
            if result:
                descricao_match = result[0]
                nCdRaca = racas_destino[descricao_match]
        except ImportError:
            # Fallback: match exato
            nome_upper = nome_raca.upper().strip()
            for desc, cod in racas_destino.items():
                if desc.upper().strip() == nome_upper:
                    nCdRaca = cod
                    break
    
    # Sexo
    codigo_sexo = row.get("Sexo")
    if codigo_sexo is not None:
        codigo_sexo = int(codigo_sexo)
    nCdSexo = mapear_sexo(codigo_sexo)
    
    # Porte
    codigo_porte = row.get("Porte")
    if codigo_porte is not None:
        codigo_porte = int(codigo_porte)
    nCdPorte = mapear_porte(codigo_porte)
    
    # Cor (usando dados em memória)
    codigo_cor = row.get("Cor")
    if codigo_cor is not None:
        codigo_cor = int(codigo_cor)
    nome_cor = cores_legado.get(codigo_cor, '')
    
    # Fuzzy match da cor (versão otimizada - só dicionário)
    nCdCor = None
    if nome_cor and cores_destino:
        try:
            from rapidfuzz import fuzz, process
            result = process.extractOne(
                nome_cor, 
                cores_destino.keys(), 
                scorer=fuzz.ratio,
                score_cutoff=70
            )
            if result:
                descricao_match = result[0]
                nCdCor = cores_destino[descricao_match]
        except ImportError:
            # Fallback: match exato
            nome_upper = nome_cor.upper().strip()
            for desc, cod in cores_destino.items():
                if desc.upper().strip() == nome_upper:
                    nCdCor = cod
                    break
    
    return {
        "sCdTenant": tenant_id,
        "sCdPessoa": sCdPessoa,
        "sNmPet": sNmPet,
        "nCdEspecie": nCdEspecie,
        "nCdRaca": nCdRaca,
        "nCdSexo": nCdSexo,
        "nCdPorte": nCdPorte,
        "nCdCor": nCdCor,
        "tDtNascimento": tDtNascimento,
        "nVlPeso": None,
        "sDsObservacoes": sDsObservacoes,
        "bFlAtivo": bFlAtivo,
        "tDtCadastro": tDtCadastro,
    }


def map_animal_to_pet(row, tenant_id: str, legacy_engine, dest_engine):
    """
    Mapeia um registro de PET_ANIMAL para PET.
    
    Origem (PET_ANIMAL):
    - Codigo: int (PK)
    - Nome: varchar(100)
    - DataNascimento: smalldatetime
    - Raca: int (FK -> PET_RACA)
    - Sexo: int (FK -> PET_SEXO)
    - Porte: int
    - Cor: int (FK -> PET_COR)
    - Proprietario: int (FK -> PET_CLIENTE)
    - DataCadastro: smalldatetime
    - Ativo: int (0/1)
    - Observacoes: text
    
    Destino (PET):
    - sCdPet: uniqueidentifier (PK)
    - sCdTenant: uniqueidentifier
    - sCdPessoa: uniqueidentifier (FK -> PESSOA)
    - sNmPet: nvarchar(100)
    - nCdEspecie: int (1=CANINA, 2=FELINA)
    - nCdRaca: int
    - nCdSexo: int
    - nCdPorte: int
    - nCdCor: int
    - tDtNascimento: date
    - nVlPeso: decimal (NULL)
    - sDsObservacoes: nvarchar(500)
    - bFlAtivo: bit
    - tDtCadastro: datetime2
    """
    def safe(val, default=""):
        return default if val is None else val
    
    sCdPet = str(uuid.uuid4())
    sNmPet = safe(row.get("Nome"), "SEM NOME")
    
    # Data de nascimento
    dt_nasc = row.get("DataNascimento")
    if isinstance(dt_nasc, (datetime, date)):
        tDtNascimento = dt_nasc if isinstance(dt_nasc, date) else dt_nasc.date()
    else:
        try:
            tDtNascimento = datetime.fromisoformat(str(dt_nasc)).date()
        except:
            tDtNascimento = None
    
    # Data de cadastro
    dt_cad = row.get("DataCadastro")
    if isinstance(dt_cad, datetime):
        tDtCadastro = dt_cad
    else:
        try:
            tDtCadastro = datetime.fromisoformat(str(dt_cad))
        except:
            tDtCadastro = datetime.utcnow()
    
    # Ativo
    ativo = row.get("Ativo")
    try:
        bFlAtivo = bool(int(ativo))
    except:
        bFlAtivo = True
    
    # Observações
    obs = safe(row.get("Observacoes"), None)
    if obs and len(obs) > 500:
        obs = obs[:497] + "..."
    sDsObservacoes = obs
    
    # Proprietário -> sCdPessoa
    codigo_proprietario = row.get("Proprietario")
    sCdPessoa = None
    if codigo_proprietario:
        sCdPessoa = get_pessoa_id_by_cliente_codigo(dest_engine, codigo_proprietario, tenant_id)
    
    if not sCdPessoa:
        # Sem proprietário válido, não pode migrar
        return None
    
    # Raça -> buscar no legado e fazer fuzzy match no destino
    codigo_raca_legado = row.get("Raca")
    raca_info = get_raca_info_from_legacy(legacy_engine, codigo_raca_legado)
    
    nCdRaca = None
    nCdEspecie = 1  # Default CANINA
    
    if raca_info:
        nCdEspecie = raca_info["especie"]
        nCdRaca, raca_matched, score = buscar_raca_por_nome(
            dest_engine, 
            raca_info["descricao"], 
            nCdEspecie,
            min_score=75  # Score mais permissivo para raças
        )
        
        if not nCdRaca:
            # Usar raça padrão: S.R.D. (código 7 para cães)
            nCdRaca = 7 if nCdEspecie == 1 else 33  # 33 = S.R.D. para gatos
    else:
        nCdRaca = 7  # S.R.D. para cães
    
    # Cor -> buscar no legado e fazer fuzzy match no destino
    codigo_cor_legado = row.get("Cor")
    cor_descricao = get_cor_info_from_legacy(legacy_engine, codigo_cor_legado)
    
    nCdCor = None
    if cor_descricao:
        nCdCor, cor_matched, score = buscar_cor_por_nome(dest_engine, cor_descricao, min_score=70)
        
        if not nCdCor:
            nCdCor = 5  # CARACTERISTICA (cor padrão quando não encontrar)
    else:
        nCdCor = 5  # CARACTERISTICA
    
    # Sexo -> mapeamento direto
    codigo_sexo_legado = row.get("Sexo", 1)
    nCdSexo = mapear_sexo(codigo_sexo_legado)
    
    # Porte -> mapeamento direto
    codigo_porte_legado = row.get("Porte", 1)
    nCdPorte = mapear_porte(codigo_porte_legado)
    
    return {
        "sCdTenant": tenant_id,
        "sCdPet": sCdPet,
        "sCdPessoa": sCdPessoa,
        "sNmPet": sNmPet,
        "nCdEspecie": nCdEspecie,
        "nCdRaca": nCdRaca,
        "nCdSexo": nCdSexo,
        "nCdPorte": nCdPorte,
        "nCdCor": nCdCor,
        "tDtNascimento": tDtNascimento,
        "nVlPeso": None,  # Peso não existe no legado
        "sDsObservacoes": sDsObservacoes,
        "bFlAtivo": bFlAtivo,
        "tDtCadastro": tDtCadastro,
    }


def insert_or_update_pet(dest_engine, pet: dict, codigo_animal_legado: int):
    """
    Insere ou atualiza pet na tabela PET.
    
    Verifica se já existe um pet migrado deste código de animal do legado.
    Se existir, atualiza. Caso contrário, insere novo.
    """
    # Verificar se já foi migrado anteriormente
    check_controle_sql = text("""
SELECT sValorChaveDestino 
FROM CONTROLE_MIGRACAO_LEGADO
WHERE sCdTenant = :tenant
  AND sTabelaOrigem = 'PET_ANIMAL'
  AND sCampoChaveOrigem = 'Codigo'
  AND sValorChaveOrigem = :codigo
  AND sTabelaDestino = 'PET'
""")
    
    update_sql = text("""
UPDATE PET SET
    sCdPessoa = :sCdPessoa,
    sNmPet = :sNmPet,
    nCdEspecie = :nCdEspecie,
    nCdRaca = :nCdRaca,
    nCdSexo = :nCdSexo,
    nCdPorte = :nCdPorte,
    nCdCor = :nCdCor,
    tDtNascimento = :tDtNascimento,
    nVlPeso = :nVlPeso,
    sDsObservacoes = :sDsObservacoes,
    bFlAtivo = :bFlAtivo,
    tDtCadastro = :tDtCadastro
WHERE sCdPet = :sCdPet
""")
    
    insert_sql = text("""
INSERT INTO PET (
    sCdTenant, sCdPet, sCdPessoa, sNmPet, nCdEspecie, nCdRaca,
    nCdSexo, nCdPorte, nCdCor, tDtNascimento, nVlPeso,
    sDsObservacoes, bFlAtivo, tDtCadastro
)
VALUES (
    :sCdTenant, :sCdPet, :sCdPessoa, :sNmPet, :nCdEspecie, :nCdRaca,
    :nCdSexo, :nCdPorte, :nCdCor, :tDtNascimento, :nVlPeso,
    :sDsObservacoes, :bFlAtivo, :tDtCadastro
)
""")
    
    with dest_engine.begin() as conn:
        # Verificar se já foi migrado
        result = conn.execute(check_controle_sql, {
            "tenant": pet["sCdTenant"],
            "codigo": str(codigo_animal_legado)
        })
        existing = result.fetchone()
        
        if existing:
            # Atualizar registro existente
            scd_pet_existente = str(existing[0])
            pet_update = pet.copy()
            pet_update["sCdPet"] = scd_pet_existente
            
            conn.execute(update_sql, pet_update)
            print("✓ Atualizado")
            
            return scd_pet_existente
        else:
            # Inserir novo pet
            conn.execute(insert_sql, pet)
            print("✓ Inserido")
    
    return pet["sCdPet"]


def migrate_pets(batch_size=500):
    """Executa a migração de pets."""
    print("\n" + "="*60)
    print("MIGRAÇÃO: PET_ANIMAL -> PET")
    print("="*60 + "\n")
    
    legacy_engine = get_engine_from_env("LEGACY_DB_URL")
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()
    
    # Garantir que a tabela de controle exista
    ensure_controle_table(dest_engine, tenant_id)
    
    # Preparar arquivo de log
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"pets_sem_proprietario_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    pets_sem_proprietario = []
    
    print("📊 Carregando dados de referência...")
    
    # 1. Carregar TODAS as raças do legado (1 query)
    print("  - Raças do legado...", end=" ", flush=True)
    racas_legado = {}
    with legacy_engine.connect() as conn:
        result = conn.execute(text("SELECT Codigo, Descricao, Especie FROM PET_RACA"))
        for row in result:
            racas_legado[row[0]] = {'descricao': row[1], 'especie': row[2]}
    print(f"✓ {len(racas_legado)} raças")
    
    # 2. Carregar TODAS as raças do destino (1 query)
    print("  - Raças do destino...", end=" ", flush=True)
    racas_destino = {}
    with dest_engine.connect() as conn:
        result = conn.execute(text("SELECT nCdRaca, sNmRaca FROM RACA WHERE bFlAtivo = 1"))
        for row in result:
            racas_destino[row[1].upper()] = row[0]
    print(f"✓ {len(racas_destino)} raças")
    
    # 3. Carregar TODAS as cores do destino (1 query)
    print("  - Cores do legado...", end=" ", flush=True)
    cores_legado = {}
    with legacy_engine.connect() as conn:
        result = conn.execute(text("SELECT Codigo, Descricao FROM PET_COR"))
        for row in result:
            cores_legado[row[0]] = row[1]
    print(f"✓ {len(cores_legado)} cores")
    
    # 4. Carregar TODAS as cores do destino (1 query)
    print("  - Cores do destino...", end=" ", flush=True)
    cores_destino = {}
    with dest_engine.connect() as conn:
        result = conn.execute(text("SELECT nCdCor, sNmCor FROM COR WHERE bFlAtivo = 1"))
        for row in result:
            cores_destino[row[1].upper()] = row[0]
    print(f"✓ {len(cores_destino)} cores")
    
    # 5. Carregar TODOS os mapeamentos de proprietários (1 query)
    print("  - Mapeamento de proprietários...", end=" ", flush=True)
    proprietarios_map = {}
    with dest_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT sValorChaveOrigem, sValorChaveDestino 
            FROM CONTROLE_MIGRACAO_LEGADO
            WHERE sCdTenant = '{tenant_id}'
              AND sTabelaOrigem = 'PET_CLIENTE'
              AND sTabelaDestino = 'PESSOA'
        """))
        for row in result:
            # sValorChaveOrigem vem como STRING do banco!
            proprietarios_map[int(row[0])] = row[1]
    print(f"✓ {len(proprietarios_map)} mapeamentos")
    
    # 6. Validar quais pessoas realmente existem (1 query com IN)
    print("  - Validando pessoas existentes...", end=" ", flush=True)
    pessoas_existentes = set()
    if proprietarios_map:
        pessoa_ids = list(proprietarios_map.values())
        # Dividir em chunks de 1000 para evitar limite do SQL
        for i in range(0, len(pessoa_ids), 1000):
            chunk = pessoa_ids[i:i+1000]
            placeholders = ','.join([f"'{p}'" for p in chunk])
            with dest_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT sCdPessoa FROM PESSOA 
                    WHERE sCdTenant = '{tenant_id}' AND sCdPessoa IN ({placeholders})
                """))
                for row in result:
                    # Converter UUID para string para comparação
                    pessoas_existentes.add(str(row[0]))
    print(f"✓ {len(pessoas_existentes)} pessoas existem")
    
    # 7. Carregar pets já migrados (1 query)
    print("  - Pets já migrados...", end=" ", flush=True)
    pets_migrados = {}
    with dest_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT sValorChaveOrigem, sValorChaveDestino 
            FROM CONTROLE_MIGRACAO_LEGADO
            WHERE sCdTenant = '{tenant_id}'
              AND sTabelaOrigem = 'PET_ANIMAL'
              AND sTabelaDestino = 'PET'
        """))
        for row in result:
            pets_migrados[int(row[0])] = row[1]
    print(f"✓ {len(pets_migrados)} pets")
    
    print("\n🔄 Processando pets...")
    
    # Ler TODOS os animais do legado (1 query)
    select_sql = text("SELECT * FROM PET_ANIMAL ORDER BY Codigo")
    
    total = 0
    sem_proprietario = 0
    pets_para_inserir = []
    pets_para_atualizar = []
    controle_para_inserir = []
    
    with legacy_engine.connect() as src_conn:
        result = src_conn.execute(select_sql)
        all_rows = result.fetchall()
        
        print(f"  Total de pets no legado: {len(all_rows)}\n")
        
        for r in all_rows:
            row = dict(r._mapping)
            codigo_animal = int(row.get("Codigo"))
            nome_animal = row.get("Nome", "SEM NOME")
            codigo_proprietario = row.get("Proprietario")
            
            # Converter Decimal para int
            if codigo_proprietario is not None:
                codigo_proprietario = int(codigo_proprietario)
            
            total += 1
            
            if total % 100 == 0:
                print(f"  [{total}] Processando...", flush=True)
            
            # Verificar se proprietário existe (usando dados em memória)
            if codigo_proprietario is None or codigo_proprietario not in proprietarios_map:
                sem_proprietario += 1
                pets_sem_proprietario.append({
                    'codigo_animal': codigo_animal,
                    'nome_animal': nome_animal,
                    'codigo_proprietario': codigo_proprietario
                })
                continue
            
            # Validar se pessoa realmente existe
            sCdPessoa = proprietarios_map[codigo_proprietario]
            if sCdPessoa not in pessoas_existentes:
                sem_proprietario += 1
                pets_sem_proprietario.append({
                    'codigo_animal': codigo_animal,
                    'nome_animal': nome_animal,
                    'codigo_proprietario': codigo_proprietario
                })
                continue
            
            # Mapear pet usando dados em memória
            pet = map_animal_to_pet_optimized(
                row, tenant_id, 
                racas_legado, racas_destino,
                cores_legado, cores_destino,
                sCdPessoa
            )
            
            if pet is None:
                continue
            
            # Verificar se já foi migrado
            if codigo_animal in pets_migrados:
                # Atualizar
                pet['sCdPet'] = pets_migrados[codigo_animal]
                pets_para_atualizar.append(pet)
            else:
                # Inserir novo
                sCdPet = str(uuid.uuid4())
                pet['sCdPet'] = sCdPet
                pets_para_inserir.append(pet)
                
                # Preparar registro de controle
                controle_para_inserir.append({
                    'sCdTenant': tenant_id,
                    'sTabelaOrigem': 'PET_ANIMAL',
                    'sCampoChaveOrigem': 'Codigo',
                    'sValorChaveOrigem': str(codigo_animal),
                    'sTabelaDestino': 'PET',
                    'sCampoChaveDestino': 'sCdPet',
                    'sValorChaveDestino': sCdPet,
                    'dtMigracao': datetime.now()
                })
    
    print(f"\n💾 Salvando no banco de dados...")
    
    # BULK INSERT de pets novos
    if pets_para_inserir:
        print(f"  - Inserindo {len(pets_para_inserir)} pets novos...", end=" ", flush=True)
        with dest_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO PET (
                    sCdTenant, sCdPet, sCdPessoa, sNmPet, nCdEspecie, nCdRaca,
                    nCdSexo, nCdPorte, nCdCor, tDtNascimento, nVlPeso,
                    sDsObservacoes, bFlAtivo, tDtCadastro
                )
                VALUES (
                    :sCdTenant, :sCdPet, :sCdPessoa, :sNmPet, :nCdEspecie, :nCdRaca,
                    :nCdSexo, :nCdPorte, :nCdCor, :tDtNascimento, :nVlPeso,
                    :sDsObservacoes, :bFlAtivo, :tDtCadastro
                )
            """), pets_para_inserir)
        print("✓")
    
    # BULK UPDATE de pets existentes
    if pets_para_atualizar:
        print(f"  - Atualizando {len(pets_para_atualizar)} pets existentes...", end=" ", flush=True)
        with dest_engine.begin() as conn:
            for pet in pets_para_atualizar:
                conn.execute(text("""
                    UPDATE PET SET
                        sCdPessoa = :sCdPessoa,
                        sNmPet = :sNmPet,
                        nCdEspecie = :nCdEspecie,
                        nCdRaca = :nCdRaca,
                        nCdSexo = :nCdSexo,
                        nCdPorte = :nCdPorte,
                        nCdCor = :nCdCor,
                        tDtNascimento = :tDtNascimento,
                        nVlPeso = :nVlPeso,
                        sDsObservacoes = :sDsObservacoes,
                        bFlAtivo = :bFlAtivo,
                        tDtCadastro = :tDtCadastro
                    WHERE sCdPet = :sCdPet
                """), pet)
        print("✓")
    
    # BULK INSERT na tabela de controle
    if controle_para_inserir:
        print(f"  - Registrando {len(controle_para_inserir)} mapeamentos...", end=" ", flush=True)
        with dest_engine.begin() as conn:
            # Deletar registros antigos antes de inserir (evitar duplicatas)
            codigos_origem = [c['sValorChaveOrigem'] for c in controle_para_inserir]
            if codigos_origem:
                placeholders = ','.join([f"'{c}'" for c in codigos_origem])
                conn.execute(text(f"""
                    DELETE FROM CONTROLE_MIGRACAO_LEGADO
                    WHERE sCdTenant = '{tenant_id}'
                    AND sTabelaOrigem = 'PET_ANIMAL'
                    AND sValorChaveOrigem IN ({placeholders})
                """))
            
            # Inserir novos registros
            conn.execute(text("""
                INSERT INTO CONTROLE_MIGRACAO_LEGADO (
                    sCdTenant, sTabelaOrigem, sCampoChaveOrigem, sValorChaveOrigem,
                    sTabelaDestino, sCampoChaveDestino, sValorChaveDestino, dtMigracao
                )
                VALUES (
                    :sCdTenant, :sTabelaOrigem, :sCampoChaveOrigem, :sValorChaveOrigem,
                    :sTabelaDestino, :sCampoChaveDestino, :sValorChaveDestino, :dtMigracao
                )
            """), controle_para_inserir)
        print("✓")
    
    # Gerar relatório de pets sem proprietário
    if pets_sem_proprietario:
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("RELATÓRIO: PETS SEM PROPRIETÁRIO\n")
            f.write(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write("="*80 + "\n\n")
            
            f.write(f"Total de pets sem proprietário: {len(pets_sem_proprietario)}\n\n")
            f.write("-"*80 + "\n")
            f.write(f"{'Cód. Pet':<12} {'Nome Pet':<30} {'Cód. Proprietário':<20}\n")
            f.write("-"*80 + "\n")
            
            for item in pets_sem_proprietario:
                codigo_prop = item['codigo_proprietario'] if item['codigo_proprietario'] else 'NULL'
                f.write(f"{item['codigo_animal']:<12} {item['nome_animal']:<30} {codigo_prop:<20}\n")
            
            f.write("-"*80 + "\n\n")
            f.write("AÇÕES NECESSÁRIAS:\n")
            f.write("1. Verificar se os proprietários (PET_CLIENTE) existem no banco legado\n")
            f.write("2. Executar migração de clientes para os códigos faltantes\n")
            f.write("3. Re-executar migração de pets após corrigir proprietários\n")
            f.write("\n" + "="*80 + "\n")
        
        print(f"\n📄 Relatório gerado: {log_file}")
    
    print("\n" + "="*60)
    print("✓ Migração finalizada!")
    print(f"  Total processado: {total}")
    print(f"  Sem proprietário: {sem_proprietario}")
    if pets_sem_proprietario:
        print(f"  Relatório: {log_file}")
    print("="*60 + "\n")
    
    return total


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migração de Pets")
    parser.add_argument("--batch-size", type=int, default=500, help="Tamanho do batch")
    parser.add_argument("--dry-run", action="store_true", help="Simula migração sem inserir dados")
    
    args = parser.parse_args()
    
    migrate_pets(batch_size=args.batch_size, dry_run=args.dry_run)
