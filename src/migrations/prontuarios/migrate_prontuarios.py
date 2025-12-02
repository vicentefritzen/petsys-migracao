"""
Migração de Prontuários - Parsing Complexo de Texto

Migra dados de PET_ANIMAL_PRONTUARIO (legado) para PRONTUARIO e RECEITA_MEDICA (destino).

O campo "Tag" contém múltiplos registros em formato texto que precisam ser parseados:
- Padrão: [DD/MM/YYYY HH:MM:SS - RESPONSÁVEL]:conteúdo
- Tipos: Prontuário normal, RECEITA_MEDICA, Lab (CITOVET), etc.
- Receitas herdam veterinário do prontuário imediatamente anterior

Origem:  PET_ANIMAL_PRONTUARIO (Codigo, Animal, Tag)
Destino: PRONTUARIO (sCdProntuario, sCdPet, tDtRegistro, sCdUsuarioRegistro, sDsProntuario)
         RECEITA_MEDICA (sCdReceitaMedica, sCdPet, tDtRegistro, sCdUsuarioRegistro, sDsReceitaMedica)
"""
import sys
import re
import logging
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal
import uuid

# Adicionar src ao path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import text
from common.db_utils import get_engine_from_env, get_tenant_id

try:
    from rapidfuzz import fuzz, process
    FUZZY_LIB = "rapidfuzz"
except ImportError:
    try:
        from fuzzywuzzy import fuzz, process
        FUZZY_LIB = "fuzzywuzzy"
    except ImportError:
        print("⚠ AVISO: Instale rapidfuzz para fuzzy matching de veterinários")
        print("Execute: pipenv install rapidfuzz")
        FUZZY_LIB = None

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/migracao_prontuarios.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_default_vet_fallback():
    """Retorna nome da veterinária padrão quando não conseguir identificar."""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    return os.getenv("DEFAULT_VET_FALLBACK_NAME", "DRA. JULIANA FARBER METZLER")


def parse_prontuario_entries(tag_text: str):
    """
    Faz o parse do campo Tag para extrair registros individuais.
    
    Padrão esperado: [DD/MM/YYYY HH:MM:SS - RESPONSÁVEL]:conteúdo
    
    Args:
        tag_text: Texto completo do campo Tag
    
    Returns:
        list: Lista de dicts com {data, responsavel, conteudo, tipo}
    """
    if not tag_text or not tag_text.strip():
        return []
    
    # Regex para encontrar padrão [data - responsável]:
    # Grupo 1: data/hora, Grupo 2: responsável, Grupo 3: conteúdo até próximo [
    pattern = r'\[(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})\s*-\s*([^\]]+)\]:\s*([^\[]*)'
    
    matches = re.findall(pattern, tag_text, re.DOTALL)
    
    entries = []
    for match in matches:
        data_str, responsavel, conteudo = match
        
        # Parse da data
        try:
            data = datetime.strptime(data_str.strip(), '%d/%m/%Y %H:%M:%S')
        except ValueError:
            logger.warning(f"Formato de data inválido: {data_str}")
            continue
        
        # Limpar responsável e conteúdo
        responsavel = responsavel.strip()
        conteudo = conteudo.strip()
        
        if not conteudo:
            continue
        
        # Determinar tipo
        tipo = 'PRONTUARIO'
        responsavel_upper = responsavel.upper()
        
        if 'RECEITA' in responsavel_upper:
            tipo = 'RECEITA_MEDICA'
        elif any(lab in responsavel_upper for lab in ['CITOVET', 'LABVET', 'LABORATORIO']):
            tipo = 'LABORATORIO'
        
        entries.append({
            'data': data,
            'responsavel': responsavel,
            'conteudo': conteudo,
            'tipo': tipo
        })
    
    # Ordenar por data
    entries.sort(key=lambda x: x['data'])
    
    return entries


def find_veterinario_by_name(nome: str, veterinarios_map: dict, min_score: int = 70):
    """
    Busca veterinário por nome usando fuzzy matching.
    
    Args:
        nome: Nome para buscar
        veterinarios_map: Dict com {nome: sCdUsuario}
        min_score: Score mínimo para match (0-100)
    
    Returns:
        str: sCdUsuario do veterinário ou None se não encontrar
    """
    if not FUZZY_LIB or not nome or not veterinarios_map:
        return None
    
    # Normalizar nome
    nome_normalizado = nome.upper().strip()
    
    # Tentar match exato primeiro
    for vet_nome, vet_id in veterinarios_map.items():
        if vet_nome.upper() == nome_normalizado:
            return vet_id
    
    # Fuzzy matching
    nomes_disponiveis = list(veterinarios_map.keys())
    result = process.extractOne(nome, nomes_disponiveis, scorer=fuzz.ratio)
    
    if result and result[1] >= min_score:
        nome_encontrado = result[0]
        logger.info(f"Fuzzy match: '{nome}' → '{nome_encontrado}' (score: {result[1]})")
        return veterinarios_map[nome_encontrado]
    
    logger.warning(f"Veterinário não encontrado: '{nome}' (melhor score: {result[1] if result else 0})")
    return None


def associate_receita_to_vet(
    receita_entry: dict,
    previous_entries: list,
    veterinarios_map: dict,
    default_vet_id: str,
    max_days_diff: int = 1
):
    """
    Associa uma receita médica ao veterinário anterior mais próximo.
    
    Args:
        receita_entry: Entry da receita
        previous_entries: Entries anteriores (já processados)
        veterinarios_map: Mapa de veterinários
        default_vet_id: ID do veterinário padrão (fallback)
        max_days_diff: Diferença máxima em dias
    
    Returns:
        str: sCdUsuario do veterinário responsável
    """
    receita_data = receita_entry['data']
    
    # Buscar prontuário anterior mais próximo (não receita)
    for entry in reversed(previous_entries):
        if entry['tipo'] != 'RECEITA_MEDICA':
            diff_days = (receita_data - entry['data']).days
            
            if diff_days <= max_days_diff:
                # Tentar encontrar veterinário
                vet_id = find_veterinario_by_name(entry['responsavel'], veterinarios_map)
                if vet_id:
                    logger.info(
                        f"Receita de {receita_data} associada a {entry['responsavel']} "
                        f"({diff_days} dias de diferença)"
                    )
                    return vet_id
    
    # Fallback: usar veterinário padrão
    logger.warning(
        f"Receita de {receita_data} sem veterinário anterior próximo. "
        f"Usando fallback: {get_default_vet_fallback()}"
    )
    return default_vet_id


def associate_receita_to_previous_vet(
    previous_entries: list,
    default_vet_id: str
):
    """
    Associa uma receita médica ao veterinário do entry imediatamente anterior.
    
    Args:
        previous_entries: Entries anteriores já processados (ordem cronológica)
        default_vet_id: ID do veterinário padrão (fallback)
    
    Returns:
        str: sCdUsuario do veterinário responsável
    """
    # Buscar o último entry que não seja RECEITA_MEDICA
    for entry in reversed(previous_entries):
        if entry['tipo'] != 'RECEITA_MEDICA' and 'sCdUsuario' in entry:
            logger.info(
                f"Receita associada a {entry.get('responsavel', 'N/A')} "
                f"(entry anterior)"
            )
            return entry['sCdUsuario']
    
    # Fallback: usar veterinário padrão
    logger.warning(
        f"Receita sem entry anterior válido. Usando fallback: {get_default_vet_fallback()}"
    )
    return default_vet_id


def migrate_prontuarios_bulk(batch_size: int = 500, dry_run: bool = False):
    """
    Migração de prontuários com parsing de texto complexo.
    
    Args:
        batch_size: Tamanho do lote (não usado, mantido por compatibilidade)
        dry_run: Se True, apenas simula
    
    Returns:
        dict: Estatísticas da migração
    """
    print("\n" + "="*80)
    print("MIGRAÇÃO DE PRONTUÁRIOS - PARSING DE TEXTO")
    print("="*80 + "\n")
    
    if dry_run:
        print("🔍 MODO DRY-RUN (simulação)")
        print("   Nenhum dado será inserido no banco de dados\n")
    
    # Conectar aos bancos
    origem_engine = get_engine_from_env("LEGACY_DB_URL")
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()
    default_vet_fallback = get_default_vet_fallback()
    
    print(f"🔑 Tenant ID: {tenant_id}")
    print(f"👨‍⚕️  Veterinário fallback: {default_vet_fallback}\n")
    
    # ==================================================================
    # FASE 1: PRE-CARREGAR MAPEAMENTOS
    # ==================================================================
    print("📊 Carregando dados de referência...")
    
    # Mapeamento de pets
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
    
    # Mapeamento de veterinários (usuários do tipo veterinário)
    print("  - Carregando veterinários...", end=" ", flush=True)
    veterinarios_map = {}
    default_vet_id = None
    
    with dest_engine.connect() as conn:
        # Buscar todos os usuários (assumindo que veterinários estão na tabela USUARIO)
        # Ajustar query conforme estrutura real
        result = conn.execute(text(f"""
            SELECT sCdUsuario, sNmUsuario
            FROM USUARIO
            WHERE sCdTenant = '{tenant_id}'
              AND bFlAtivo = 1
        """))
        
        for row in result:
            nome = row.sNmUsuario.strip() if row.sNmUsuario else ""
            if nome:
                veterinarios_map[nome] = row.sCdUsuario
                
                # Verificar se é o fallback
                if nome.upper() == default_vet_fallback.upper():
                    default_vet_id = row.sCdUsuario
    
    print(f"✓ {len(veterinarios_map):,} veterinários")
    
    if not default_vet_id:
        logger.error(f"Veterinário fallback '{default_vet_fallback}' não encontrado!")
        print(f"\n✗ ERRO: Veterinário fallback '{default_vet_fallback}' não encontrado")
        print("  Cadastre este usuário ou ajuste DEFAULT_VET_FALLBACK_NAME no .env\n")
        return None
    
    print(f"  - Veterinário fallback: {default_vet_fallback} ({default_vet_id})")
    
    # Prontuários já migrados
    print("  - Prontuários já migrados...", end=" ", flush=True)
    prontuarios_migrados = set()
    with dest_engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT sValorChaveOrigem
            FROM CONTROLE_MIGRACAO_LEGADO
            WHERE sCdTenant = '{tenant_id}'
              AND sTabelaOrigem = 'PET_ANIMAL_PRONTUARIO'
              AND sTabelaDestino = 'PRONTUARIO'
        """))
        
        for row in result:
            prontuarios_migrados.add(int(row.sValorChaveOrigem))
    
    print(f"✓ {len(prontuarios_migrados):,} prontuários")
    
    # ==================================================================
    # FASE 2: CARREGAR PRONTUÁRIOS DA ORIGEM
    # ==================================================================
    print("\n🔄 Carregando prontuários da origem...")
    
    with origem_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT Codigo, Animal, Tag
            FROM PET_ANIMAL_PRONTUARIO
            WHERE Tag IS NOT NULL
            ORDER BY Codigo
        """))
        
        all_rows = result.fetchall()
    
    total = len(all_rows)
    print(f"  Total de registros na origem: {total:,}\n")
    
    if dry_run:
        print(f"[DRY-RUN] Processando amostra de 5 registros...\n")
        all_rows = all_rows[:5]
    
    # ==================================================================
    # FASE 3: PROCESSAR E PARSEAR PRONTUÁRIOS
    # ==================================================================
    print("⚙️  Processando e parseando prontuários...")
    
    prontuarios_para_inserir = []
    receitas_para_inserir = []
    controle_para_inserir = []
    
    stats = {
        'total_registros': 0,
        'total_entries': 0,
        'prontuarios': 0,
        'receitas': 0,
        'laboratorios': 0,
        'sem_pet': 0,
        'ja_migrado': 0,
        'vet_nao_encontrado': 0,
        'parse_error': 0
    }
    
    for i, row in enumerate(all_rows, 1):
        stats['total_registros'] += 1
        
        # Progresso
        if i % 100 == 0:
            print(f"  Processando: {i:,}/{len(all_rows):,} registros...")
        
        codigo_origem = int(row.Codigo)
        animal_id = int(row.Animal)
        tag_text = row.Tag
        
        # Verificar se já foi migrado
        if codigo_origem in prontuarios_migrados:
            stats['ja_migrado'] += 1
            continue
        
        # Verificar se pet foi migrado
        if animal_id not in pets_map:
            stats['sem_pet'] += 1
            continue
        
        sCdPet = pets_map[animal_id]
        
        # Parse do texto
        try:
            entries = parse_prontuario_entries(tag_text)
        except Exception as e:
            logger.error(f"Erro ao parsear prontuário {codigo_origem}: {e}")
            stats['parse_error'] += 1
            continue
        
        if not entries:
            continue
        
        stats['total_entries'] += len(entries)
        processed_entries = []
        
        # Processar cada entry
        for entry in entries:
            entry_data = entry['data']
            entry_tipo = entry['tipo']
            entry_responsavel = entry['responsavel']
            entry_conteudo = entry['conteudo']
            
            if entry_tipo == 'RECEITA_MEDICA':
                # Associar ao veterinário do entry imediatamente anterior
                sCdUsuario = associate_receita_to_previous_vet(
                    processed_entries,
                    default_vet_id
                )
                
                receitas_para_inserir.append({
                    'sCdReceitaMedica': str(uuid.uuid4()),
                    'sCdTenant': tenant_id,
                    'sCdPet': sCdPet,
                    'tDtRegistro': entry_data,
                    'sCdUsuarioRegistro': sCdUsuario,
                    'tDtAlteracao': None,
                    'sCdUsuarioAlteracao': None,
                    'sDsObservacao': '',
                    'sDsReceitaMedica': entry_conteudo,
                    'bFlReceitaControlada': 0
                })
                
                stats['receitas'] += 1
                # Adicionar à lista de processados (sem sCdUsuario próprio)
                processed_entries.append(entry)
                
            elif entry_tipo == 'LABORATORIO':
                # Registrar como prontuário com observação do laboratório
                prontuarios_para_inserir.append({
                    'sCdProntuario': str(uuid.uuid4()),
                    'sCdTenant': tenant_id,
                    'sCdPet': sCdPet,
                    'tDtRegistro': entry_data,
                    'sCdUsuarioRegistro': default_vet_id,
                    'sDsObservacao': entry_responsavel,  # Nome do laboratório
                    'sDsProntuario': entry_conteudo,
                    'tDtAlteracao': None,
                    'sCdUsuarioAlteracao': None
                })
                
                stats['laboratorios'] += 1
                # Adicionar à lista de processados
                entry['sCdUsuario'] = default_vet_id
                processed_entries.append(entry)
                
            else:  # PRONTUARIO
                # Buscar veterinário
                sCdUsuario = find_veterinario_by_name(entry_responsavel, veterinarios_map)
                
                if not sCdUsuario:
                    sCdUsuario = default_vet_id
                    stats['vet_nao_encontrado'] += 1
                
                prontuarios_para_inserir.append({
                    'sCdProntuario': str(uuid.uuid4()),
                    'sCdTenant': tenant_id,
                    'sCdPet': sCdPet,
                    'tDtRegistro': entry_data,
                    'sCdUsuarioRegistro': sCdUsuario,
                    'sDsObservacao': '',  # Vazio para prontuários normais
                    'sDsProntuario': entry_conteudo,
                    'tDtAlteracao': None,
                    'sCdUsuarioAlteracao': None
                })
                
                stats['prontuarios'] += 1
                # Adicionar à lista de processados com o veterinário encontrado
                entry['sCdUsuario'] = sCdUsuario
                processed_entries.append(entry)
        
        # Registro de controle (um por registro de origem)
        controle_para_inserir.append({
            'sCdTenant': tenant_id,
            'sTabelaOrigem': 'PET_ANIMAL_PRONTUARIO',
            'sCampoChaveOrigem': 'Codigo',
            'sValorChaveOrigem': str(codigo_origem),
            'sTabelaDestino': 'PRONTUARIO',
            'sCampoChaveDestino': 'sCdProntuario',
            'sValorChaveDestino': 'MULTIPLE',  # Indica múltiplos registros
            'dtMigracao': datetime.now()
        })
    
    print(f"  ✓ Processamento concluído!")
    print(f"    - Prontuários: {stats['prontuarios']:,}")
    print(f"    - Receitas médicas: {stats['receitas']:,}")
    print(f"    - Laboratórios: {stats['laboratorios']:,}")
    print(f"    - Sem pet: {stats['sem_pet']:,}")
    print(f"    - Vet não encontrado: {stats['vet_nao_encontrado']:,}\n")
    
    if dry_run:
        print("[DRY-RUN] Simulação concluída. Nenhum dado foi inserido.\n")
        return stats
    
    # ==================================================================
    # FASE 4: INSERIR NO BANCO
    # ==================================================================
    print("💾 Salvando no banco de dados...")
    
    with dest_engine.begin() as conn:
        # Inserir prontuários
        if prontuarios_para_inserir:
            print(f"  - Inserindo {len(prontuarios_para_inserir):,} prontuários...", end=" ", flush=True)
            
            insert_pront_sql = text("""
                INSERT INTO PRONTUARIO (
                    sCdProntuario, sCdTenant, sCdPet, tDtRegistro,
                    sCdUsuarioRegistro, sDsObservacao, sDsProntuario,
                    tDtAlteracao, sCdUsuarioAlteracao
                )
                VALUES (
                    :sCdProntuario, :sCdTenant, :sCdPet, :tDtRegistro,
                    :sCdUsuarioRegistro, :sDsObservacao, :sDsProntuario,
                    :tDtAlteracao, :sCdUsuarioAlteracao
                )
            """)
            
            conn.execute(insert_pront_sql, prontuarios_para_inserir)
            print("✓")
        
        # Inserir receitas
        if receitas_para_inserir:
            print(f"  - Inserindo {len(receitas_para_inserir):,} receitas médicas...", end=" ", flush=True)
            
            insert_rec_sql = text("""
                INSERT INTO RECEITA_MEDICA (
                    sCdReceitaMedica, sCdTenant, sCdPet, tDtRegistro,
                    sCdUsuarioRegistro, tDtAlteracao, sCdUsuarioAlteracao,
                    sDsObservacao, sDsReceitaMedica, bFlReceitaControlada
                )
                VALUES (
                    :sCdReceitaMedica, :sCdTenant, :sCdPet, :tDtRegistro,
                    :sCdUsuarioRegistro, :tDtAlteracao, :sCdUsuarioAlteracao,
                    :sDsObservacao, :sDsReceitaMedica, :bFlReceitaControlada
                )
            """)
            
            conn.execute(insert_rec_sql, receitas_para_inserir)
            print("✓")
        
        # Registrar controle
        if controle_para_inserir:
            print(f"  - Registrando {len(controle_para_inserir):,} mapeamentos...", end=" ", flush=True)
            
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
            
            conn.execute(insert_controle_sql, controle_para_inserir)
            print("✓")
    
    # ==================================================================
    # ESTATÍSTICAS FINAIS
    # ==================================================================
    print("\n" + "="*80)
    print("✓ Migração finalizada!")
    print("="*80)
    print(f"  Total de registros processados: {stats['total_registros']:,}")
    print(f"  Total de entries parseados: {stats['total_entries']:,}")
    print(f"  Prontuários inseridos: {stats['prontuarios']:,}")
    print(f"  Receitas médicas inseridas: {stats['receitas']:,}")
    print(f"  Registros de laboratório: {stats['laboratorios']:,}")
    print(f"  Sem pet migrado: {stats['sem_pet']:,}")
    print(f"  Veterinário não encontrado (usou fallback): {stats['vet_nao_encontrado']:,}")
    print(f"  Erros de parsing: {stats['parse_error']:,}")
    print("="*80 + "\n")
    
    return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migração de Prontuários com Parse de Texto")
    parser.add_argument("--batch-size", type=int, default=500, help="Não utilizado (compatibilidade)")
    parser.add_argument("--dry-run", action="store_true", help="Simula migração sem inserir dados")
    
    args = parser.parse_args()
    
    # Criar diretório de logs se não existir
    Path("logs").mkdir(exist_ok=True)
    
    migrate_prontuarios_bulk(batch_size=args.batch_size, dry_run=args.dry_run)
