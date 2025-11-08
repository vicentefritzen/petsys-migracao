"""
Utilitários para fuzzy matching de domínios (raças, cores, etc)
"""
try:
    from rapidfuzz import fuzz, process
    FUZZY_LIB = "rapidfuzz"
except ImportError:
    try:
        from fuzzywuzzy import fuzz, process
        FUZZY_LIB = "fuzzywuzzy"
    except ImportError:
        print("⚠ AVISO: Instale rapidfuzz ou fuzzywuzzy para matching fuzzy")
        print("Execute: pipenv install rapidfuzz")
        FUZZY_LIB = None


def fuzzy_match(query: str, choices: dict, min_score: int = 80):
    """
    Faz fuzzy matching de uma string contra um dicionário de opções.
    
    Args:
        query: String a ser pesquisada
        choices: Dict {descricao: codigo} com as opções disponíveis
        min_score: Score mínimo para considerar match (0-100)
    
    Returns:
        tuple: (codigo_matched, descricao_matched, score) ou (None, None, 0) se não encontrar
    """
    if not query or not choices:
        return (None, None, 0)
    
    # Se não temos biblioteca de fuzzy matching, tentar match exato
    if FUZZY_LIB is None:
        query_upper = query.upper().strip()
        for descricao, codigo in choices.items():
            if descricao.upper().strip() == query_upper:
                return (codigo, descricao, 100)
        return (None, None, 0)
    
    # Fazer fuzzy matching
    result = process.extractOne(
        query, 
        choices.keys(), 
        scorer=fuzz.ratio,
        score_cutoff=min_score
    )
    
    if result:
        descricao_match, score, _ = result if len(result) == 3 else (result[0], result[1], None)
        codigo = choices[descricao_match]
        return (codigo, descricao_match, score)
    
    return (None, None, 0)


def buscar_raca_por_nome(dest_engine, nome_raca: str, especie: int, min_score: int = 80):
    """
    Busca raça pelo nome usando fuzzy matching.
    
    Args:
        dest_engine: Engine do banco destino
        nome_raca: Nome da raça a buscar
        especie: Código da espécie (1=CANINA, 2=FELINA)
        min_score: Score mínimo para match (padrão 80)
    
    Returns:
        tuple: (nCdRaca, sNmRaca_matched, score) ou (None, None, 0)
    """
    from sqlalchemy import text
    
    if not nome_raca:
        return (None, None, 0)
    
    # Buscar todas as raças da espécie
    select_sql = text("""
SELECT nCdRaca, sNmRaca
FROM RACA
WHERE nCdEspecie = :especie AND bFlAtivo = 1
""")
    
    racas = {}
    with dest_engine.connect() as conn:
        result = conn.execute(select_sql, {"especie": especie})
        rows = result.fetchall()
        for row in rows:
            racas[row[1]] = row[0]  # {sNmRaca: nCdRaca}
    
    if not racas:
        return (None, None, 0)
    
    # Fazer fuzzy matching
    codigo, descricao, score = fuzzy_match(nome_raca, racas, min_score)
    
    return (codigo, descricao, score)


def buscar_cor_por_nome(dest_engine, nome_cor: str, min_score: int = 80):
    """
    Busca cor pelo nome usando fuzzy matching.
    
    Args:
        dest_engine: Engine do banco destino
        nome_cor: Nome da cor a buscar
        min_score: Score mínimo para match (padrão 80)
    
    Returns:
        tuple: (nCdCor, sNmCor_matched, score) ou (None, None, 0)
    """
    from sqlalchemy import text
    
    if not nome_cor:
        return (None, None, 0)
    
    # Buscar todas as cores
    select_sql = text("""
SELECT nCdCor, sNmCor
FROM COR
WHERE bFlAtivo = 1
""")
    
    cores = {}
    with dest_engine.connect() as conn:
        result = conn.execute(select_sql)
        rows = result.fetchall()
        for row in rows:
            cores[row[1]] = row[0]  # {sNmCor: nCdCor}
    
    if not cores:
        return (None, None, 0)
    
    # Fazer fuzzy matching
    codigo, descricao, score = fuzzy_match(nome_cor, cores, min_score)
    
    return (codigo, descricao, score)


def mapear_sexo(codigo_sexo_legado: int):
    """
    Mapeia código de sexo do legado para o destino.
    
    Legado:
    1 = FEMEA
    2 = FEMEA CASTRADA
    3 = MACHO
    4 = MACHO CASTRADO
    
    Destino:
    1 = FÊMEA
    2 = MACHO
    3 = FÊMEA CASTRADA
    4 = MACHO CASTRADO
    
    Args:
        codigo_sexo_legado: Código do sexo no banco legado
    
    Returns:
        int: Código do sexo no banco destino
    """
    mapeamento = {
        1: 1,  # FEMEA -> FÊMEA
        2: 3,  # FEMEA CASTRADA -> FÊMEA CASTRADA
        3: 2,  # MACHO -> MACHO
        4: 4,  # MACHO CASTRADO -> MACHO CASTRADO
    }
    
    return mapeamento.get(int(codigo_sexo_legado), 1)  # Default FÊMEA se não encontrar


def mapear_porte(codigo_porte_legado: int):
    """
    Mapeia código de porte do legado para o destino.
    
    Legado e Destino aparentam ser iguais:
    1 = PEQUENO
    2 = MÉDIO  
    3 = GRANDE
    4 = GIGANTE
    
    Args:
        codigo_porte_legado: Código do porte no banco legado
    
    Returns:
        int: Código do porte no banco destino
    """
    # Mapeamento direto (parecem ser iguais)
    porte = int(codigo_porte_legado)
    if porte in [1, 2, 3, 4]:
        return porte
    return 1  # Default PEQUENO se inválido


def mapear_especie_por_raca(codigo_raca_legado: int, legacy_engine):
    """
    Descobre a espécie (CANINA ou FELINA) baseado na raça do legado.
    
    Args:
        codigo_raca_legado: Código da raça no banco legado
        legacy_engine: Engine do banco legado
    
    Returns:
        int: 1 para CANINA, 2 para FELINA
    """
    from sqlalchemy import text
    
    select_sql = text("""
SELECT Especie FROM PET_RACA WHERE Codigo = :codigo
""")
    
    with legacy_engine.connect() as conn:
        result = conn.execute(select_sql, {"codigo": codigo_raca_legado})
        row = result.fetchone()
        if row:
            return int(row[0])
    
    return 1  # Default CANINA se não encontrar
