"""
Script de teste para validar o fuzzy matching de cidades.
Útil para testar o algoritmo sem precisar conectar ao banco.
"""

try:
    from rapidfuzz import fuzz, process
    print("✓ Usando rapidfuzz (otimizado)")
except ImportError:
    try:
        from fuzzywuzzy import fuzz, process
        print("✓ Usando fuzzywuzzy")
    except ImportError:
        print("✗ Instale rapidfuzz: pipenv install rapidfuzz")
        exit(1)


# Exemplo de cidades do banco
cidades_exemplo = [
    ("e644a337-65ef-5745-bdb3-000faeef6736", "Florianópolis", "SC"),
    ("e1ff3373-6bd8-5b4a-b6c1-002753b1e6e7", "São José", "SC"),
    ("a1234567-89ab-cdef-0123-456789abcdef", "Sao Jose", "SC"),
    ("b2345678-9abc-def0-1234-56789abcdef0", "Joinville", "SC"),
    ("c3456789-abcd-ef01-2345-6789abcdef01", "Blumenau", "SC"),
    ("d4567890-bcde-f012-3456-789abcdef012", "São José dos Campos", "SP"),
    ("e5678901-cdef-0123-4567-89abcdef0123", "Cajazeiras do Piauí", "PI"),
    ("f6789012-def0-1234-5678-9abcdef01234", "Lamim", "MG"),
]


def testar_match(nome_busca, uf_busca, min_score=85):
    """Simula o algoritmo de busca do update_cities.py"""
    print(f"\n{'='*60}")
    print(f"Buscando: '{nome_busca}' / {uf_busca}")
    print(f"{'='*60}")
    
    # Filtrar por UF
    cidades_uf = [c for c in cidades_exemplo if c[2] == uf_busca]
    
    if not cidades_uf:
        print(f"⚠ Nenhuma cidade encontrada em {uf_busca}, buscando em todo Brasil...")
        cidades_uf = cidades_exemplo
    
    # Preparar para fuzzy matching
    choices = {cidade[1]: (cidade[0], cidade[2]) for cidade in cidades_uf}
    
    # Fazer fuzzy matching
    result = process.extractOne(
        nome_busca,
        choices.keys(),
        scorer=fuzz.ratio,
        score_cutoff=min_score
    )
    
    if result:
        cidade_match, score, _ = result if len(result) == 3 else (result[0], result[1], None)
        scd_cidade, uf_match = choices[cidade_match]
        
        print(f"✓ Match encontrado: '{cidade_match}' ({uf_match})")
        print(f"  Score: {score}%")
        print(f"  ID: {scd_cidade}")
        
        # Verificar se devemos preferir SC
        if score < 95 and uf_match != uf_busca and uf_busca != "SC":
            cidades_sc = [c for c in cidades_exemplo if c[2] == "SC"]
            if cidades_sc:
                choices_sc = {cidade[1]: (cidade[0], cidade[2]) for cidade in cidades_sc}
                result_sc = process.extractOne(
                    nome_busca,
                    choices_sc.keys(),
                    scorer=fuzz.ratio,
                    score_cutoff=min_score - 10
                )
                if result_sc:
                    cidade_sc, score_sc, _ = result_sc if len(result_sc) == 3 else (result_sc[0], result_sc[1], None)
                    if score_sc >= score - 15:
                        scd_cidade_sc, _ = choices_sc[cidade_sc]
                        print(f"\n⭐ PREFERÊNCIA SC:")
                        print(f"  '{cidade_sc}' (SC)")
                        print(f"  Score: {score_sc}%")
                        print(f"  ID: {scd_cidade_sc}")
                        return scd_cidade_sc
        
        return scd_cidade
    else:
        print(f"✗ Nenhum match encontrado (score mínimo: {min_score}%)")
        return None


if __name__ == "__main__":
    # Testes
    print("\n" + "="*60)
    print("TESTE DE FUZZY MATCHING DE CIDADES")
    print("="*60)
    
    # Teste 1: Match exato
    testar_match("Florianópolis", "SC")
    
    # Teste 2: Match sem acento
    testar_match("Florianopolis", "SC")
    
    # Teste 3: Match aproximado
    testar_match("Sao Jose", "SC")
    
    # Teste 4: Nome comum em vários estados - deve preferir SC
    print("\n[TESTE ESPECIAL: Preferência SC quando UF diferente]")
    testar_match("São José", "RJ")  # Busca em RJ, mas deve preferir SC
    
    # Teste 5: Cidade de outro estado
    testar_match("Lamim", "MG")
    
    # Teste 6: Typo leve
    testar_match("Blumenal", "SC")
    
    # Teste 7: Match impossível
    testar_match("Cidade Inexistente", "XX")
    
    print("\n" + "="*60)
    print("FIM DOS TESTES")
    print("="*60 + "\n")
