"""
Testes para validar o parsing de prontuários.

Testa a extração de entries do campo Tag com padrão [DD/MM/YYYY HH:MM:SS - RESPONSÁVEL]:
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from migrations.prontuarios.migrate_prontuarios import parse_prontuario_entries


def test_sample_parsing():
    """Testa parsing com amostra real do banco."""
    
    sample_text = """[08/11/2025 10:54:05 - DRA MIRELLA]:
paciente sem histórico de vacinação. Iniciou protocolo completo de V10 e raiva hoje. paciente sem histórico de vacinação. Iniciou protocolo completo de V10 e raiva hoje.

[08/11/2025 10:53:32 - DRA MIRELLA]:
veio para fazer somente para atestado obito 

[08/11/2025 10:47:25 - RECEITA MÉDICA]:
USO ORAL
1. Queranon pasta felinos 100g
   Aplicar 2x ao dia, durante 60 dias
   (pasta oral) 
2. Azulon suspensão 30ml
   1ml, 2x ao dia, durante 60 dias 
   (Suplemento oral)

[05/11/2025 14:25:30 - DRA JULIANA]:
Animal veio para consulta de rotina. Estado geral bom.

[04/11/2025 09:15:00 - CITOVET LABORATORIO]:
Resultado de hemograma completo disponível."""

    print("="*80)
    print("TESTE DE PARSING DE PRONTUÁRIOS")
    print("="*80 + "\n")
    
    entries = parse_prontuario_entries(sample_text)
    
    print(f"Total de entries encontrados: {len(entries)}\n")
    
    for i, entry in enumerate(entries, 1):
        print(f"Entry {i}:")
        print(f"  Data: {entry['data']}")
        print(f"  Tipo: {entry['tipo']}")
        print(f"  Responsável: {entry['responsavel']}")
        print(f"  Conteúdo: {entry['conteudo'][:100]}...")
        print()
    
    # Validações
    assert len(entries) == 5, f"Esperado 5 entries, encontrado {len(entries)}"
    
    # Verificar tipo da receita
    receitas = [e for e in entries if e['tipo'] == 'RECEITA_MEDICA']
    assert len(receitas) == 1, f"Esperado 1 receita, encontrado {len(receitas)}"
    
    # Verificar tipo de laboratório
    labs = [e for e in entries if e['tipo'] == 'LABORATORIO']
    assert len(labs) == 1, f"Esperado 1 lab, encontrado {len(labs)}"
    
    # Verificar prontuários normais
    pronts = [e for e in entries if e['tipo'] == 'PRONTUARIO']
    assert len(pronts) == 3, f"Esperado 3 prontuários, encontrado {len(pronts)}"
    
    print("="*80)
    print("✓ Todos os testes passaram!")
    print("="*80)


def test_edge_cases():
    """Testa casos extremos."""
    
    print("\n" + "="*80)
    print("TESTE DE CASOS EXTREMOS")
    print("="*80 + "\n")
    
    # Texto vazio
    assert parse_prontuario_entries("") == []
    assert parse_prontuario_entries(None) == []
    print("✓ Texto vazio/None")
    
    # Sem padrão válido
    assert parse_prontuario_entries("Texto sem padrão") == []
    print("✓ Sem padrão válido")
    
    # Padrão incompleto
    incomplete = "[08/11/2025 10:54:05 - DRA MIRELLA]:"
    entries = parse_prontuario_entries(incomplete)
    assert len(entries) == 0  # Sem conteúdo
    print("✓ Padrão sem conteúdo")
    
    # Múltiplas quebras de linha no conteúdo
    multiline = """[08/11/2025 10:54:05 - DRA MIRELLA]:
Linha 1
Linha 2
Linha 3

[08/11/2025 10:55:00 - DRA MIRELLA]:
Outro entry"""
    
    entries = parse_prontuario_entries(multiline)
    assert len(entries) == 2
    assert "Linha 1" in entries[0]['conteudo']
    assert "Linha 2" in entries[0]['conteudo']
    print("✓ Múltiplas linhas no conteúdo")
    
    print("\n✓ Todos os casos extremos passaram!")


if __name__ == "__main__":
    test_sample_parsing()
    test_edge_cases()
    
    print("\n" + "="*80)
    print("RESUMO: TODOS OS TESTES CONCLUÍDOS COM SUCESSO!")
    print("="*80 + "\n")
