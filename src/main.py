#!/usr/bin/env python3
"""
Sistema de Migração PetSys - Legado para Web

Menu interativo para executar migrações de diferentes entidades.
"""
import sys
from pathlib import Path

# Adicionar src ao path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent))

from migrations.clientes.migrate_clientes import migrate_clientes
from migrations.pets.migrate_pets import migrate_pets
from migrations.vacinas.migrate_vacinas import migrate_vacinas
from migrations.aplicacoes_vacinas.migrate_aplicacoes_vacinas_bulk import migrate_aplicacoes_vacinas_bulk
from migrations.pesos.migrate_pesos_bulk import migrate_pesos_bulk
from clear_migrated_data import clear_all_data

# Importar função de atualização de cidades
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from update_cities import update_cities


def print_header():
    """Imprime o cabeçalho do sistema."""
    print("\n" + "="*60)
    print("  SISTEMA DE MIGRAÇÃO PETSYS")
    print("  Legado -> Web")
    print("="*60 + "\n")


def print_menu():
    """Imprime o menu de opções."""
    print("Escolha a migração que deseja executar:")
    print()
    print("  1. Clientes (PET_CLIENTE -> PESSOA)")
    print("  2. Pets (PET_ANIMAL -> PET)")
    print("  3. Vacinas (PET_VACINA -> VACINA)")
    print("  4. Aplicações de Vacinas (PET_ANIMAL_VACINA -> PET_VACINA)")
    print("  5. Pesos dos Pets (PET_ANIMAL_PESO -> PET_PESO)")
    print("  6. Atualizar Cidades via ViaCEP")
    print()
    print("  9. ⚠️  EXCLUIR TODOS os dados migrados")
    print()
    print("  0. Sair")
    print()


def confirm_action(message="Deseja continuar?"):
    """Solicita confirmação do usuário."""
    while True:
        response = input(f"{message} (s/n): ").strip().lower()
        if response in ['s', 'sim', 'y', 'yes']:
            return True
        elif response in ['n', 'nao', 'não', 'no']:
            return False
        print("Por favor, responda 's' para sim ou 'n' para não.")


def get_batch_size():
    """Solicita tamanho do batch ao usuário."""
    while True:
        try:
            size = input("Tamanho do batch (padrão 500): ").strip()
            if not size:
                return 500
            size = int(size)
            if size > 0:
                return size
            print("Por favor, informe um número maior que zero.")
        except ValueError:
            print("Por favor, informe um número válido.")


def run_migration_clientes():
    """Executa a migração de clientes."""
    print("\n" + "-"*60)
    print("MIGRAÇÃO DE CLIENTES")
    print("-"*60 + "\n")
    
    if not confirm_action("Deseja continuar?"):
        print("\nMigração cancelada.\n")
        return
    
    # Executar migração
    total = migrate_clientes(batch_size=500)
    
    print(f"\n✓ Migração concluída! {total} registros processados.\n")


def run_migration_pets():
    """Executa a migração de pets."""
    print("\n" + "-"*60)
    print("MIGRAÇÃO DE PETS")
    print("-"*60 + "\n")
    
    print("⚠ IMPORTANTE: Execute a migração de CLIENTES antes!")
    print("  Pets sem proprietário migrado serão pulados.\n")
    
    if not confirm_action("Deseja continuar?"):
        print("\nMigração cancelada.\n")
        return
    
    # Executar migração
    total = migrate_pets(batch_size=500)
    
    print(f"\n✓ Migração concluída! {total} registros processados.\n")


def run_migration_vacinas():
    """Executa a migração de vacinas."""
    print("\n" + "-"*60)
    print("MIGRAÇÃO DE VACINAS")
    print("-"*60 + "\n")
    
    print("Esta migração irá:")
    print("  • Ler registros de PET_VACINA (banco legado)")
    print("  • Mapear para tabela VACINA (banco destino)")
    print("  • Definir espécie padrão como CANINA (1)")
    print("  • Configurar valores padrão para desconto e plano\n")
    
    # Perguntar sobre dry-run
    if confirm_action("Executar em modo DRY-RUN primeiro?"):
        print("\n→ Executando DRY-RUN...\n")
        migrate_vacinas(batch_size=500, dry_run=True)
        
        if not confirm_action("\nDeseja executar a migração real agora?"):
            print("\nMigração cancelada.\n")
            return
    
    # Solicitar batch size
    batch_size = get_batch_size()
    
    # Executar migração real
    print("\n→ Executando migração real...\n")
    stats = migrate_vacinas(batch_size=batch_size, dry_run=False)
    
    print(f"\n✓ Migração concluída!")
    print(f"  Total: {stats['total']}")
    print(f"  Inseridos: {stats['inseridos']}")
    print(f"  Atualizados: {stats['atualizados']}\n")


def run_migration_aplicacoes_vacinas():
    """Executa a migração de aplicações de vacinas."""
    print("\n" + "-"*60)
    print("MIGRAÇÃO DE APLICAÇÕES DE VACINAS (CARTEIRA)")
    print("-"*60 + "\n")
    
    print("Esta migração irá:")
    print("  • Ler registros de PET_ANIMAL_VACINA (banco legado)")
    print("  • Buscar pet e vacina via tabela de controle")
    print("  • Migrar histórico de vacinas aplicadas e previstas")
    print("  • Inserir em PET_VACINA (banco destino)\n")
    
    print("⚠ IMPORTANTE:")
    print("  Execute migrações de CLIENTES, PETS e VACINAS antes!")
    print("  Aplicações sem pet ou vacina migrados serão puladas.\n")
    
    # Perguntar sobre dry-run
    if confirm_action("Executar em modo DRY-RUN primeiro?"):
        print("\n→ Executando DRY-RUN...\n")
        migrate_aplicacoes_vacinas_bulk(batch_size=1000, dry_run=True)
        
        if not confirm_action("\nDeseja executar a migração real agora?"):
            print("\nMigração cancelada.\n")
            return
    
    # Solicitar batch size
    batch_size = get_batch_size()
    if batch_size < 1000:
        print("  ℹ Recomendado batch_size >= 1000 para melhor performance")
    
    # Executar migração real
    print("\n→ Executando migração BULK...\n")
    total = migrate_aplicacoes_vacinas_bulk(batch_size=batch_size, dry_run=False)
    
    print(f"\n✓ Migração concluída! {total} registros processados.\n")


def run_update_cities():
    """Executa atualização de cidades via ViaCEP."""
    print("\n" + "-"*60)
    print("ATUALIZAÇÃO DE CIDADES VIA VIACEP")
    print("-"*60 + "\n")
    
    print("Esta atualização irá:")
    print("  • Buscar pessoas com CEP cadastrado")
    print("  • Consultar dados na API ViaCEP")
    print("  • Atualizar cidade/UF baseado no CEP")
    print("  • Usar fuzzy matching para encontrar cidade correta\n")
    
    print("⚠ IMPORTANTE:")
    print("  • Respeita delay entre requisições (configurável)")
    print("  • Processa em lotes para evitar sobrecarga")
    print("  • Não altera dados se cidade já estiver correta\n")
    
    # Perguntar sobre dry-run
    if confirm_action("Executar em modo DRY-RUN primeiro?"):
        print("\n→ Executando DRY-RUN...\n")
        
        # Criar objeto args para simular argumentos
        class Args:
            def __init__(self):
                self.tenant = None
                self.dry_run = True
        
        update_cities(Args())
        
        if not confirm_action("\nDeseja executar a atualização real agora?"):
            print("\nAtualização cancelada.\n")
            return
    
    # Executar atualização real
    print("\n→ Executando atualização REAL...\n")
    
    class Args:
        def __init__(self):
            self.tenant = None
            self.dry_run = False
    
    update_cities(Args())
    
    print("\n✓ Atualização concluída!\n")


def run_migration_pesos():
    """Executa a migração de pesos dos pets."""
    print("\n" + "-"*60)
    print("MIGRAÇÃO DE PESOS DOS PETS")
    print("-"*60 + "\n")
    
    print("Esta migração irá:")
    print("  • Ler registros de PET_ANIMAL_PESO (banco legado)")
    print("  • Buscar pet via tabela de controle")
    print("  • Migrar histórico de pesagens")
    print("  • Inserir em PET_PESO (banco destino)")
    print("  • Corrigir automaticamente pesos acima de 999kg\n")
    
    print("⚠ IMPORTANTE:")
    print("  Execute migrações de CLIENTES e PETS antes!")
    print("  Pesos sem pet migrado serão pulados.\n")
    
    # Perguntar sobre dry-run
    if confirm_action("Executar em modo DRY-RUN primeiro?"):
        print("\n→ Executando DRY-RUN...\n")
        migrate_pesos_bulk(batch_size=1000, dry_run=True)
        
        if not confirm_action("\nDeseja executar a migração real agora?"):
            print("\nMigração cancelada.\n")
            return
    
    # Solicitar batch size
    batch_size = get_batch_size()
    if batch_size < 1000:
        print("  ℹ Recomendado batch_size >= 1000 para melhor performance")
    
    # Executar migração real
    print("\n→ Executando migração BULK...\n")
    total = migrate_pesos_bulk(batch_size=batch_size, dry_run=False)
    
    print(f"\n✓ Migração concluída! {total} registros processados.\n")


def run_clear_all_data():
    """Executa exclusão de todos os dados migrados."""
    print("\n" + "-"*60)
    print("EXCLUSÃO DE DADOS MIGRADOS")
    print("-"*60 + "\n")
    
    print("⚠️  ATENÇÃO: Esta operação é IRREVERSÍVEL!")
    print("\nSerão excluídos (nesta ordem):")
    print("  1. Aplicações de Vacinas")
    print("  2. Pesos")
    print("  3. Vacinas")
    print("  4. Pets")
    print("  5. Clientes")
    print("  6. Registros de Controle\n")
    
    # Perguntar sobre dry-run
    if confirm_action("Executar em modo DRY-RUN primeiro (simulação)?"):
        print("\n→ Executando SIMULAÇÃO...\n")
        clear_all_data(dry_run=True)
        
        if not confirm_action("\n⚠️  Deseja REALMENTE EXCLUIR todos os dados?"):
            print("\nOperação cancelada.\n")
            return
    else:
        if not confirm_action("\n⚠️  Deseja REALMENTE EXCLUIR todos os dados SEM simular?"):
            print("\nOperação cancelada.\n")
            return
    
    # Executar exclusão real
    print("\n→ Executando EXCLUSÃO REAL...\n")
    stats = clear_all_data(dry_run=False)
    
    if stats:
        print(f"\n✓ Exclusão concluída!")
        print(f"  Total excluído: {sum(stats.values())} registros\n")
    else:
        print(f"\n✗ Erro durante exclusão.\n")


def main():
    """Função principal do menu."""
    print_header()
    
    while True:
        print_menu()
        
        try:
            choice = input("Opção: ").strip()
            
            if choice == "0":
                print("\nEncerrando sistema de migração. Até logo!\n")
                break
            
            elif choice == "1":
                run_migration_clientes()
            
            elif choice == "2":
                run_migration_pets()
            
            elif choice == "3":
                run_migration_vacinas()
            
            elif choice == "4":
                run_migration_aplicacoes_vacinas()
            
            elif choice == "5":
                run_migration_pesos()
            
            elif choice == "6":
                run_update_cities()
            
            elif choice == "9":
                run_clear_all_data()
            
            else:
                print("\n✗ Opção inválida. Por favor, escolha uma opção válida.\n")
        
        except KeyboardInterrupt:
            print("\n\nOperação cancelada pelo usuário. Até logo!\n")
            break
        except Exception as e:
            print(f"\n✗ Erro: {e}\n")
            if confirm_action("Deseja continuar?"):
                continue
            else:
                break


if __name__ == "__main__":
    main()
