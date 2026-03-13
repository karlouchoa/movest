from datetime import datetime

from main import obter_data_corte_base, solicitar_parametros_conexao, validar_conexao
from src.auditoria import auditar_saldos_pos_update, salvar_relatorio_auditoria_saldoit
from src.database import get_engine
from src.utils import obter_ultima_copia_seguranca_t_saldoit


def solicitar_codigo_empresa():
    codigo_empresa_raw = input("Informe o codigo da empresa [todas]: ").strip()
    if not codigo_empresa_raw:
        return None
    if not codigo_empresa_raw.isdigit() or int(codigo_empresa_raw) <= 0:
        raise ValueError("O codigo da empresa deve ser um numero inteiro maior que zero.")
    return int(codigo_empresa_raw)


def solicitar_tabela_backup(engine_atual):
    with engine_atual.connect() as conn:
        tabela_sugerida = obter_ultima_copia_seguranca_t_saldoit(conn)

    if tabela_sugerida:
        mensagem = f"Informe a tabela de backup da T_SALDOIT [{tabela_sugerida}]: "
    else:
        mensagem = "Informe a tabela de backup da T_SALDOIT: "

    tabela_informada = input(mensagem).strip()
    tabela_backup = tabela_informada or tabela_sugerida
    if not tabela_backup:
        raise ValueError("Nenhuma tabela de backup foi informada e nao ha copia recente no banco atual.")
    return tabela_backup


def main():
    (
        servidor,
        banco_base,
        banco_atual,
        username,
        password,
        codigo_item,
        _importa_ajuste_inventario,
    ) = solicitar_parametros_conexao()
    codigo_empresa = solicitar_codigo_empresa()
    engine_base = get_engine(servidor, banco_base, username, password)
    engine_atual = get_engine(servidor, banco_atual, username, password)

    validar_conexao(engine_base, banco_base, "base", servidor)
    validar_conexao(engine_atual, banco_atual, "atual", servidor)

    tabela_backup = solicitar_tabela_backup(engine_atual)
    data_corte, data_maxima_base, coluna_data_base = obter_data_corte_base(engine_base)
    if data_maxima_base and data_maxima_base > datetime.now():
        print(
            f"Data maxima futura detectada em T_MOVEST.{coluna_data_base} ({data_maxima_base}). "
            f"Usando a maior data valida ate hoje: {data_corte}"
        )
    elif data_maxima_base:
        print(f"Data maxima em T_MOVEST.{coluna_data_base}: {data_maxima_base}")
    print(f"Data de corte usada na auditoria: {data_corte}")
    print(f"Tabela de backup usada: {tabela_backup}")

    if codigo_item is not None:
        print(f"Auditoria filtrada para o item {codigo_item}.")
    else:
        print("Auditoria para todos os itens movimentados.")
    if codigo_empresa is not None:
        print(f"Auditoria filtrada para a empresa {codigo_empresa}.")
    else:
        print("Auditoria por empresa para todos os itens e empresas movimentados.")

    print("Lendo saldos e movimentacoes para a auditoria final...")
    df_discrepancias, resumo = auditar_saldos_pos_update(
        engine_atual,
        tabela_backup,
        data_corte,
        codigo_item=codigo_item,
        codigo_empresa=codigo_empresa,
    )
    caminho_relatorio = salvar_relatorio_auditoria_saldoit(
        df_discrepancias,
        tabela_backup,
        codigo_item=codigo_item,
        codigo_empresa=codigo_empresa,
    )

    print(f"Movimentos auditados: {resumo['qtd_movimentos_auditados']}")
    print(f"Itens auditados: {resumo['qtd_itens_auditados']}")
    print(f"Pares item/empresa auditados: {resumo['qtd_pares_item_empresa_auditados']}")
    print(f"Discrepancias encontradas: {resumo['qtd_discrepancias']}")
    print(f"Relatorio salvo em: {caminho_relatorio}")

    if not df_discrepancias.empty:
        print("Primeiras discrepancias encontradas:")
        print(df_discrepancias.head(20).to_string(index=False))
    else:
        print("Nenhuma discrepancia encontrada.")


if __name__ == "__main__":
    main()
