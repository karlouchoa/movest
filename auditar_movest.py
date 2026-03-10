from main import obter_data_corte_base, solicitar_parametros_conexao, validar_conexao
from src.auditoria import auditar_movest, salvar_relatorio_auditoria
from src.database import get_engine


def solicitar_codigo_empresa():
    codigo_empresa_raw = input("Informe o codigo da empresa [todas]: ").strip()
    if not codigo_empresa_raw:
        return None
    if not codigo_empresa_raw.isdigit() or int(codigo_empresa_raw) <= 0:
        raise ValueError("O codigo da empresa deve ser um numero inteiro maior que zero.")
    return int(codigo_empresa_raw)


def main():
    servidor, banco_base, banco_atual, codigo_item = solicitar_parametros_conexao()
    codigo_empresa = solicitar_codigo_empresa()
    engine_base = get_engine(servidor, banco_base)
    engine_atual = get_engine(servidor, banco_atual)

    validar_conexao(engine_base, banco_base, "base", servidor)
    validar_conexao(engine_atual, banco_atual, "atual", servidor)

    data_corte, data_maxima_base = obter_data_corte_base(engine_base)
    if data_maxima_base:
        print(f"Data maxima da T_MOVEST base: {data_maxima_base}")
    print(f"Data de corte usada na auditoria: {data_corte}")

    if codigo_item is not None:
        print(f"Auditoria filtrada para o item {codigo_item}.")
    else:
        print("Auditoria para todos os itens.")
    if codigo_empresa is not None:
        print(
            f"Auditoria filtrada para a empresa {codigo_empresa}, usando o campo SldAntEmp."
        )
    else:
        print("Auditoria sem filtro de empresa, usando o campo saldoant.")

    print("Lendo movimentacoes e saldos para auditoria...")
    df_discrepancias, resumo = auditar_movest(
        engine_base,
        engine_atual,
        data_corte,
        codigo_item=codigo_item,
        codigo_empresa=codigo_empresa,
    )
    caminho_relatorio = salvar_relatorio_auditoria(
        df_discrepancias,
        codigo_item=codigo_item,
        codigo_empresa=codigo_empresa,
    )

    print(f"Movimentos auditados: {resumo['qtd_movimentos_auditados']}")
    print(f"Itens auditados: {resumo['qtd_itens_auditados']}")
    print(f"Campo auditado: {resumo['campo_auditado']}")
    print(f"Discrepancias encontradas: {resumo['qtd_discrepancias']}")
    print(f"Relatorio salvo em: {caminho_relatorio}")

    if not df_discrepancias.empty:
        print("Primeiras discrepancias encontradas:")
        print(df_discrepancias.head(20).to_string(index=False))
    else:
        print("Nenhuma discrepancia encontrada.")


if __name__ == "__main__":
    main()
