from datetime import datetime

from main import obter_data_corte_base, solicitar_parametros_conexao, validar_conexao
from src.auditoria import (
    auditar_anomalias_movest,
    resumir_anomalias_por_item,
    salvar_relatorio_anomalias_movest,
    salvar_relatorio_resumo_anomalias_por_item,
)
from src.database import get_engine


def solicitar_codigo_empresa():
    codigo_empresa_raw = input("Informe o codigo da empresa [todas]: ").strip()
    if not codigo_empresa_raw:
        return None
    if not codigo_empresa_raw.isdigit() or int(codigo_empresa_raw) <= 0:
        raise ValueError("O codigo da empresa deve ser um numero inteiro maior que zero.")
    return int(codigo_empresa_raw)


def main():
    (
        servidor,
        banco_base,
        banco_atual,
        username,
        password,
        codigo_item,
        importa_ajuste_inventario,
    ) = solicitar_parametros_conexao()
    codigo_empresa = solicitar_codigo_empresa()

    engine_base = get_engine(servidor, banco_base, username, password)
    engine_atual = get_engine(servidor, banco_atual, username, password)

    validar_conexao(engine_base, banco_base, "base", servidor)
    validar_conexao(engine_atual, banco_atual, "atual", servidor)

    data_corte, data_maxima_base, coluna_data_base = obter_data_corte_base(engine_base)
    if data_maxima_base and data_maxima_base > datetime.now():
        print(
            f"Data maxima futura detectada em T_MOVEST.{coluna_data_base} ({data_maxima_base}). "
            f"Usando a maior data valida ate hoje: {data_corte}"
        )
    elif data_maxima_base:
        print(f"Data maxima em T_MOVEST.{coluna_data_base}: {data_maxima_base}")

    print(f"Data base usada na simulacao: {data_corte}")
    if codigo_item is not None:
        print(f"Auditoria filtrada para o item {codigo_item}.")
    else:
        print("Auditoria para todos os itens.")
    if codigo_empresa is not None:
        print(f"Relatorio final filtrado para a empresa {codigo_empresa}.")
    else:
        print("Relatorio final para todas as empresas.")
    print(
        "Ajustes de inventario considerados na simulacao: "
        + ("sim" if importa_ajuste_inventario else "nao")
    )

    print("Executando varredura detalhada da T_MOVEST...")
    df_anomalias, resumo = auditar_anomalias_movest(
        engine_base,
        engine_atual,
        data_corte,
        codigo_item=codigo_item,
        codigo_empresa=codigo_empresa,
        importar_ajuste_inventario=importa_ajuste_inventario,
    )
    df_resumo_itens = resumir_anomalias_por_item(df_anomalias)
    caminho_relatorio = salvar_relatorio_anomalias_movest(
        df_anomalias,
        codigo_item=codigo_item,
        codigo_empresa=codigo_empresa,
    )
    caminho_resumo_itens = salvar_relatorio_resumo_anomalias_por_item(
        df_resumo_itens,
        codigo_item=codigo_item,
        codigo_empresa=codigo_empresa,
    )

    print(f"Movimentos atuais analisados: {resumo['qtd_movimentos_atuais_analisados']}")
    print(f"Movimentos esperados na simulacao: {resumo['qtd_movimentos_esperados']}")
    print(f"Ajustes de inventario lidos: {resumo['qtd_ajustes_inventario_lidos']}")
    print(f"Anomalias encontradas: {resumo['qtd_anomalias']}")
    print(f"Itens com anomalia: {resumo['qtd_itens_com_anomalia']}")
    print(f"Tipos de anomalia encontrados: {resumo['qtd_tipos_anomalia']}")
    print(f"Relatorio salvo em: {caminho_relatorio}")
    print(f"Resumo por item salvo em: {caminho_resumo_itens}")

    if not df_anomalias.empty:
        if "possui_ajuste_inventario_posterior" in df_anomalias.columns:
            qtd_com_ajuste = int(
                df_anomalias["possui_ajuste_inventario_posterior"].fillna(False).astype(bool).sum()
            )
            print(f"Anomalias com ajuste de inventario posterior: {qtd_com_ajuste}")
        if not df_resumo_itens.empty:
            print("Itens anomalos e quantidade de anomalias:")
            print(df_resumo_itens.head(50).to_string(index=False))
        print("Resumo por tipo:")
        print(df_anomalias["tipo"].value_counts().to_string())
        print("Primeiras anomalias:")
        print(df_anomalias.head(20).to_string(index=False))
    else:
        print("Nenhuma anomalia encontrada.")


if __name__ == "__main__":
    main()
