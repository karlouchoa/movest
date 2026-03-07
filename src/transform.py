import pandas as pd
from sqlalchemy import inspect, text

COLUNAS_UNIFICADAS = [
    "numdoc",
    "data",
    "datadoc",
    "cdemp",
    "cditem",
    "qtde",
    "especie",
    "st",
    "clifor",
    "empfor",
    "empitem",
    "obs",
    "obsit",
    "codusu",
    "ip",
    "empven",
    "SEQIT",
    "_ordem",
]


def _resolver_nome_tabela(engine, nome_tabela):
    inspector = inspect(engine)
    tabelas = inspector.get_table_names(schema="dbo")
    lookup = {t.lower(): t for t in tabelas}
    return lookup.get(nome_tabela.lower())


def _detectar_tabela_inventario(engine):
    inspector = inspect(engine)
    tabelas = inspector.get_table_names(schema="dbo")
    lookup = {t.lower(): t for t in tabelas}

    for candidata in ("t_movest_bak", "t_movest_bkp", "t_movest"):
        resolvida = lookup.get(candidata)
        if resolvida:
            return resolvida

    raise RuntimeError("Nenhuma tabela de inventario encontrada (T_MOVEST_BAK/T_MOVEST_BKP/T_MOVEST).")


def _colunas_tabela(engine, tabela):
    inspector = inspect(engine)
    return {c["name"].lower() for c in inspector.get_columns(tabela, schema="dbo")}


def _expressao_data_inventario(colunas):
    if "datalan" in colunas:
        return "DataLan"
    if "data" in colunas:
        return "[data]"
    raise RuntimeError("Tabela de inventario nao possui coluna de data valida.")


def _expressao_ip_inventario(colunas):
    if "ip" in colunas:
        return "CAST(ip AS VARCHAR(50))"
    return "CAST(NULL AS VARCHAR(50))"


def _expressao_seqit_inventario(colunas):
    if "seqit" in colunas:
        return "SEQIT"
    if "registro" in colunas:
        return "Registro"
    return "NULL"


def extrair_movimentacoes_novas(engine, data_corte, tabela_inventario=None):
    q_vendas = text(
        """
    SELECT v.nrven_v as numdoc, v.emisven_v as data, v.emisven_v as datadoc,
           iv.cdemp_iv as cdemp, iv.cditem_iv as cditem, iv.qtdeSol_iv as qtde,
           'V' as especie, CASE WHEN v.status_v = 'C' THEN 'E' ELSE iv.st END as st,
           1 as clifor, 1 as empfor, 1 as empitem,
           v.obsven_v as obs, CAST(v.obsven_v AS VARCHAR(255)) as obsit, v.codusu_v as codusu,
           CAST(v.ip AS VARCHAR(50)) as ip, v.cdemp_v as empven,
           iv.registro as SEQIT,
           (CAST(v.nrven_v AS BIGINT) * 10) + 1 as _ordem
    FROM T_ITSVEN iv
    JOIN T_VENDAS v ON iv.nrven_iv = v.nrven_v
    WHERE v.emisven_v >= :data_corte

    UNION ALL

    SELECT v.nrven_v as numdoc, v.emisven_v as data, v.emisven_v as datadoc,
           iv.cdemp_iv as cdemp, iv.cditem_iv as cditem, iv.qtdeSol_iv as qtde,
           'V' as especie, 'S' as st,
           1 as clifor, 1 as empfor, 1 as empitem,
           v.obsven_v as obs, CAST(v.obsven_v AS VARCHAR(255)) as obsit, v.codusu_v as codusu,
           CAST(v.ip AS VARCHAR(50)) as ip, v.cdemp_v as empven,
           iv.registro as SEQIT,
           (CAST(v.nrven_v AS BIGINT) * 10) as _ordem
    FROM T_ITSVEN iv
    JOIN T_VENDAS v ON iv.nrven_iv = v.nrven_v
    WHERE v.emisven_v >= :data_corte
      AND v.status_v = 'C'
      AND NOT EXISTS (
          SELECT 1
          FROM T_MOVEST m
          WHERE m.numdoc = v.nrven_v
            AND m.st = 'S'
            AND m.especie = 'V'
      )
    """
    )

    q_pdc = text(
        """
    SELECT p.nrNFC as numdoc, p.DtSta as data, p.DtSta as datadoc,
           p.empent as cdemp, it.cditem as cditem, it.QtSol as qtde,
           CASE WHEN p.StaReq = 'E' THEN 'C' ELSE 'D' END as especie,
           'E' as st,
           1 as clifor, 1 as empfor, 1 as empitem,
           p.obscmp as obs, CAST(p.obscmp AS VARCHAR(255)) as obsit, p.UsuSta as codusu,
           CAST(p.HOSTNAME AS VARCHAR(50)) as ip, p.cdemp as empven,
           it.Registro as SEQIT,
           CAST(p.NrReq AS BIGINT) as _ordem
    FROM T_ITPDC it
    JOIN T_PDC p ON it.Nrreq = p.NrReq
    WHERE p.DtSta >= :data_corte AND p.StaReq IN ('E', 'A')
    """
    )

    q_transf = text(
        """
    SELECT t.codtransf as numdoc, COALESCE(t.datahorarec, t.datahoratransf) as data,
           COALESCE(t.datahorarec, t.datahoratransf) as datadoc,
           t.cdempsaida as cdemp, it.cditem as cditem, it.qtditem as qtde,
           'F' as especie, 'S' as st,
           1 as clifor, 1 as empfor, 1 as empitem,
           t.observacao as obs, CAST(t.observacao AS VARCHAR(255)) as obsit, t.codusu_transf as codusu,
           CAST(t.codusu_rec AS VARCHAR(50)) as ip, t.cdempsaida as empven,
           it.cditemtransf as SEQIT,
           CAST(t.codtransf AS BIGINT) as _ordem
    FROM T_ITTRANSF it
    JOIN T_TRANSF t ON it.cdtransf = t.codtransf
    WHERE COALESCE(t.datahorarec, t.datahoratransf) >= :data_corte AND t.statustransf = 'E'

    UNION ALL

    SELECT t.codtransf as numdoc, COALESCE(t.datahorarec, t.datahoratransf) as data,
           COALESCE(t.datahorarec, t.datahoratransf) as datadoc,
           t.cdempentrada as cdemp, it.cditem as cditem, it.qtditem as qtde,
           'F' as especie, 'E' as st,
           1 as clifor, 1 as empfor, 1 as empitem,
           t.observacao as obs, CAST(t.observacao AS VARCHAR(255)) as obsit, t.codusu_transf as codusu,
           CAST(t.codusu_rec AS VARCHAR(50)) as ip, t.cdempsaida as empven,
           it.cditemtransf as SEQIT,
           CAST(t.codtransf AS BIGINT) as _ordem
    FROM T_ITTRANSF it
    JOIN T_TRANSF t ON it.cdtransf = t.codtransf
    WHERE COALESCE(t.datahorarec, t.datahoratransf) >= :data_corte AND t.statustransf = 'E'
    """
    )

    if tabela_inventario:
        tabela_inv = _resolver_nome_tabela(engine, tabela_inventario)
        if not tabela_inv:
            raise RuntimeError(f"Tabela de inventario informada nao encontrada: {tabela_inventario}")
    else:
        tabela_inv = _detectar_tabela_inventario(engine)

    colunas_inv = _colunas_tabela(engine, tabela_inv)
    data_expr_inv = _expressao_data_inventario(colunas_inv)
    ip_expr_inv = _expressao_ip_inventario(colunas_inv)
    seqit_expr_inv = _expressao_seqit_inventario(colunas_inv)

    q_inv = text(
        f"""
    SELECT numdoc, {data_expr_inv} as data, {data_expr_inv} as datadoc,
           cdemp, cditem, qtde, 'I' as especie, st,
           1 as clifor, 1 as empfor, 1 as empitem,
           obs, CAST(obs AS VARCHAR(255)) as obsit, codusu,
           {ip_expr_inv} as ip, cdemp as empven,
           {seqit_expr_inv} as SEQIT,
           CAST(numdoc AS BIGINT) as _ordem
    FROM {tabela_inv}
    WHERE especie = 'I' AND {data_expr_inv} >= :data_corte AND {data_expr_inv} <= GETDATE()
    """
    )

    params = {"data_corte": data_corte}
    df_v = pd.read_sql(q_vendas, engine, params=params)
    df_p = pd.read_sql(q_pdc, engine, params=params)
    df_t = pd.read_sql(q_transf, engine, params=params)
    df_i = pd.read_sql(q_inv, engine, params=params)

    unificado = pd.concat([df_v, df_p, df_t, df_i], ignore_index=True)
    unificado = unificado.loc[:, ~unificado.columns.duplicated()].copy()
    return unificado[COLUNAS_UNIFICADAS]
