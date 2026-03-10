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
    "Preco",
    "valor",
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


def _coluna_tabela(coluna, alias=None):
    if not alias:
        return coluna
    return f"{alias}.{coluna}"


def _expressao_data_inventario(colunas, alias=None):
    if "datalan" in colunas:
        return _coluna_tabela("DataLan", alias)
    if "data" in colunas:
        return _coluna_tabela("[data]", alias)
    raise RuntimeError("Tabela de inventario nao possui coluna de data valida.")


def _expressao_ip_inventario(colunas, alias=None):
    if "ip" in colunas:
        return f"CAST({_coluna_tabela('ip', alias)} AS VARCHAR(50))"
    return "CAST(NULL AS VARCHAR(50))"


def _expressao_seqit_inventario(colunas, alias=None):
    if "seqit" in colunas:
        return _coluna_tabela("SEQIT", alias)
    if "registro" in colunas:
        return _coluna_tabela("Registro", alias)
    return "NULL"


def _expressao_seqit_tabela(engine, tabela, alias=None):
    return _expressao_seqit_inventario(_colunas_tabela(engine, tabela), alias)


def _expressao_ordem_inventario(colunas, alias=None):
    if "nrlan" in colunas:
        return f"TRY_CAST({_coluna_tabela('nrlan', alias)} AS BIGINT)"
    return f"TRY_CAST({_coluna_tabela('numdoc', alias)} AS BIGINT)"


def _expressao_numdoc_numerico(alias):
    numdoc_texto = f"LTRIM(RTRIM(CAST({_coluna_tabela('numdoc', alias)} AS VARCHAR(50))))"
    return f"TRY_CAST(NULLIF({numdoc_texto}, '') AS BIGINT)"


def extrair_movimentacoes_novas(engine, data_corte, tabela_inventario=None, codigo_item=None):
    filtro_item_vendas = ""
    filtro_item_pdc = ""
    filtro_item_transf = ""
    filtro_item_inv = ""
    params = {"data_corte": data_corte}

    if codigo_item is not None:
        filtro_item_vendas = "\n      AND iv.cditem_iv = :codigo_item"
        filtro_item_pdc = "\n      AND it.cditem = :codigo_item"
        filtro_item_transf = "\n      AND it.cditem = :codigo_item"
        filtro_item_inv = "\n      AND cditem = :codigo_item"
        params["codigo_item"] = codigo_item

    seqit_expr_movest_m = _expressao_seqit_tabela(engine, "T_MOVEST", alias="m")

    q_vendas = text(
        f"""
    WITH itens_venda AS (
        SELECT
            iv.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    iv.nrven_iv,
                    iv.cdemp_iv,
                    iv.cditem_iv,
                    ISNULL(iv.empitem, 1),
                    ISNULL(iv.qtdeSol_iv, 0),
                    ISNULL(iv.deitem_iv, ''),
                    ISNULL(iv.st, '')
                ORDER BY
                    CASE
                        WHEN ISNULL(iv.precpra_iv, 0) * ISNULL(iv.qtdeSol_iv, 0) > 0 THEN 0
                        WHEN ISNULL(iv.precven_iv, 0) * ISNULL(iv.qtdeSol_iv, 0) > 0 THEN 1
                        ELSE 2
                    END,
                    ISNULL(iv.precpra_iv, 0) DESC,
                    ISNULL(iv.precven_iv, 0) DESC,
                    iv.registro
            ) AS dup_rn
        FROM T_ITSVEN iv
    )
    SELECT v.nrven_v as numdoc, v.emisven_v as data, v.emisven_v as datadoc,
           iv.cdemp_iv as cdemp, iv.cditem_iv as cditem, iv.qtdeSol_iv as qtde,
           CASE WHEN v.TrocReq = 'S' THEN 'T' ELSE 'V' END as especie,
           CASE
               WHEN v.TrocReq = 'S' THEN iv.st
               WHEN v.status_v = 'C' THEN 'E'
               ELSE iv.st
           END as st,
           1 as clifor, 1 as empfor, ISNULL(iv.empitem, 1) as empitem,
           v.obsven_v as obs, CAST(v.obsven_v AS VARCHAR(255)) as obsit, v.codusu_v as codusu,
           CAST(v.ip AS VARCHAR(50)) as ip, v.cdemp_v as empven,
           CAST(ISNULL(iv.precpra_iv, 0) AS DECIMAL(18, 4)) as Preco,
           CAST(ISNULL(iv.precpra_iv, 0) * ISNULL(iv.qtdeSol_iv, 0) AS DECIMAL(18, 4)) as valor,
           iv.registro as SEQIT,
           (COALESCE(TRY_CAST(v.nrven_v AS BIGINT), 0) * 10) + 1 as _ordem
    FROM itens_venda iv
    JOIN T_VENDAS v ON iv.nrven_iv = v.nrven_v
    WHERE v.emisven_v >= :data_corte
    {filtro_item_vendas}
      AND (ISNULL(v.TrocReq, 'N') <> 'S' OR iv.dup_rn = 1)

    UNION ALL

    SELECT v.nrven_v as numdoc, v.emisven_v as data, v.emisven_v as datadoc,
           iv.cdemp_iv as cdemp, iv.cditem_iv as cditem, iv.qtdeSol_iv as qtde,
           'V' as especie, 'S' as st,
           1 as clifor, 1 as empfor, ISNULL(iv.empitem, 1) as empitem,
           v.obsven_v as obs, CAST(v.obsven_v AS VARCHAR(255)) as obsit, v.codusu_v as codusu,
           CAST(v.ip AS VARCHAR(50)) as ip, v.cdemp_v as empven,
           CAST(ISNULL(iv.precpra_iv, 0) AS DECIMAL(18, 4)) as Preco,
           CAST(ISNULL(iv.precpra_iv, 0) * ISNULL(iv.qtdeSol_iv, 0) AS DECIMAL(18, 4)) as valor,
           iv.registro as SEQIT,
           (COALESCE(TRY_CAST(v.nrven_v AS BIGINT), 0) * 10) as _ordem
    FROM itens_venda iv
    JOIN T_VENDAS v ON iv.nrven_iv = v.nrven_v
    WHERE v.emisven_v >= :data_corte
      {filtro_item_vendas}
      AND v.status_v = 'C'
      AND ISNULL(v.TrocReq, 'N') <> 'S'
      AND NOT EXISTS (
          SELECT 1
          FROM T_MOVEST m
          WHERE m.numdoc = v.nrven_v
            AND m.st = 'S'
            AND m.especie = 'V'
            AND m.cdemp = iv.cdemp_iv
            AND m.cditem = iv.cditem_iv
            AND ISNULL(TRY_CAST({seqit_expr_movest_m} AS BIGINT), 0) = ISNULL(TRY_CAST(iv.registro AS BIGINT), 0)
      )
    """
    )

    q_pdc = text(
        f"""
    SELECT p.nrNFC as numdoc, p.DtSta as data, p.DtSta as datadoc,
           p.empent as cdemp, it.cditem as cditem, it.QtSol as qtde,
           CASE WHEN p.StaReq = 'E' THEN 'C' ELSE 'D' END as especie,
           'E' as st,
           1 as clifor, 1 as empfor, 1 as empitem,
           p.obscmp as obs, CAST(p.obscmp AS VARCHAR(255)) as obsit, p.UsuSta as codusu,
           CAST(p.HOSTNAME AS VARCHAR(50)) as ip, p.cdemp as empven,
           CAST(0 AS DECIMAL(18, 4)) as Preco,
           CAST(0 AS DECIMAL(18, 4)) as valor,
           it.Registro as SEQIT,
           COALESCE(TRY_CAST(p.NrReq AS BIGINT), 0) as _ordem
    FROM T_ITPDC it
    JOIN T_PDC p ON it.Nrreq = p.NrReq
    WHERE p.DtSta >= :data_corte AND p.StaReq IN ('E', 'A')
      {filtro_item_pdc}
    """
    )

    q_transf = text(
        f"""
    SELECT t.codtransf as numdoc, COALESCE(t.datahorarec, t.datahoratransf) as data,
           COALESCE(t.datahorarec, t.datahoratransf) as datadoc,
           t.cdempsaida as cdemp, it.cditem as cditem, it.qtditem as qtde,
           'F' as especie, 'S' as st,
           1 as clifor, 1 as empfor, 1 as empitem,
           t.observacao as obs, CAST(t.observacao AS VARCHAR(255)) as obsit, t.codusu_transf as codusu,
           CAST(t.codusu_rec AS VARCHAR(50)) as ip, t.cdempsaida as empven,
           CAST(0 AS DECIMAL(18, 4)) as Preco,
           CAST(0 AS DECIMAL(18, 4)) as valor,
           it.cditemtransf as SEQIT,
           COALESCE(TRY_CAST(t.codtransf AS BIGINT), 0) as _ordem
    FROM T_ITTRANSF it
    JOIN T_TRANSF t ON it.cdtransf = t.codtransf
    WHERE COALESCE(t.datahorarec, t.datahoratransf) >= :data_corte AND t.statustransf = 'E'
      {filtro_item_transf}

    UNION ALL

    SELECT t.codtransf as numdoc, COALESCE(t.datahorarec, t.datahoratransf) as data,
           COALESCE(t.datahorarec, t.datahoratransf) as datadoc,
           t.cdempentrada as cdemp, it.cditem as cditem, it.qtditem as qtde,
           'F' as especie, 'E' as st,
           1 as clifor, 1 as empfor, 1 as empitem,
           t.observacao as obs, CAST(t.observacao AS VARCHAR(255)) as obsit, t.codusu_transf as codusu,
           CAST(t.codusu_rec AS VARCHAR(50)) as ip, t.cdempsaida as empven,
           CAST(0 AS DECIMAL(18, 4)) as Preco,
           CAST(0 AS DECIMAL(18, 4)) as valor,
           it.cditemtransf as SEQIT,
           COALESCE(TRY_CAST(t.codtransf AS BIGINT), 0) as _ordem
    FROM T_ITTRANSF it
    JOIN T_TRANSF t ON it.cdtransf = t.codtransf
    WHERE COALESCE(t.datahorarec, t.datahoratransf) >= :data_corte AND t.statustransf = 'E'
      {filtro_item_transf}
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
    ordem_expr_inv = _expressao_ordem_inventario(colunas_inv)
    data_expr_inv_m = _expressao_data_inventario(colunas_inv, alias="m")
    ip_expr_inv_m = _expressao_ip_inventario(colunas_inv, alias="m")
    seqit_expr_inv_m = _expressao_seqit_inventario(colunas_inv, alias="m")
    ordem_expr_inv_m = _expressao_ordem_inventario(colunas_inv, alias="m")
    numdoc_numerico_inv_m = _expressao_numdoc_numerico(alias="m")

    q_inv = text(
        f"""
    SELECT numdoc, {data_expr_inv} as data, {data_expr_inv} as datadoc,
           cdemp, cditem, qtde, 'I' as especie, st,
           1 as clifor, 1 as empfor, 1 as empitem,
           obs, CAST(obs AS VARCHAR(255)) as obsit, codusu,
           {ip_expr_inv} as ip, cdemp as empven,
           CAST(0 AS DECIMAL(18, 4)) as Preco,
           CAST(0 AS DECIMAL(18, 4)) as valor,
           {seqit_expr_inv} as SEQIT,
           {ordem_expr_inv} as _ordem
    FROM dbo.{tabela_inv}
    WHERE especie = 'I' AND {data_expr_inv} >= :data_corte AND {data_expr_inv} <= GETDATE()
      {filtro_item_inv}
    """
    )

    q_avulsas = text(
        f"""
    SELECT m.numdoc, {data_expr_inv_m} as data, {data_expr_inv_m} as datadoc,
           m.cdemp, m.cditem, m.qtde, m.especie, m.st,
           1 as clifor, 1 as empfor, 1 as empitem,
           m.obs, CAST(m.obs AS VARCHAR(255)) as obsit, m.codusu,
           {ip_expr_inv_m} as ip, m.cdemp as empven,
           CAST(0 AS DECIMAL(18, 4)) as Preco,
           CAST(0 AS DECIMAL(18, 4)) as valor,
           {seqit_expr_inv_m} as SEQIT,
           {ordem_expr_inv_m} as _ordem
    FROM dbo.{tabela_inv} m
    WHERE {data_expr_inv_m} >= :data_corte
      AND {data_expr_inv_m} <= GETDATE()
      {"AND m.cditem = :codigo_item" if codigo_item is not None else ""}
      AND (
          (m.st = 'S' AND m.especie IN ('C', 'O', 'A'))
          OR (
              m.st = 'E'
              AND m.especie = 'C'
              AND NOT EXISTS (
                  SELECT 1
                  FROM T_PDC p
                  WHERE TRY_CAST(NULLIF(LTRIM(RTRIM(CAST(p.nrNFC AS VARCHAR(50)))), '') AS BIGINT) = {numdoc_numerico_inv_m}
              )
          )
          OR (
              m.st = 'E'
              AND m.especie = 'T'
              AND NOT EXISTS (
                  SELECT 1
                  FROM T_TRANSF t
                  WHERE TRY_CAST(t.codtransf AS BIGINT) = {numdoc_numerico_inv_m}
              )
          )
          OR (m.st = 'E' AND m.especie = 'O')
      )
    """
    )

    df_v = pd.read_sql(q_vendas, engine, params=params)
    df_p = pd.read_sql(q_pdc, engine, params=params)
    df_t = pd.read_sql(q_transf, engine, params=params)
    df_i = pd.read_sql(q_inv, engine, params=params)
    df_a = pd.read_sql(q_avulsas, engine, params=params)

    dataframes = [df for df in (df_v, df_p, df_t, df_i, df_a) if not df.empty]
    if not dataframes:
        return pd.DataFrame(columns=COLUNAS_UNIFICADAS)

    unificado = pd.concat(dataframes, ignore_index=True)
    unificado = unificado.loc[:, ~unificado.columns.duplicated()].copy()
    return unificado[COLUNAS_UNIFICADAS]
