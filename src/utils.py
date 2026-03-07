from sqlalchemy import text


def recriar_indices(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                IF EXISTS (
                    SELECT 1
                    FROM sys.indexes
                    WHERE object_id = OBJECT_ID('dbo.T_MOVEST')
                      AND name = 'IX_T_MOVEST_Data'
                )
                DROP INDEX IX_T_MOVEST_Data ON dbo.T_MOVEST
                """
            )
        )
        conn.execute(
            text(
                """
                IF EXISTS (
                    SELECT 1
                    FROM sys.indexes
                    WHERE object_id = OBJECT_ID('dbo.T_MOVEST')
                      AND name = 'IX_T_MOVEST_ItemEmp'
                )
                DROP INDEX IX_T_MOVEST_ItemEmp ON dbo.T_MOVEST
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE NONCLUSTERED INDEX IX_T_MOVEST_Data
                ON dbo.T_MOVEST (DataLan, nrlan)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE NONCLUSTERED INDEX IX_T_MOVEST_ItemEmp
                ON dbo.T_MOVEST (cditem, cdemp)
                """
            )
        )


def atualizar_saldos_finais(conn, saldos_finais_item_emp):
    for (cditem, cdemp), saldo in saldos_finais_item_emp.items():
        conn.execute(
            text("UPDATE t_saldoit SET saldo = :s WHERE cditem = :i AND cdemp = :e"),
            {"s": saldo, "i": cditem, "e": cdemp},
        )
