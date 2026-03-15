-- Origem: dbo.T_MOVEST_INV_20260315_122904
-- Destino: dbo.T_MOVEST
-- Gerado em: 2026-03-15T12:29:16.217078
-- Execucao manual: rode este script apos a carga da nova T_MOVEST.
-- Em caso de falha, a transacao deve ser revertida automaticamente.
SET XACT_ABORT ON;
GO
BEGIN TRANSACTION;

-- DROP DE OBJETOS NA TABELA RENOMEADA
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_122904] DROP CONSTRAINT [PK_t_movest_final]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_122904] DROP CONSTRAINT [DF_t_movest_DataLan]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_122904] DROP CONSTRAINT [DF_t_movest_isdeleted]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_122904] DROP CONSTRAINT [DF_t_movest_data]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_122904] DROP CONSTRAINT [DF_t_movest_createdat]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_122904] DROP CONSTRAINT [DF_t_movest_datadoc]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_122904] DROP CONSTRAINT [DF_t_movest_updatedat]
GO
DROP INDEX [IX_t_movest] ON dbo.[T_MOVEST_INV_20260315_122904]
GO
DROP INDEX [IX_t_movest_1] ON dbo.[T_MOVEST_INV_20260315_122904]
GO
DROP TRIGGER dbo.[trg_t_movest_AtualizaSaldo]
GO
DROP TRIGGER dbo.[trg_dbo_t_movest_UpdateTimestamp]
GO

-- CREATE DE INDICES, CONSTRAINTS, PK, FK E TRIGGERS NA NOVA T_MOVEST
GO
ALTER TABLE dbo.[T_MOVEST] ADD [createdat] datetime2(0) CONSTRAINT [DF_t_movest_createdat] DEFAULT (getdate()) WITH VALUES NULL
GO
ALTER TABLE dbo.[T_MOVEST] ADD [isdeleted] bit CONSTRAINT [DF_t_movest_isdeleted] DEFAULT ((0)) WITH VALUES NOT NULL
GO
ALTER TABLE dbo.[T_MOVEST] ADD [updatedat] datetime2(0) CONSTRAINT [DF_t_movest_updatedat] DEFAULT (getdate()) WITH VALUES NULL
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [PK_t_movest_final] UNIQUE CLUSTERED ([nrlan] ASC)
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_DataLan] DEFAULT (getdate()) FOR [datalan]
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_data] DEFAULT (getdate()) FOR [data]
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_datadoc] DEFAULT (getdate()) FOR [datadoc]
GO
CREATE NONCLUSTERED INDEX [IX_t_movest] ON dbo.[T_MOVEST] ([cditem] ASC)
GO
CREATE NONCLUSTERED INDEX [IX_t_movest_1] ON dbo.[T_MOVEST] ([data] ASC)
GO
CREATE TRIGGER [dbo].[trg_t_movest_AtualizaSaldo] 
ON [dbo].[t_movest] 
AFTER INSERT 
AS 
BEGIN 
    SET NOCOUNT ON; 
    
    -- 1) Tabela temporária para processar o lote de inserção
    DECLARE @Mov TABLE ( 
        nrlan INT, cdemp INT, cditem INT, qtde DECIMAL(18,4), st VARCHAR(1), 
        empitem INT, codusu VARCHAR(10) 
    ); 
    
    INSERT INTO @Mov (nrlan, cdemp, cditem, qtde, st, empitem, codusu) 
    SELECT nrlan, cdemp, cditem, qtde, st, empitem, codusu FROM inserted; 
    
    -- 2) Garante que o registro de saldo exista para evitar nulos
    INSERT INTO dbo.t_saldoit (cdemp, cditem, saldo, empitem) 
    SELECT DISTINCT m.cdemp, m.cditem, 0, ISNULL(m.empitem, 1) 
    FROM @Mov m 
    WHERE NOT EXISTS ( 
        SELECT 1 FROM dbo.t_saldoit s WHERE s.cdemp = m.cdemp 
        AND s.cditem = m.cditem AND s.empitem = ISNULL(m.empitem, 1) 
    ); 
    
    -- 3) Grava os saldos anteriores na t_movest (Corrigido o Alias)
    UPDATE real_mov 
    SET real_mov.saldoant = (SELECT ISNULL(SUM(saldo), 0) FROM dbo.t_saldoit s WHERE s.cditem = tmp.cditem AND s.empitem = tmp.empitem), 
        real_mov.sldantemp = (SELECT ISNULL(saldo, 0) FROM dbo.t_saldoit s WHERE s.cditem = tmp.cditem AND s.empitem = tmp.empitem AND s.cdemp = tmp.cdemp) 
    FROM dbo.t_movest real_mov 
    INNER JOIN @Mov tmp ON real_mov.nrlan = tmp.nrlan; 
    
    -- 4) Atualiza os saldos físicos (Entradas e Saídas)
    UPDATE s SET s.saldo = s.saldo + m.qtde FROM dbo.t_saldoit s 
    INNER JOIN @Mov m ON s.cdemp = m.cdemp AND s.cditem = m.cditem AND s.empitem = m.empitem 
    WHERE m.st = 'E'; 
    
    UPDATE s SET s.saldo = s.saldo - m.qtde FROM dbo.t_saldoit s 
    INNER JOIN @Mov m ON s.cdemp = m.cdemp AND s.cditem = m.cditem AND s.empitem = m.empitem 
    WHERE m.st = 'S'; 
    
    -- 5) Grava auditoria (Usando nrlan como referência segura)
    INSERT INTO t_auditoriaEstoque (data, usuario, cdemp, cditem, qtde, tipo, saldo_ant, saldo_atual, origem, NRLAN) 
    SELECT GETDATE(), m.codusu, m.cdemp, m.cditem, m.qtde, m.st, m.saldoant, 
           (SELECT ISNULL(saldo, 0) FROM dbo.t_saldoit s WHERE s.cdemp = m.cdemp AND s.cditem = m.cditem AND s.empitem = m.empitem), 
           'T_MOVEST', m.nrlan 
    FROM dbo.t_movest m 
    INNER JOIN @Mov i ON m.nrlan = i.nrlan; 
END
GO
CREATE TRIGGER trg_dbo_t_movest_UpdateTimestamp 
 ON t_movest 
 AFTER UPDATE 
 AS 
 BEGIN 
     SET NOCOUNT ON; 
     
     /* Evita recursão e dispara apenas se a coluna UpdatedAt não for a que está sendo alterada*/ 
     IF TRIGGER_NESTLEVEL() > 1 RETURN; 
 
     /* Só atualiza se houver mudança real (evita updates fantasmas)*/ 
     UPDATE m 
     SET m.UpdatedAt = GETDATE() 
     FROM dbo.t_movest AS m 
     INNER JOIN inserted AS i ON m.nrlan = i.nrlan; 
 END
GO
COMMIT TRANSACTION;
GO
