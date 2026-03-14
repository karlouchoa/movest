-- Origem: dbo.T_MOVEST_INV_20260314_035408
-- Destino: dbo.T_MOVEST
-- Gerado em: 2026-03-14T03:54:23.369078
-- Execucao manual: rode este script apos a carga da nova T_MOVEST.
-- Em caso de falha, a transacao deve ser revertida automaticamente.
SET XACT_ABORT ON;
GO
BEGIN TRANSACTION;

-- DROP DE OBJETOS NA TABELA RENOMEADA
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260314_035408] DROP CONSTRAINT [PK_T_MOVEST_final]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260314_035408] DROP CONSTRAINT [DF_T_MOVEST_DataLan]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260314_035408] DROP CONSTRAINT [DF_t_movest_createdat]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260314_035408] DROP CONSTRAINT [DF_t_movest_isdeleted]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260314_035408] DROP CONSTRAINT [DF_t_movest_updatedat]
GO
DROP INDEX [IX_T_MOVEST_2] ON dbo.[T_MOVEST_INV_20260314_035408]
GO
DROP INDEX [IX_T_MOVEST_3] ON dbo.[T_MOVEST_INV_20260314_035408]
GO
DROP TRIGGER dbo.[trg_dbo_t_movest_UpdateTimestamp]
GO
DROP TRIGGER dbo.[trg_t_movest_AtualizaSaldo]
GO

-- CREATE DE INDICES, CONSTRAINTS, PK, FK E TRIGGERS NA NOVA T_MOVEST
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [PK_T_MOVEST_final] UNIQUE CLUSTERED ([nrlan] ASC)
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_T_MOVEST_DataLan] DEFAULT (getdate()) FOR [DataLan]
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_createdat] DEFAULT (getdate()) FOR [createdat]
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_isdeleted] DEFAULT ((0)) FOR [isdeleted]
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_updatedat] DEFAULT (getdate()) FOR [updatedat]
GO
CREATE NONCLUSTERED INDEX [IX_T_MOVEST_2] ON dbo.[T_MOVEST] ([cditem] ASC, [empitem] ASC, [cdemp] ASC)
GO
CREATE NONCLUSTERED INDEX [IX_T_MOVEST_3] ON dbo.[T_MOVEST] ([DataLan] ASC)
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
CREATE TRIGGER [dbo].[trg_t_movest_AtualizaSaldo] 
ON [dbo].[T_MOVEST] 
AFTER INSERT, UPDATE 
AS 
BEGIN 
    SET NOCOUNT ON; 
    
    
    DECLARE @Mov TABLE ( 
        nrlan INT, cdemp INT, cditem INT, qtde DECIMAL(18,4), st VARCHAR(1), 
        empitem INT, codusu VARCHAR(10) 
    ); 
    
    INSERT INTO @Mov (nrlan, cdemp, cditem, qtde, st, empitem, codusu) 
    SELECT nrlan, cdemp, cditem, qtde, st, empitem, codusu FROM inserted; 
    
    
    INSERT INTO dbo.t_saldoit (cdemp, cditem, saldo, empitem) 
    SELECT DISTINCT m.cdemp, m.cditem, 0, ISNULL(m.empitem, 1) 
    FROM @Mov m 
    WHERE NOT EXISTS ( 
        SELECT 1 FROM dbo.t_saldoit s WHERE s.cdemp = m.cdemp 
        AND s.cditem = m.cditem AND s.empitem = ISNULL(m.empitem, 1) 
    ); 
    
    
    UPDATE real_mov 
    SET real_mov.saldoant = (SELECT ISNULL(SUM(saldo), 0) FROM dbo.t_saldoit s WHERE s.cditem = tmp.cditem AND s.empitem = tmp.empitem), 
        real_mov.sldantemp = (SELECT ISNULL(saldo, 0) FROM dbo.t_saldoit s WHERE s.cditem = tmp.cditem AND s.empitem = tmp.empitem AND s.cdemp = tmp.cdemp) 
    FROM dbo.t_movest real_mov 
    INNER JOIN @Mov tmp ON real_mov.nrlan = tmp.nrlan; 
    
    
    UPDATE s SET s.saldo = s.saldo + m.qtde FROM dbo.t_saldoit s 
    INNER JOIN @Mov m ON s.cdemp = m.cdemp AND s.cditem = m.cditem AND s.empitem = m.empitem 
    WHERE m.st = 'E'; 
    
    UPDATE s SET s.saldo = s.saldo - m.qtde FROM dbo.t_saldoit s 
    INNER JOIN @Mov m ON s.cdemp = m.cdemp AND s.cditem = m.cditem AND s.empitem = m.empitem 
    WHERE m.st = 'S'; 
    
    
    INSERT INTO t_auditoriaEstoque (data, usuario, cdemp, cditem, qtde, tipo, saldo_ant, saldo_atual, origem, NRLAN) 
    SELECT GETDATE(), m.codusu, m.cdemp, m.cditem, m.qtde, m.st, m.saldoant, 
           (SELECT ISNULL(saldo, 0) FROM dbo.t_saldoit s WHERE s.cdemp = m.cdemp AND s.cditem = m.cditem AND s.empitem = m.empitem), 
           'T_MOVEST', m.nrlan 
    FROM dbo.t_movest m 
    INNER JOIN @Mov i ON m.nrlan = i.nrlan; 
END
GO
COMMIT TRANSACTION;
GO
