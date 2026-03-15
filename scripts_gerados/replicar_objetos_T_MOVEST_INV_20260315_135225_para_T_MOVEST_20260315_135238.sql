-- Origem: dbo.T_MOVEST_INV_20260315_135225
-- Destino: dbo.T_MOVEST
-- Gerado em: 2026-03-15T13:52:38.239277
-- Execucao manual: rode este script apos a carga da nova T_MOVEST.
-- Em caso de falha, a transacao deve ser revertida automaticamente.
SET XACT_ABORT ON;
GO
BEGIN TRANSACTION;

-- DROP DE OBJETOS NA TABELA RENOMEADA
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_135225] DROP CONSTRAINT [PK_t_movest_final]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_135225] DROP CONSTRAINT [DF_t_movest_isdeleted]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_135225] DROP CONSTRAINT [DF_t_movest_DataLan]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_135225] DROP CONSTRAINT [DF_t_movest_createdat]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_135225] DROP CONSTRAINT [DF_t_movest_updatedat]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_135225] DROP CONSTRAINT [DF_t_movest_data]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260315_135225] DROP CONSTRAINT [DF_t_movest_datadoc]
GO
DROP INDEX [IX_t_movest] ON dbo.[T_MOVEST_INV_20260315_135225]
GO
DROP INDEX [IX_t_movest_1] ON dbo.[T_MOVEST_INV_20260315_135225]
GO
DROP TRIGGER dbo.[trg_dbo_t_movest_UpdateTimestamp]
GO

-- CREATE DE INDICES, CONSTRAINTS, PK, FK E TRIGGERS NA NOVA T_MOVEST
GO
ALTER TABLE dbo.[T_MOVEST] ADD [createdat] datetime CONSTRAINT [DF_t_movest_createdat] DEFAULT (getdate()) WITH VALUES NULL
GO
ALTER TABLE dbo.[T_MOVEST] ADD [isdeleted] bit CONSTRAINT [DF_t_movest_isdeleted] DEFAULT ((0)) WITH VALUES NOT NULL
GO
ALTER TABLE dbo.[T_MOVEST] ADD [updatedat] datetime CONSTRAINT [DF_t_movest_updatedat] DEFAULT (getdate()) WITH VALUES NULL
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
