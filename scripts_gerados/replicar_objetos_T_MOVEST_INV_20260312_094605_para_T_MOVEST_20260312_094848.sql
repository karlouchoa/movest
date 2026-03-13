-- Origem: dbo.T_MOVEST_INV_20260312_094605
-- Destino: dbo.T_MOVEST
-- Gerado em: 2026-03-12T09:48:48.553085
-- Execucao manual: rode este script apos a carga da nova T_MOVEST.
-- Em caso de falha, a transacao deve ser revertida automaticamente.
SET XACT_ABORT ON;
GO
BEGIN TRANSACTION;

-- DROP DE OBJETOS NA TABELA RENOMEADA
GO

-- CREATE DE INDICES, CONSTRAINTS, PK, FK E TRIGGERS NA NOVA T_MOVEST
GO
COMMIT TRANSACTION;
GO
