-- Origem: dbo.T_MOVEST_INV_20260316_115116
-- Destino: dbo.T_MOVEST
-- Gerado em: 2026-03-16T11:52:00.644726
-- Execucao manual: rode este script apos a carga da nova T_MOVEST.
-- Em caso de falha, a transacao deve ser revertida automaticamente.
SET XACT_ABORT ON;
GO
BEGIN TRANSACTION;

-- DROP DE OBJETOS NA TABELA RENOMEADA
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260316_115116] DROP CONSTRAINT [PK_t_movest]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260316_115116] DROP CONSTRAINT [DF__t_movest__ID__393D4D3D]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260316_115116] DROP CONSTRAINT [DF_t_movest_isdeleted]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260316_115116] DROP CONSTRAINT [DF_t_movest_DataLan]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260316_115116] DROP CONSTRAINT [DF_t_movest_data]
GO
ALTER TABLE dbo.[T_MOVEST_INV_20260316_115116] DROP CONSTRAINT [DF_t_movest_datadoc]
GO
DROP INDEX [idx_cditem] ON dbo.[T_MOVEST_INV_20260316_115116]
GO
DROP INDEX [idx_data] ON dbo.[T_MOVEST_INV_20260316_115116]
GO
DROP INDEX [idx_numdoc] ON dbo.[T_MOVEST_INV_20260316_115116]
GO
DROP TRIGGER dbo.[trg_t_movest_AtualizaSaldo]
GO

-- CREATE DE INDICES, CONSTRAINTS, PK, FK E TRIGGERS NA NOVA T_MOVEST
GO
ALTER TABLE dbo.[T_MOVEST] ADD [ID] uniqueidentifier CONSTRAINT [DF__t_movest__ID__393D4D3D] DEFAULT (newid()) WITH VALUES NULL
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [PK_t_movest] UNIQUE CLUSTERED ([nrlan] ASC, [cdemp] ASC)
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_isdeleted] DEFAULT ((0)) FOR [isdeleted]
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_DataLan] DEFAULT (getdate()) FOR [DataLan]
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_data] DEFAULT (getdate()) FOR [data]
GO
ALTER TABLE dbo.[T_MOVEST] ADD CONSTRAINT [DF_t_movest_datadoc] DEFAULT (getdate()) FOR [datadoc]
GO
CREATE NONCLUSTERED INDEX [idx_cditem] ON dbo.[T_MOVEST] ([cditem] ASC, [empitem] ASC)
GO
CREATE NONCLUSTERED INDEX [idx_data] ON dbo.[T_MOVEST] ([data] ASC)
GO
CREATE NONCLUSTERED INDEX [idx_numdoc] ON dbo.[T_MOVEST] ([numdoc] ASC, [empven] ASC)
GO
CREATE TRIGGER trg_t_movest_AtualizaSaldo 
      ON t_movest 
      AFTER INSERT 
      AS 
      BEGIN 
        SET NOCOUNT ON; 
  
 /* -------------------------------------------------------------- 
 -- 1) Captura do movimento inserido 
 -------------------------------------------------------------- */ 
  
        DECLARE @Mov TABLE ( 
          nrlan    INT, 
          cdemp    INT, 
          cditem   INT, 
          qtde     DECIMAL(18,4), 
          st       VARCHAR(1), 
          empitem  INT, 
          preco    DECIMAL(18,4), 
          data     DATETIME, 
          datadoc  DATETIME, 
          codusu   VARCHAR(10), 
          obs      VARCHAR(200), 
          obsit    VARCHAR(200), 
          clifor   INT, 
          especie  VARCHAR(1), 
          numdoc   VARCHAR(10), 
          empven   INT, 
          empmov   INT, 
          empfor   INT, 
          ip       VARCHAR(30) 
        ); 
  
        INSERT INTO @Mov 
        SELECT 
          nrlan, cdemp, cditem, qtde, st, empitem, preco, data, datadoc, 
          codusu, obs, obsit, clifor, especie, numdoc, empven, empmov, 
          empfor, ip 
        FROM inserted; 
  
 /*-------------------------------------------------------------- 
 -- 2) Garante existência do item na tabela de saldo 
 -------------------------------------------------------------- */ 
  
        INSERT INTO dbo.t_saldoit (cdemp, cditem, saldo, empitem) 
        SELECT DISTINCT 
          m.cdemp, 
          m.cditem, 
          0, 
          ISNULL(m.empitem, 1) 
        FROM @Mov m 
        WHERE NOT EXISTS ( 
          SELECT 1 
          FROM dbo.t_saldoit s 
          WHERE s.cdemp   = m.cdemp 
            AND s.cditem  = m.cditem 
            AND s.empitem = ISNULL(m.empitem, 1) 
        ); 
  
 /* -------------------------------------------------------------- 
 -- 3) Atualiza SALDOANT e SLDANTEMP na t_movest 
 -------------------------------------------------------------- */ 
  
        UPDATE m 
        SET m.saldoant = ( 
              SELECT ISNULL(SUM(saldo), 0) 
              FROM dbo.t_saldoit 
              WHERE cditem  = m.cditem 
                AND empitem = m.empitem 
            ), 
            m.sldantemp = ( 
              SELECT ISNULL(saldo, 0) 
              FROM dbo.t_saldoit 
              WHERE cditem  = m.cditem 
                AND empitem = m.empitem 
                AND cdemp   = m.cdemp 
            ) 
        FROM dbo.t_movest m 
        INNER JOIN @Mov i ON m.nrlan = i.nrlan; 
  
 /* -------------------------------------------------------------- 
 -- 4) Atualiza o saldo conforme tipo de movimento 
 -------------------------------------------------------------- */ 
  
        /* -- Entrada (E): soma saldo */ 
        UPDATE s 
        SET saldo = saldo + m.qtde 
        FROM dbo.t_saldoit s 
        INNER JOIN @Mov m ON 
          s.cdemp   = m.cdemp AND 
          s.cditem  = m.cditem AND 
          s.empitem = m.empitem 
        WHERE m.st = 'E'; 
  
        /* -- Saída (S): subtrai saldo */ 
        UPDATE s 
        SET saldo = saldo - m.qtde 
        FROM dbo.t_saldoit s 
        INNER JOIN @Mov m ON 
          s.cdemp   = m.cdemp AND 
          s.cditem  = m.cditem AND 
          s.empitem = m.empitem 
        WHERE m.st = 'S'; 
  
 /* -------------------------------------------------------------- 
 -- 5) Auditoria 
 -------------------------------------------------------------- */ 
  
        INSERT INTO t_auditoriaEstoque ( 
          data, usuario, cdemp, cditem, qtde, tipo, 
          saldo_ant, saldo_atual, origem 
        ) 
        SELECT 
          GETDATE(), 
          m.codusu, 
          m.cdemp, 
          m.cditem, 
          m.qtde, 
          m.st, 
          m.saldoant, 
          m.sldantemp, 
          'T_MOVEST' 
        FROM dbo.t_movest m 
        INNER JOIN @Mov i ON m.nrlan = i.nrlan; 
      END;
GO
COMMIT TRANSACTION;
GO
