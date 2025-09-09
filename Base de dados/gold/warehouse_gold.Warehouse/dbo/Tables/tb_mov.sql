CREATE TABLE [dbo].[tb_mov] (

	[Chave] varchar(8000) NULL, 
	[Chave_Empresa] varchar(8000) NULL, 
	[CPF] varchar(8000) NULL, 
	[DataAlteracao] datetime2(6) NULL, 
	[Cargo] varchar(8000) NULL, 
	[Nome] varchar(8000) NULL, 
	[DataAdmissao] datetime2(6) NULL, 
	[Salário] float NULL, 
	[Motivo] varchar(8000) NULL, 
	[Variacao] float NULL, 
	[MES_ANO_ALTERACAO] varchar(8) NULL
);