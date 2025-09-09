[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_empresa_bronze_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_empresa_bronze = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
    #"Filtro pasta work" = Table.SelectRows(Fonte, each ([Folder Path] = "https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos Compartilhados/04 - ALLOS/work/empresa/")),
    #"Selecionar arquivo" = #"Filtro pasta work"{[Name="tb_empresa.xlsx",#"Folder Path"="https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos Compartilhados/04 - ALLOS/work/empresa/"]}[Content],
    #"Importar arquivo" = Excel.Workbook(#"Selecionar arquivo"),
    #"Selecionar pasta excel" = #"Importar arquivo"{[Item="Planilha1",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(#"Selecionar pasta excel", [PromoteAllScalars=true]),
  #"Filtered rows" = Table.SelectRows(#"Cabeçalhos Promovidos", each ([CNPJ] <> null)),
  #"Tipo de coluna alterado" = Table.TransformColumnTypes(#"Filtered rows", {{"CPF diretor", type text}})
in
    #"Tipo de coluna alterado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_grade_bronze_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_grade_bronze = let
    Fonte = Excel.Workbook(Web.Contents("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos%20Compartilhados/grades.xlsx"), null, true),
    Grade1 = Fonte{[Name="Grade"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(Grade1, [PromoteAllScalars=true]),
    #"Linhas Filtradas" = Table.SelectRows(#"Cabeçalhos Promovidos", each ([Posição] <> "-")),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Linhas Filtradas",{{"0,8", "80%"}, {"0,9", "90%"}, {"1", "100%"}, {"1,1000000000000001", "110%"}, {"1,2", "120%"}}),
    #"Outras Colunas Removidas" = Table.SelectColumns(#"Colunas Renomeadas",{"Grade Salarial", "Posição", "80%", "90%", "100%", "110%", "120%"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Outras Colunas Removidas", {{"Grade Salarial", Int64.Type}, {"Posição", type text}, {"80%", type number}, {"90%", type number}, {"100%", Int64.Type}, {"110%", type number}, {"120%", type number}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_colaboradores_bronze_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_colaboradores_bronze = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Fonte, each [Folder Path] = "https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos Compartilhados/03 - PROPAY/work/colaboradores/"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", let latest = List.Max(#"Filtered rows"[Date created]) in each [Date created] = latest),
  #"Filtrando a planilha" = #"Filtered rows 1"[Content]{0},
  #"Abrindo o csv" = Csv.Document(#"Filtrando a planilha",[Delimiter=";", Columns=71, Encoding=1252, QuoteStyle=QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Abrindo o csv", [PromoteAllScalars = true])
in
    #"Promoted headers";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_success_bronze_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_success_bronze = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
    #"Filtro pasta work" = Table.SelectRows(Fonte, each ([Folder Path] = "https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos Compartilhados/01 - SUCCESS/work/colaboradores/")),
    #"Filtro arquivo mais recente" = Table.SelectRows(#"Filtro pasta work", let latest = List.Max(#"Filtro pasta work"[Date created]) in each [Date created] = latest),
    #"Selecionar arquivo" = #"Filtro arquivo mais recente"{0}[Content],
    #"Importar arquivo" = Excel.Workbook(#"Selecionar arquivo"),
    #"Selecionar pasta excel" = #"Importar arquivo"{[Item="tb_colaboradores",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(#"Selecionar pasta excel", [PromoteAllScalars=true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos", {{"Data da Admissão", type text}, {"Data de nascimento", type text}, {"Detalhes do emprego Data do Desligamento", type text}, {"Subordinados diretos", type text}, {"Detalhes do emprego Data da admissão", type text}}, "pt-BR"),
  #"Colunas renomeadas" = Table.RenameColumns(#"Changed column type", {{"Brazil Cadastro de Pessoas Físicas (CPF)  Documento de identificação nacional (CPF)", "CPF"}})

in
    #"Colunas renomeadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_movimentacoes_bronze_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "EMP", DestinationColumnName = "EMP"], [SourceColumnName = "RAZÃO SOCIAL", DestinationColumnName = "RAZÃO SOCIAL"], [SourceColumnName = "MATRICULA", DestinationColumnName = "MATRICULA"], [SourceColumnName = "NOME", DestinationColumnName = "NOME"], [SourceColumnName = "CPF", DestinationColumnName = "CPF"], [SourceColumnName = "ADMISSÃO", DestinationColumnName = "ADMISSÃO"], [SourceColumnName = "CARGO ATUAL", DestinationColumnName = "CARGO ATUAL"], [SourceColumnName = "DATA ALTERAÇÃO", DestinationColumnName = "DATA ALTERAÇÃO"], [SourceColumnName = "CARGO NA DATA DA OCORRÊNCIA", DestinationColumnName = "CARGO NA DATA DA OCORRÊNCIA"], [SourceColumnName = "SALARIAL ANTERIOR", DestinationColumnName = "SALARIAL ANTERIOR"], [SourceColumnName = "SALARIAL ATUAL", DestinationColumnName = "SALARIAL ATUAL"], [SourceColumnName = "VARIAÇÃO %", DestinationColumnName = "VARIAÇÃO %"], [SourceColumnName = "NOME_MOTIVO", DestinationColumnName = "NOME_MOTIVO"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared tb_movimentacoes_bronze = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
    #"Filtro pasta work" = Table.SelectRows(Fonte, each Text.Contains([Folder Path], "https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos Compartilhados/03 - PROPAY/work/movimentacao_salarial/")),
    #"Filtro arquivo mais recente" = Table.SelectRows(#"Filtro pasta work", let latest = List.Max(#"Filtro pasta work"[Date created]) in each [Date created] = latest),
    #"Selecionar arquivo" = #"Filtro arquivo mais recente"{0}[Content],
    #"Importar CSV" = Csv.Document(#"Selecionar arquivo",[Delimiter=";", Columns=14, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(#"Importar CSV", [PromoteAllScalars=true])
in
    #"Cabeçalhos Promovidos";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_vagas_bronze_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_vagas_bronze = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
    #"Filtro pasta work" = Table.SelectRows(Fonte, each ([Folder Path] = "https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos Compartilhados/01 - SUCCESS/work/vagas/")),
    #"Linhas Filtradas" = Table.SelectRows(#"Filtro pasta work", let latest = List.Max(#"Filtro pasta work"[Date created]) in each [Date created] = latest),
    #"Abrindo arquivo" = Excel.Workbook(#"Linhas Filtradas"{0}[Content]),
    tb_vagas_Sheet = #"Abrindo arquivo"{[Item="tb_vagas",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(tb_vagas_Sheet, [PromoteAllScalars=true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos", {{"ID da req. de vaga", type text}, {"Data de abertura", type text}, {"Data de início da publicação", type text}, {"Última modificação", type text}, {"Data fechada", type text}, {"Número de vagas", type text}, {"Salário Final", type text}, {"Tempo para preenchimento da vaga", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_vagas_gupy_bronze_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_vagas_gupy_bronze = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
    #"Filtro pasta work" = Table.SelectRows(Fonte, each ([Folder Path] = "https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos Compartilhados/05 - GUPY/work/")),
    #"Linhas Filtradas" = Table.SelectRows(#"Filtro pasta work", let latest = List.Max(#"Filtro pasta work"[Date created]) in each [Date created] = latest),
    #"Abrindo arquivo" = Excel.Workbook(#"Linhas Filtradas"{0}[Content]),
    tb_vagas_Sheet = #"Abrindo arquivo"{[Item="Sheet1",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(tb_vagas_Sheet, [PromoteAllScalars=true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos", {{"Código da vaga", type text}, {"Posições", type text}, {"Posições Abertas", type text}, {"Posições Fechadas", type text}, {"Tempo Congelada", type text}, {"Número de Inscritos", type text}, {"Número de Contratados", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_imagens_perfil_bronze_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_imagens_perfil_bronze = let
    Fonte = Excel.Workbook(Web.Contents("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos%20Compartilhados/imagens_perfil.xlsx"), null, true),
    Planilha1_Sheet = Fonte{[Item="Planilha1",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(Planilha1_Sheet, [PromoteAllScalars=true]),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos",{{"Nome Completo", type text}, {"Gestor", type text}, {"CPF", Int64.Type}, {"Usuário", type text}, {"Link Google Drive", type text}, {"Link adaptação Google Drive", type text}}),
    #"Outras Colunas Removidas" = Table.SelectColumns(#"Tipo Alterado",{"Nome Completo", "Gestor", "CPF", "Usuário", "Link Google Drive", "Link adaptação Google Drive", "Link LinkedIn"}),
    #"Linhas Filtradas" = Table.SelectRows(#"Outras Colunas Removidas", each ([Link Google Drive] <> null)),
    #"Texto Inserido Entre os Delimitadores" = Table.AddColumn(#"Linhas Filtradas", "Codigo imagem", each Text.BetweenDelimiters([Link Google Drive], "/", "/", 4, 0), type text),
    #"Coluna Mesclada Inserida" = Table.AddColumn(#"Texto Inserido Entre os Delimitadores", "Link imagem", each Text.Combine({"https://drive.google.com/thumbnail?id=", [Codigo imagem], "&sz=w1000"}), type text),
    #"Link LinkedIn" = #"Linhas Filtradas",
    #"Colunas Removidas" = Table.RemoveColumns(#"Link LinkedIn",{"Link Google Drive", "Link adaptação Google Drive"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Colunas Removidas",{{"Link LinkedIn", "Link imagem"}}),
    #"Buscando pelo link Google Drive" = #"Coluna Mesclada Inserida",
  #"Changed column type" = Table.TransformColumnTypes(#"Buscando pelo link Google Drive", {{"CPF", type text}, {"Link LinkedIn", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_diretorias_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_diretorias = let
  Source = Excel.Workbook(Web.Contents("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos%20Compartilhados/de-para_diretorias.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "Planilha1", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"NOME", type text}, {"CPF",type text}, {"DIRETORIA", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_colaboradores_helloo_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_colaboradores_helloo = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
    #"Filtro pasta work" = Table.SelectRows(Fonte, each Text.Contains([Folder Path], "HELLOO")),
    #"Filtro arquivo mais recente" = Table.SelectRows(#"Filtro pasta work", let latest = List.Max(#"Filtro pasta work"[Date created]) in each [Date created] = latest),
    #"Inserir arquivo" = Excel.Workbook(#"Filtro arquivo mais recente"{0}[Content]),
    #"Linhas Filtradas" = let
lista = Table.SelectRows(#"Inserir arquivo", each ([Name] = "BASE "))[Data],
tabela = List.First(lista) in tabela,
    #"Linhas Superiores Removidas" = Table.Skip(#"Linhas Filtradas",2),
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(#"Linhas Superiores Removidas", [PromoteAllScalars=true]),
  #"Tipo de coluna alterado" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos", {{"SITUAÇÃO", type text}, {"MATRICULA", type text}, {"FOLHA", type text}, {"PORTFÓLIO", type text}, {"EMPRESA", type text}, {"DIRETORIA", type text}, {"CPF", type text}, {"NOME", type text}, {"CARGO", type text}, {"GRUPO DE CARGO", type text}, {"TIPO DE CONTRATO", type text}, {"GERÊNCIA", type text}, {"NÍVEL", type text}, {"CAMPO", type text}, {"SHOPPING", type text}, {"REGIONAL", type text}, {"UF", type text}, {"SALARIO", type text}, {"% FAIXA", type text}, {"100% faixa", type text}, {"GRADE", type text}, {"DATA DA ÚLTIMA MOVIMENTAÇÃO", type text}, {"LIDERANÇA", type text}, {"ADMISSAO", type text}, {"FIM EXPERIÊNCIA", type text}, {"TEMPO#(lf)CIA", type text}, {"NASCIMENTO", type text}, {"DATA ATUAL", type text}, {"IDADE", type text}, {"GERAÇÃO", type text}, {"COD CCUSTO", type text}, {"CENTRO DE CUSTO", type text}, {"GESTOR FOLHA", type text}, {"GESTOR PORTAL", type text}, {"GESTOR DIRETO", type text}, {"SEXO", type text}, {"RAÇA", type text}, {"GRAU DE INSTRUÇÃO", type text}, {"ENDEREÇO COMPLETO", type text}, {"CIDADE", type text}, {"EMAIL", type text}, {"DATA DE DESLIGAMENTO", type text}, {"TIPO DE DESLIGAMENTO", type text}})
in
    #"Tipo de coluna alterado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_de-para_areas_shopping_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"tb_de-para_areas_shopping" = let
    Fonte = Excel.Workbook(Web.Contents("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos%20Compartilhados/de-para_areas_shopping.xlsx"), null, true),
    Planilha1_Sheet = Fonte{[Item="Planilha1",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(Planilha1_Sheet, [PromoteAllScalars=true]),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos",{{"Área Mereo SHOPPING", type text}, {"Subárea", type text}}),
    #"Texto em Maiúscula" = Table.TransformColumns(#"Tipo Alterado",{{"Subárea", Text.Upper, type text}, {"Área Mereo SHOPPING", Text.Upper, type text}}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Texto em Maiúscula",{{"Subárea", "CARGO SHOPPING"}, {"Área Mereo SHOPPING", "ÁREA SHOPPING"}})
in
    #"Colunas Renomeadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_shoppings_administrados_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_shoppings_administrados = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
    Planilha1_Sheet = Table.SelectRows(Fonte, each Text.Contains([Folder Path], "SHOPPINGS ADMINISTRADOS")),
    #"Outras Colunas Removidas" = Table.SelectColumns(Planilha1_Sheet,{"Content"}),
    Personalizar1 = Table.AddColumn(#"Outras Colunas Removidas", "Tabelas", each 
let 
passo1 = Excel.Workbook([Content], null, true),
passo2 = passo1{[Item="Planilha1",Kind="Sheet"]}[Data],
passo3 = Table.PromoteHeaders(passo2, [PromoteAllScalars=true])
in passo3),
    #"Colunas Removidas" = Table.RemoveColumns(Personalizar1,{"Content"}),
    #"Tabelas Expandido" = Table.ExpandTableColumn(#"Colunas Removidas", "Tabelas", {"Shopping", "Unidade", "CPF", "Nome", "Email", "Cargo", "Data de admissão", "Líder", "CPF Líder", "Portfólio", "Data desligamento", "Shopping Origem/Destino"}, {"Shopping", "Unidade", "CPF", "Nome", "Email", "Cargo", "Data de admissão", "Líder", "CPF Líder", "Portfólio", "Data desligamento", "Shopping Origem/Destino"}),
    #"Linhas Filtradas" = Table.SelectRows(#"Tabelas Expandido", each ([Shopping] <> null)),
    #"Texto em Maiúscula" = Table.TransformColumns(#"Linhas Filtradas",{{"Shopping", Text.Upper, type text}, {"Nome", Text.Upper, type text}, {"Cargo", Text.Upper, type text}, {"Líder", Text.Upper, type text}, {"Shopping Origem/Destino", Text.Upper, type text}}),
    #"Colocar Cada Palavra Em Maiúscula" = Table.TransformColumns(#"Texto em Maiúscula",{{"Unidade", Text.Proper, type text}, {"Portfólio", Text.Proper, type text}}),
    #"Texto em Minúscula" = Table.TransformColumns(#"Colocar Cada Palavra Em Maiúscula",{{"Email", Text.Lower, type text}}),
    #"Valor Substituído" = Table.ReplaceValue(#"Texto em Minúscula","","Shopping",Replacer.ReplaceValue,{"Unidade"}),
    #"Valor Substituído1" = Table.ReplaceValue(#"Valor Substituído",null,"Shopping",Replacer.ReplaceValue,{"Unidade"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Valor Substituído1",{{"Data de admissão", "ADMISSAO"}, {"Líder", "GESTOR"}, {"CPF Líder", "CPF GESTOR"}, {"Data desligamento", "DEMISSAO"}, {"Shopping", "SHOPPING"}, {"Nome", "NOME"}, {"Email", "EMAIL"}, {"Cargo", "CARGO"}, {"Portfólio", "PORTFÓLIO"}, {"Unidade", "UNIDADE"}}),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Colunas Renomeadas",{{"CPF GESTOR", type text}, {"CPF", type text}}),
    #"Valor Substituído2" = Table.ReplaceValue(#"Tipo Alterado","-","",Replacer.ReplaceText,{"CPF GESTOR"}),
    #"Valor Substituído3" = Table.ReplaceValue(#"Valor Substituído2",".","",Replacer.ReplaceText,{"CPF GESTOR"}),
    #"Valor Substituído4" = Table.ReplaceValue(#"Valor Substituído3",".","",Replacer.ReplaceText,{"CPF"}),
    #"Valor Substituído5" = Table.ReplaceValue(#"Valor Substituído4","-","",Replacer.ReplaceText,{"CPF"}),
  #"Ajustando coluna CPF" = Table.TransformColumns(#"Valor Substituído5", {{"CPF", each if _ <> "" or _ <> null then Text.PadStart(Text.From(_), 11, "0") else null}}),
  #"Ajustando coluna CPF_GESTOR" = Table.TransformColumns(#"Ajustando coluna CPF", {{"CPF GESTOR", each if _ <> "" or _ <> null then Text.PadStart(Text.From(_), 11, "0") else null}}),
    #"Coluna 'É SHOPPING ADM' inserida" = Table.AddColumn(#"Ajustando coluna CPF_GESTOR", "É_SHOPPING_ADM", each "S"),
    #"Coluna 'CNPJ' inserida" = Table.AddColumn(#"Coluna 'É SHOPPING ADM' inserida", "CNPJ", each "00.000.000/0000-00"),
    #"Coluna 'EMPRESA' inserida" = Table.AddColumn(#"Coluna 'CNPJ' inserida", "EMPRESA", each [SHOPPING], type text),
  #"Coluna 'DIRETORIA' inserida" = Table.TransformColumnTypes(Table.AddColumn(#"Coluna 'EMPRESA' inserida", "DIRETORIA", each "DIRETORIA DE OPERAÇÕES"), {{"DIRETORIA", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Coluna 'DIRETORIA' inserida", {{"CPF GESTOR", "CPF_GESTOR"}, {"Shopping Origem/Destino", "Shopping_Origem/Destino"}}),
    #"Tipo Alterado1" = Table.TransformColumnTypes(#"Renamed columns", {{"SHOPPING", type text}, {"UNIDADE", type text}, {"CPF", type text}, {"NOME", type text}, {"EMAIL", type text}, {"CARGO", type text}, {"ADMISSAO", type date}, {"GESTOR", type text}, {"CPF_GESTOR", type text}, {"PORTFÓLIO", type text}, {"DEMISSAO", type date}, {"Shopping_Origem/Destino", type text}, {"É_SHOPPING_ADM", type text}, {"CNPJ", type text}})
in
    #"Tipo Alterado1";
shared tb_empresa_bronze_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_empresa_bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_grade_bronze_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_grade_bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_colaboradores_bronze_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_colaboradores_bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_success_bronze_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_success_bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_vagas_bronze_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_vagas_bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_vagas_gupy_bronze_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_vagas_gupy_bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_imagens_perfil_bronze_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_imagens_perfil_bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_diretorias_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_diretorias", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_shoppings_administrados_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_shoppings_administrados", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"tb_de-para_areas_shopping_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_de_para_areas_shopping", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_movimentacoes_bronze_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_mov_bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_autodeclaracao_racial_forms = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
    #"Filtro pasta work" = Table.SelectRows(Fonte, each ([Folder Path] = "https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos Compartilhados/01 - SUCCESS/work/autodeclaracao_racial_forms/")),
    #"Filtro arquivo mais recente" = Table.SelectRows(#"Filtro pasta work", let latest = List.Max(#"Filtro pasta work"[Date created]) in each [Date created] = latest),
    #"Selecionar arquivo" = #"Filtro arquivo mais recente"{0}[Content],
    #"Importar arquivo" = Excel.Workbook(#"Selecionar arquivo"),
    #"Selecionar pasta excel" = #"Importar arquivo"{[Item="Sheet1",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(#"Selecionar pasta excel", [PromoteAllScalars=true]),
    #"Linhas Filtradas" = Table.SelectRows(#"Cabeçalhos Promovidos", each ([CPF] <> null)),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Linhas Filtradas",{{"Completion time", type datetime}}),
    #"Valor Substituído2" = Table.TransformColumns(#"Tipo Alterado", {{"CPF", each Text.Select(_, {"0".."9"}), type text}}),
    #"Linhas Classificadas" = Table.Sort(#"Valor Substituído2",{{"CPF", Order.Ascending}, {"Completion time", Order.Descending}}),
    #"Tabela em buffer" = Table.Buffer(#"Linhas Classificadas"),
    #"Duplicatas Removidas" = Table.Distinct(#"Tabela em buffer", {"CPF"}),
    #"Outras Colunas Removidas" = Table.SelectColumns(#"Duplicatas Removidas",{"CPF", "Você já se sentiu desconfortável em algum lugar por conta da cor da sua pele?", "Você já ouviu piadas por conta da cor da sua pele ou do seu de cabelo?       ", "Você já desejou ter outra cor de pele?         ", "Como você se autodeclara? Os parâmetros estabelecidos para a autodeclaração, são os mesmos utilizados pelo IBGE (Instituto Brasileiro de Geografia e Estatística)"}),
    #"Texto Extraído Antes do Delimitador" = Table.TransformColumns(#"Outras Colunas Removidas", {{"Como você se autodeclara? Os parâmetros estabelecidos para a autodeclaração, são os mesmos utilizados pelo IBGE (Instituto Brasileiro de Geografia e Estatística)", each Text.BeforeDelimiter(_, " ("), type text}}),
    #"Colocar Cada Palavra Em Maiúscula" = Table.TransformColumns(#"Texto Extraído Antes do Delimitador",{{"Como você se autodeclara? Os parâmetros estabelecidos para a autodeclaração, são os mesmos utilizados pelo IBGE (Instituto Brasileiro de Geografia e Estatística)", Text.Proper, type text}}),
    #"Texto Extraído Antes do Delimitador1" = Table.TransformColumns(#"Colocar Cada Palavra Em Maiúscula", {{"Como você se autodeclara? Os parâmetros estabelecidos para a autodeclaração, são os mesmos utilizados pelo IBGE (Instituto Brasileiro de Geografia e Estatística)", each Text.BeforeDelimiter(_, " -"), type text}}),
    #"Valor Substituído" = Table.ReplaceValue(#"Texto Extraído Antes do Delimitador1","o","a",Replacer.ReplaceText,{"Como você se autodeclara? Os parâmetros estabelecidos para a autodeclaração, são os mesmos utilizados pelo IBGE (Instituto Brasileiro de Geografia e Estatística)"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Valor Substituído",{{"Você já se sentiu desconfortável em algum lugar por conta da cor da sua pele?", "Você já se sentiu desconfortável em algum lugar por causa da sua cor?"}, {"Você já ouviu piadas por conta da cor da sua pele ou do seu de cabelo?       ", "Você já ouviu piadas por conta da cor da sua pele ou do seu tipo de cabelo?"}, {"Você já desejou ter outra cor de pele?         ", "Você já desejou ter outra cor de pele?"}, {"Como você se autodeclara? Os parâmetros estabelecidos para a autodeclaração, são os mesmos utilizados pelo IBGE (Instituto Brasileiro de Geografia e Estatística)", "Com qual cor ou raça você se identifica?"}})
in
    #"Colunas Renomeadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_autodeclaracao_racial_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_autodeclaracao_racial = let
    Fonte = SharePoint.Files("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB", [ApiVersion = 15]),
    #"Filtro pasta work" = Table.SelectRows(Fonte, each ([Folder Path] = "https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO/G&P-DB/Documentos Compartilhados/01 - SUCCESS/work/autodeclaracao_racial/")),
    #"Filtro arquivo mais recente" = Table.SelectRows(#"Filtro pasta work", let latest = List.Max(#"Filtro pasta work"[Date created]) in each [Date created] = latest),
    #"Selecionar arquivo" = #"Filtro arquivo mais recente"{0}[Content],
    #"Importar arquivo" = Excel.Workbook(#"Selecionar arquivo"),
    #"Selecionar pasta excel" = #"Importar arquivo"{[Item="Saída para Excel",Kind="Sheet"]}[Data],
    #"Linhas Superiores Removidas" = Table.Skip(#"Selecionar pasta excel",2),
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(#"Linhas Superiores Removidas", [PromoteAllScalars=true]),
    #"Linhas Filtradas" = Table.SelectRows(#"Cabeçalhos Promovidos", each not Text.Contains([ID do sistema do usuário], "Admin_Luan")),
    #"Valor Substituído" = Table.TransformColumns(#"Linhas Filtradas", {{"CPF", each Text.Select(_, {"0".."9"}), type text}}),
    #"Linhas Filtradas1" = Table.SelectRows(#"Valor Substituído", each ([CPF] <> "")),
    #"Outras Colunas Removidas" = Table.SelectColumns(#"Linhas Filtradas1",{"CPF", "Você já se sentiu desconfortável em algum lugar por causa da sua cor?", "Você já ouviu piadas por conta da cor da sua pele ou do seu tipo de cabelo?", "Você já desejou ter outra cor de pele?", "Com qual cor ou raça você se identifica?"}),
    #"(Etapa desativada) Unindo com tabela do forms" = Table.Combine({#"Outras Colunas Removidas", tb_autodeclaracao_racial_forms}),
    #"Duplicatas Removidas" = Table.Distinct(#"Outras Colunas Removidas", {"CPF"}),
  #"Colunas renomeadas" = Table.RenameColumns(#"Duplicatas Removidas", {{"Você já se sentiu desconfortável em algum lugar por causa da sua cor?", "Você_já_se_sentiu_desconfortável_em_algum_lugar_por_causa_da_sua_cor?"}, {"Você já ouviu piadas por conta da cor da sua pele ou do seu tipo de cabelo?", "Você_já_ouviu_piadas_por_conta_da_cor_da_sua_pele_ou_do_seu_tipo_de_cabelo?"}, {"Você já desejou ter outra cor de pele?", "Você_já_desejou_ter_outra_cor_de_pele?"}, {"Com qual cor ou raça você se identifica?", "Com_qual_cor_ou_raça_você_se_identifica?"}})
in
    #"Colunas renomeadas";
shared tb_autodeclaracao_racial_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_autodeclaracao_racial", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_colaboradores_helloo_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_colaboradores_helloo", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
