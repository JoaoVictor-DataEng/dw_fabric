[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
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
shared tb_vagas_gupy_bronze_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_vagas_gupy_bronze", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
