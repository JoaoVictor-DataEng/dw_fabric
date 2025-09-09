[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_estrutura_newco_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_estrutura_newco = let
    Fonte = SharePoint.Tables("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO", [Implementation=null, ApiVersion=15]),
    #"53e0e94d-88e9-4463-964a-cc6c5678ad27" = Fonte{[Id="53e0e94d-88e9-4463-964a-cc6c5678ad27"]}[Items],
    #"Colunas Removidas" = Table.RemoveColumns(#"53e0e94d-88e9-4463-964a-cc6c5678ad27",{"FileSystemObjectType", "Id", "ServerRedirectedEmbedUri", "ServerRedirectedEmbedUrl", "ContentTypeId"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Colunas Removidas",{{"Title", "Localidade"}}),
    #"Colunas Removidas1" = Table.RemoveColumns(#"Colunas Renomeadas",{"OData__ColorTag", "ComplianceAssetId", "nome_origem", "ID", "Modified", "Created", "AuthorId", "EditorId", "OData__UIVersionString", "Attachments", "GUID", "FirstUniqueAncestorSecurableObject", "RoleAssignments", "AttachmentFiles", "ContentType", "GetDlpPolicyTip", "FieldValuesAsHtml", "FieldValuesAsText", "FieldValuesForEdit", "File", "Folder", "LikedByInformation", "ParentList", "Properties", "Versions", "Author", "Editor"}),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Colunas Removidas1", {{"cluster", Int64.Type}, {"Localidade", type text}, {"regional", type text}, {"portfolio", type text}, {"vertical", type text}, {"legado", type text}})
in
    #"Tipo Alterado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_midia_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_midia = let
    Fonte = SharePoint.Tables("https://brmallsadm.sharepoint.com/sites/BRM-REMUNERAO", [Implementation=null, ApiVersion=15]),
    #"5a81f412-492c-483a-a995-aa959364d789" = Fonte{[Id="5a81f412-492c-483a-a995-aa959364d789"]}[Items],
    #"Colunas Renomeadas1" = Table.RenameColumns(#"5a81f412-492c-483a-a995-aa959364d789",{{"ID", "ID.1"}}),
    #"Colunas Removidas" = Table.RemoveColumns(#"Colunas Renomeadas1",{"FileSystemObjectType", "Id", "ServerRedirectedEmbedUri", "ServerRedirectedEmbedUrl", "ContentTypeId", "OData__ColorTag", "ComplianceAssetId"}),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Colunas Removidas",{{"data_abertura", type date}, {"data_admissao", type date}, {"status_atracao", type text}, {"origem", type text}, {"funcao", type text}, {"status_vaga", type text}, {"salario", Currency.Type}, {"midia", type text}, {"observacoes", type text}}),
    #"Colunas Removidas1" = Table.RemoveColumns(#"Tipo Alterado",{"ID.1", "Modified", "Created", "AuthorId", "EditorId", "OData__UIVersionString", "Attachments", "GUID", "FirstUniqueAncestorSecurableObject", "RoleAssignments", "AttachmentFiles", "ContentType", "GetDlpPolicyTip", "FieldValuesAsHtml", "FieldValuesAsText", "FieldValuesForEdit"}),
    #"localidade Expandido" = Table.ExpandRecordColumn(#"Colunas Removidas1", "localidade", {"Title"}, {"Title.1"}),
    #"Colunas Removidas2" = Table.RemoveColumns(#"localidade Expandido",{"File", "Folder", "LikedByInformation", "ParentList", "Properties", "Versions", "Author", "Editor"}),
    #"Colunas Reordenadas" = Table.ReorderColumns(#"Colunas Removidas2",{"Title", "localidadeId", "Title.1", "data_abertura", "data_admissao", "status_atracao", "Colaborador", "status_vaga", "funcao", "origem", "salario", "midia", "observacoes"}),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Colunas Reordenadas",{{"Title.1", "Localidade"}}),
    #"Colunas Removidas3" = Table.RemoveColumns(#"Colunas Renomeadas", {"localidadeId", "Recrutador(a)"}),
    #"Colunas Renomeadas2" = Table.RenameColumns(#"Colunas Removidas3",{{"Title", "Codigo vaga"}, {"data_abertura", "Data abertura"}, {"data_admissao", "Data admissão"}, {"status_atracao", "Status processo"}, {"status_vaga", "Status vaga"}, {"funcao", "Função"}, {"salario", "Salário"}, {"origem", "Tipo de recrutamento"}, {"Colaboradorsubstituído_x00", "Colaborador Substituído"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Colunas Renomeadas2", {{"Tipo de recrutamento", type text}, {"Salário", type text}, {"midia", type text}, {"legado", type text}, {"Colaborador Substituído", type text}, {"Recrutador(a)Id", type text}, {"Recrutador(a)StringId", type text}, {"Gestor", type text}, {"Codigo vaga", type text}, {"Localidade", type text}, {"Colaborador", type text}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tb_cc_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tb_cc = let
    Fonte = SharePoint.Tables("https://brmallsadm.sharepoint.com/sites/BRM-ATRAOEONBOARDING", [ApiVersion = 15]),
    #"b43f7812-c0f9-4f30-9311-96e1dd840bfd" = Fonte{[Id="b43f7812-c0f9-4f30-9311-96e1dd840bfd"]}[Items],
    #"Colunas Renomeadas" = Table.RenameColumns(#"b43f7812-c0f9-4f30-9311-96e1dd840bfd",{{"ID", "ID.1"}}),
    #"Colunas Removidas" = Table.RemoveColumns(#"Colunas Renomeadas",{"FileSystemObjectType", "ServerRedirectedEmbedUri", "ServerRedirectedEmbedUrl", "ID.1", "ContentTypeId", "Modified", "Created", "AuthorId", "EditorId", "OData__UIVersionString", "Attachments", "GUID", "ComplianceAssetId", "Executiva", "Adjunta", "OData_Área", "NomeCentrodeCusto", "aliansce+brmalls", "Subáreas", "Código", "OData__ColorTag", "FirstUniqueAncestorSecurableObject", "RoleAssignments", "AttachmentFiles", "ContentType", "GetDlpPolicyTip", "FieldValuesAsHtml", "FieldValuesAsText", "FieldValuesForEdit", "File", "Folder", "LikedByInformation", "ParentList", "Properties", "Versions", "Author", "Editor"}),
    #"Colunas Renomeadas1" = Table.RenameColumns(#"Colunas Removidas",{{"Title", "cc"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Colunas Renomeadas1", {{"Id", type text}, {"cc", type text}})
in
    #"Changed column type";
shared tb_estrutura_newco_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_estrutura_newco", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_midia_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_midia", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared tb_cc_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bba0219c-62af-4fa9-a4da-84977313ebc8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "27363035-663a-47b7-9d72-fb679ddb5b75"]}[Data],
  TableNavigation = Navigation_2{[Id = "tb_cc", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
