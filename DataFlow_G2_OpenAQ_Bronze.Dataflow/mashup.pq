[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NYC_Taxi_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared NYC_Taxi = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "95ebcbcf-35b4-4eda-bae3-344163c79c6c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "aa1ee62d-2f24-4c14-b409-b89563d3e7fa"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Bronze"]}[Content],
  #"Expanded Content" = Table.ExpandTableColumn(#"Navigation 3", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes.1", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each not Text.Contains([Name.1], "openaq")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (2)", each #"Transform file (2)"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (2)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (2)", Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"VendorID", Int64.Type}, {"tpep_pickup_datetime", type datetime}, {"tpep_dropoff_datetime", type datetime}, {"passenger_count", Int64.Type}, {"trip_distance", type number}, {"RatecodeID", Int64.Type}, {"store_and_fwd_flag", type text}, {"PULocationID", Int64.Type}, {"DOLocationID", Int64.Type}, {"payment_type", Int64.Type}, {"fare_amount", type number}, {"extra", type number}, {"mta_tax", type number}, {"tip_amount", type number}, {"tolls_amount", type number}, {"improvement_surcharge", Int64.Type}, {"total_amount", type number}, {"congestion_surcharge", type number}, {"Airport_fee", type number}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Airport_fee", "congestion_surcharge", "improvement_surcharge", "tolls_amount", "mta_tax", "extra", "store_and_fwd_flag", "PULocationID", "DOLocationID", "fare_amount"})
in
  #"Removed columns";
shared #"Sample file (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "95ebcbcf-35b4-4eda-bae3-344163c79c6c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "aa1ee62d-2f24-4c14-b409-b89563d3e7fa"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Bronze"]}[Content],
  #"Expanded Content" = Table.ExpandTableColumn(#"Navigation 3", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes.1", "Folder Path.1"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Content", each not Text.Contains([Name.1], "openaq")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 4" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 4";
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Parquet.Document(#"Parameter (2)")
in
  Source;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Parquet.Document(#"Parameter (2)")
in
  Source;
shared NYC_Taxi_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "95ebcbcf-35b4-4eda-bae3-344163c79c6c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "aa1ee62d-2f24-4c14-b409-b89563d3e7fa"]}[Data],
  TableNavigation = Navigation_2{[Id = "Silver_NYC_Taxi", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Silver_OpenAQ_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Silver_OpenAQ = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "95ebcbcf-35b4-4eda-bae3-344163c79c6c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "aa1ee62d-2f24-4c14-b409-b89563d3e7fa"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Bronze"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "OpenAQ"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", each [Name] = "openaq_measurements.parquet"),
  #"Navigation 5" = #"Filtered rows"{[Name = "openaq_measurements.parquet"]}[Content],
  #"Imported Parquet" = Parquet.Document(#"Navigation 5"),
  #"Removed columns" = Table.RemoveColumns(#"Imported Parquet", {"coordinates", "flagInfo.hasFlags", "summary", "parameter.displayName", "period.label", "period.interval", "coverage.datetimeFrom.local", "coverage.datetimeTo.utc", "coverage.datetimeTo.local", "coverage.datetimeFrom.utc", "coverage.percentCoverage", "coverage.percentComplete", "coverage.observedInterval", "coverage.observedCount", "coverage.expectedInterval", "coverage.expectedCount", "period.datetimeTo.utc", "period.datetimeTo.local", "period.datetimeFrom.local", "period.datetimeFrom.utc"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"value", "measurement_value"}, {"parameter.name", "pollutant"}, {"parameter.units", "units"}})
in
  #"Renamed columns";
shared Silver_OpenAQ_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "95ebcbcf-35b4-4eda-bae3-344163c79c6c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "aa1ee62d-2f24-4c14-b409-b89563d3e7fa"]}[Data],
  TableNavigation = Navigation_2{[Id = "Silver_OpenAQ", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
