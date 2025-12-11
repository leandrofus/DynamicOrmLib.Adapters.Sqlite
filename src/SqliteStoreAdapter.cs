using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Microsoft.Data.Sqlite;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Linq;
using DynamicOrmLib;

namespace DynamicOrmLib.Adapters.Sqlite;

public class SqliteStoreAdapter : IStoreProvider
{
  private readonly string _connectionString;
  private SqliteConnection? _transactionConn;
  private SqliteTransaction? _transaction;
  private readonly string _tablePrefix;
  private readonly Dictionary<string, bool> _modelTableExists = new();

  public SqliteStoreAdapter(string connectionString, string tablePrefix = "records_")
  {
    _connectionString = connectionString;
    _tablePrefix = tablePrefix ?? string.Empty;
  }

  private SqliteConnection GetOrCreateConnection(out bool created)
  {
    if (_transactionConn != null) { created = false; return _transactionConn; }
    var conn = new SqliteConnection(_connectionString);
    conn.Open();
    created = true;
    return conn;
  }

  private void PrepareCommand(SqliteCommand cmd)
  {
    if (_transaction != null)
    {
      cmd.Transaction = _transaction;
    }
  }

  public void Init()
  {
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {

      // Always create managed schema and history tables
      using var cmd3 = conn.CreateCommand();
      PrepareCommand(cmd3);
      cmd3.CommandText = @"CREATE TABLE IF NOT EXISTS schema_manager (
          model_name TEXT PRIMARY KEY,
          module TEXT,
          metadata TEXT,
          applied_at TEXT
        );";
      cmd3.ExecuteNonQuery();
      using var cmd4 = conn.CreateCommand();
      PrepareCommand(cmd4);
      cmd4.CommandText = @"CREATE TABLE IF NOT EXISTS schema_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          model_name TEXT,
          change TEXT,
          module TEXT,
          operation TEXT,
          applied_at TEXT
        );";
      cmd4.ExecuteNonQuery();

      using var cmd5 = conn.CreateCommand();
      PrepareCommand(cmd5);
      cmd5.CommandText = @"CREATE TABLE IF NOT EXISTS schema_counters (
          model_name TEXT PRIMARY KEY,
          counter INTEGER
        );";
      cmd5.ExecuteNonQuery();


    }
    finally { if (connCreated) conn.Dispose(); }
  }

  public void BeginTransaction()
  {
    if (_transaction != null) throw new InvalidOperationException("Transaction already in progress");
    _transactionConn = new SqliteConnection(_connectionString);
    _transactionConn.Open();
    _transaction = _transactionConn.BeginTransaction();
  }

  public void Commit()
  {
    if (_transaction == null) return;
    _transaction.Commit();
    _transaction.Dispose();
    _transaction = null;
    if (_transactionConn != null) { _transactionConn.Dispose(); _transactionConn = null; }
  }

  public void Rollback()
  {
    if (_transaction == null) return;
    _transaction.Rollback();
    _transaction.Dispose();
    _transaction = null;
    if (_transactionConn != null) { _transactionConn.Dispose(); _transactionConn = null; }
  }

  public void RegisterModel(ModelDefinition model)
  {
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      // Persist fields inside metadata for portability
      var metadataObj = model.Metadata ?? new JsonObject();
      if (model.Fields != null && model.Fields.Any())
      {
        var fieldsArr = new JsonArray();
        foreach (var f in model.Fields)
        {
          var node = JsonSerializer.SerializeToNode(f, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
          fieldsArr.Add(node);
        }
        metadataObj["fields"] = fieldsArr;
      }
      // Persist model via schema_manager instead of 'models' table
      cmd.CommandText = $"INSERT OR REPLACE INTO schema_manager (model_name, module, metadata, applied_at) VALUES (@name, @module, @metadata, @applied);";
      cmd.Parameters.AddWithValue("@name", model.Name);
      cmd.Parameters.AddWithValue("@module", (model.Module ?? string.Empty));
      cmd.Parameters.AddWithValue("@metadata", metadataObj.ToJsonString());
      cmd.Parameters.AddWithValue("@applied", DateTime.UtcNow.ToString("o"));
      cmd.ExecuteNonQuery();
      Console.WriteLine($"SqliteStoreAdapter: Registered model {model.Name} with fields: {(model.Fields?.Count ?? 0)}");


      // ensure counters row exists if any field has AutoIncrement
      if (model.Fields != null && model.Fields.Any(f => f.AutoIncrement))
      {
        using var cmdc = conn.CreateCommand();
        cmdc.CommandText = "INSERT OR IGNORE INTO schema_counters (model_name, counter) VALUES (@model, 0);";
        cmdc.Parameters.AddWithValue("@model", model.Name);
        cmdc.ExecuteNonQuery();
      }
      // If model metadata requests a per-model table, or by default create per-model table
      var persistAsTable = true;
      if (model.Metadata != null && model.Metadata.TryGetPropertyValue("persistAsTable", out var pat) && pat != null)
      {
        var asBool = false;
        try { asBool = pat.GetValue<bool>(); } catch { try { asBool = bool.Parse(pat.GetValue<string?>() ?? "false"); } catch { asBool = false; } }
        persistAsTable = asBool;
      }
      if (persistAsTable) EnsureModelTableExists(model.Name);
      // If the table exists already, ensure all fields exist as columns
      try
      {
        var conn2 = GetOrCreateConnection(out var connCreated2);
        try
        {
          if (ModelTableExists(model.Name, conn2))
          {
            if (model.Fields != null)
            {
              foreach (var f in model.Fields) EnsureModelColumnExists(model.Name, f, conn2);
            }
          }
        }
        finally { if (connCreated2) conn2.Dispose(); }
      }
      catch { }
    }
    finally
    {
      if (connCreated) conn.Dispose();
    }
  }

  public void UpsertManagedSchema(ModelDefinition model, ModuleInfo module)
  {
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      var metadataObj = model.Metadata ?? new JsonObject();
      if (model.Fields != null && model.Fields.Any())
      {
        var fieldsArr = new JsonArray();
        foreach (var f in model.Fields)
        {
          var node = JsonSerializer.SerializeToNode(f, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
          fieldsArr.Add(node);
        }
        metadataObj["fields"] = fieldsArr;
      }
      cmd.CommandText = "INSERT OR REPLACE INTO schema_manager (model_name, module, metadata, applied_at) VALUES (@name, @module, @metadata, @applied);";
      cmd.Parameters.AddWithValue("@name", model.Name);
      cmd.Parameters.AddWithValue("@module", module?.Name ?? string.Empty);
      cmd.Parameters.AddWithValue("@metadata", metadataObj.ToJsonString());
      cmd.Parameters.AddWithValue("@applied", DateTime.UtcNow.ToString("o"));
      cmd.ExecuteNonQuery();
    }
    finally
    {
      if (connCreated) conn.Dispose();
    }
  }

  private string GetModelTableName(string modelName) => $"{_tablePrefix}{SqlProtection.ValidateModelName(modelName)}";

  private bool ModelTableExists(string modelName, SqliteConnection conn)
  {
    var tableName = GetModelTableName(modelName);
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT count(1) FROM sqlite_master WHERE type='table' AND name = @name;";
    cmd.Parameters.AddWithValue("@name", tableName);
    var v = cmd.ExecuteScalar();
    return Convert.ToInt32(v) > 0;
  }

  private bool ColumnExists(string tableName, string columnName, SqliteConnection conn)
  {
    using var cmd = conn.CreateCommand();
    cmd.CommandText = $"PRAGMA table_info(\"{tableName}\");";
    using var r = cmd.ExecuteReader();
    while (r.Read())
    {
      var col = r.GetString(1);
      if (string.Equals(col, columnName, StringComparison.OrdinalIgnoreCase)) return true;
    }
    return false;
  }

  private void EnsureModelColumnExists(string modelName, FieldDefinition field, SqliteConnection conn)
  {
    var tableName = GetModelTableName(modelName);
    try
    {
      if (ColumnExists(tableName, field.Name, conn)) return;
    }
    catch { }
    // Add column
    using var cmd = conn.CreateCommand();
    PrepareCommand(cmd);
    var sqlType = FieldTypeToSql(field.Type);
    var colDef = $"\"{SqlProtection.ValidateFieldName(field.Name)}\" {sqlType}";
    if (field.Length != null && (field.Type == FieldType.String || field.Type == FieldType.Text))
    {
      colDef += $" CHECK(LENGTH(\"{SqlProtection.ValidateFieldName(field.Name)}\") <= {field.Length.Value})";
    }
    if (field.DefaultValue != null)
    {
      colDef += " DEFAULT ";
      if (field.Type == FieldType.Number || field.Type == FieldType.Boolean) colDef += field.DefaultValue.ToString();
      else colDef += "'" + field.DefaultValue.ToString()?.Replace("'", "''") + "'";
    }
    if (field.Required) colDef += " NOT NULL";
    cmd.CommandText = $"ALTER TABLE \"{tableName}\" ADD COLUMN {colDef};";
    cmd.ExecuteNonQuery();
    // Note: adding foreign key constraints or primary keys requires table recreation in SQLite, not supported here.
  }

  private void EnsureModelTableExists(string modelName)
  {
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      var tableName = GetModelTableName(modelName);
      Console.WriteLine($"SqliteStoreAdapter: Creating/ensuring table {tableName}");
      // TODO: Create typed columns based on model definition stored in schema_manager (metadata.fields)
      // Retrieve the ModelDefinition from schema_manager metadata, if available
      var modelDef = GetModelDefinition(modelName);
      // Determine primary key column type: if model defines an 'id' numeric AutoIncrement field, make it the INTEGER PK AUTOINCREMENT
      var pkCol = "id TEXT PRIMARY KEY";
      var idField = modelDef?.Fields?.FirstOrDefault(f => string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase));
      if (idField != null && idField.AutoIncrement && idField.Type == FieldType.Number)
      {
        pkCol = "id INTEGER PRIMARY KEY AUTOINCREMENT";
      }
      var columns = new List<string> { pkCol };
      var pkFields = modelDef != null && modelDef.Fields != null ? modelDef.Fields.Where(f => f.PrimaryKey).Select(f => SqlProtection.ValidateFieldName(f.Name)).ToList() : new System.Collections.Generic.List<string>();
      if (modelDef != null && modelDef.Fields != null && modelDef.Fields.Any())
      {
        foreach (var f in modelDef.Fields)
        {
          try
          {
            // Skip 'id' because it will be used as primary key column
            if (string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase))
            {
              if (pkFields.Count > 0 && pkFields.Contains("id"))
              {
                // We'll create id column as normal, primary key handled separately
              }
              else continue;
            }
            var sqlType = FieldTypeToSql(f.Type);
            var col = $"\"{SqlProtection.ValidateFieldName(f.Name)}\" {sqlType}";
            // Handle length constraint for string-like fields
            if (f.Length != null && (f.Type == FieldType.String || f.Type == FieldType.Text))
            {
              // Add CHECK constraint: LENGTH <= value
              col += $" CHECK(LENGTH(\"{SqlProtection.ValidateFieldName(f.Name)}\") <= {f.Length.Value})";
            }
            // Add default value if present
            if (f.DefaultValue != null)
            {
              col += " DEFAULT ";
              if (f.Type == FieldType.Number || f.Type == FieldType.Boolean) col += f.DefaultValue.ToString();
              else col += "'" + f.DefaultValue.ToString()?.Replace("'", "''") + "'";
            }
            if (f.Required) col += " NOT NULL";
            columns.Add(col);
          }
          catch { }
        }
      }
      // Keep data column for compatibility only when model has no explicit fields or when metadata requests it
      var keepRawData = modelDef == null || modelDef.Fields == null || !modelDef.Fields.Any();
      if (modelDef != null && modelDef.Metadata != null && modelDef.Metadata.TryGetPropertyValue("persistRawJson", out var pr) && pr != null)
      {
        try { keepRawData = pr.GetValue<bool>(); } catch { try { keepRawData = bool.Parse(pr.GetValue<string?>() ?? "false"); } catch { } }
      }
      if (keepRawData) columns.Add("data TEXT");
      columns.Add("created_at TEXT");
      columns.Add("updated_at TEXT");
      // Add table level primary key constraint if fields declare PrimaryKey (non-id or composite)
      if (pkFields != null && pkFields.Count > 0)
      {
        // Ensure pk columns exist
        var pkList = string.Join(", ", pkFields.Select(p => "\"" + p + "\""));
        columns.Add($"PRIMARY KEY ({pkList})");
      }
      // Collect foreign key constraints for relation fields, append them as table constraints
      var fkConstraints = new System.Collections.Generic.List<string>();
      if (modelDef?.Fields != null)
      {
        foreach (var f in modelDef.Fields.Where(x => x.Relation != null))
        {
          try
          {
            var fname = SqlProtection.ValidateFieldName(f.Name);
            var targetTable = GetModelTableName(f.Relation!.Model);
            var onDelete = string.IsNullOrWhiteSpace(f.Relation.OnDelete) ? string.Empty : $" ON DELETE {f.Relation.OnDelete.ToUpper()}";
            var onUpdate = string.IsNullOrWhiteSpace(f.Relation.OnUpdate) ? string.Empty : $" ON UPDATE {f.Relation.OnUpdate.ToUpper()}";
            fkConstraints.Add($"FOREIGN KEY(\"{fname}\") REFERENCES \"{targetTable}\"(id){onDelete}{onUpdate}");
          }
          catch { }
        }
      }
      if (fkConstraints.Count > 0)
        cmd.CommandText = $"CREATE TABLE IF NOT EXISTS \"{tableName}\" ({string.Join(", ", columns)}, {string.Join(", ", fkConstraints)});";
      else
        cmd.CommandText = $"CREATE TABLE IF NOT EXISTS \"{tableName}\" ({string.Join(", ", columns)});";
      cmd.ExecuteNonQuery();
      _modelTableExists[modelName] = true;
      // If table existed and we don't want the raw 'data' column, migrate rows to remove it
      try
      {
        var tableHasRawData = false;
        try { tableHasRawData = ColumnExists(tableName, "data", conn); } catch { tableHasRawData = false; }
        if (!keepRawData && tableHasRawData)
        {
          // Ensure typed columns exist
          Console.WriteLine($"SqliteStoreAdapter: keepRawData={keepRawData}, tableHasRawData={tableHasRawData}, fieldsCount={(modelDef?.Fields?.Count ?? 0)} for {tableName}");
          if (modelDef != null && modelDef.Fields != null)
          {
            foreach (var f in modelDef.Fields)
            {
              try { EnsureModelColumnExists(modelName, f, conn); } catch { }
            }
          }

          // Populate typed columns from JSON data for existing rows
          try
          {
            // Debug: print data extraction and existing column values before updates
            if (modelDef?.Fields != null)
            {
              foreach (var fdbg in modelDef.Fields)
              {
                try
                {
                  var fn = SqlProtection.ValidateFieldName(fdbg.Name);
                  using var dbg4 = conn.CreateCommand();
                  dbg4.CommandText = $"SELECT json_extract(data, '$.{fn}') AS jv, \"{fn}\" as cval FROM \"{tableName}\" LIMIT 1;";
                  using var rr = dbg4.ExecuteReader();
                  if (rr.Read())
                  {
                    var jv = rr.IsDBNull(0) ? "null" : rr.GetValue(0).ToString();
                    var cv = rr.IsDBNull(1) ? "null" : rr.GetValue(1).ToString();
                    Console.WriteLine($"SqliteStoreAdapter: PRE-UPDATE {tableName}.{fn}: json_extract(data)={jv}, col={cv}");
                  }
                }
                catch { }
              }
            }
          }
          catch { }
          try
          {
            using var dbg2 = conn.CreateCommand();
            dbg2.CommandText = $"SELECT data FROM \"{tableName}\" LIMIT 1;";
            using var dr2 = dbg2.ExecuteReader();
            if (dr2.Read())
            {
              var dt = dr2.IsDBNull(0) ? string.Empty : dr2.GetString(0);
              Console.WriteLine($"SqliteStoreAdapter: DEBUG - {tableName} sample data = {dt}");
            }
          }
          catch { }
          if (modelDef != null && modelDef.Fields != null)
          {
            foreach (var f in modelDef.Fields)
            {
              if (string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase)) continue;
              try
              {
                var fname = SqlProtection.ValidateFieldName(f.Name);
                var jsonPath = "$." + fname;
                var castExpr = f.Type == FieldType.Number ? "CAST(json_extract(data, '$." + fname + "') AS REAL)" : f.Type == FieldType.Boolean ? "CASE json_extract(data, '$." + fname + "') WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE json_extract(data, '$." + fname + "') END" : $"json_extract(data, '$.{fname}')";
                using var ucmd = conn.CreateCommand();
                PrepareCommand(ucmd);
                ucmd.CommandText = $"UPDATE \"{tableName}\" SET \"{fname}\" = {castExpr} WHERE \"{fname}\" IS NULL;";
                ucmd.ExecuteNonQuery();
                try
                {
                  using var dbg = conn.CreateCommand();
                  dbg.CommandText = $"SELECT json_extract(data, '$.{fname}') AS jval, \"{fname}\" as cval FROM \"{tableName}\" LIMIT 1;";
                  using var rr = dbg.ExecuteReader();
                  if (rr.Read())
                  {
                    var jv = rr.IsDBNull(0) ? "null" : rr.GetValue(0).ToString();
                    var cv = rr.IsDBNull(1) ? "null" : rr.GetValue(1).ToString();
                    Console.WriteLine($"SqliteStoreAdapter: DEBUG - {tableName}.{fname}: json_extract(data)={jv}, col={cv}");
                  }
                }
                catch { }
              }
              catch { }
            }
          }

          try
          {
            Console.WriteLine($"SqliteStoreAdapter: Migrating data column to typed columns for {tableName} using helper");
            MigrateRemoveDataColumnToTypedColumns(tableName, modelDef, conn);
            Console.WriteLine($"SqliteStoreAdapter: Migration helper finished for {tableName}");
          }
          catch (Exception ex) { Console.WriteLine($"SqliteStoreAdapter: Migration helper failed for {tableName}: {ex.Message}"); }

          try
          {
            using var info = conn.CreateCommand(); info.CommandText = $"PRAGMA table_info('{tableName}');";
            using var ir = info.ExecuteReader();
            var cols = new System.Collections.Generic.List<string>();
            while (ir.Read()) cols.Add(ir.GetString(1));
            Console.WriteLine($"SqliteStoreAdapter: Post-migration columns for {tableName}: {string.Join(",", cols)}");
            try
            {
              using var dbg3 = conn.CreateCommand();
              dbg3.CommandText = $"SELECT id, \"name\", \"email\", json_extract(data, '$.name') as jname FROM \"{tableName}\" LIMIT 1;";
              using var rr2 = dbg3.ExecuteReader();
              if (rr2.Read())
              {
                var idv = rr2.IsDBNull(0) ? "null" : rr2.GetValue(0).ToString();
                var namev = rr2.IsDBNull(1) ? "null" : rr2.GetValue(1).ToString();
                var emailv = rr2.IsDBNull(2) ? "null" : rr2.GetValue(2).ToString();
                var jsonnv = rr2.IsDBNull(3) ? "null" : rr2.GetValue(3).ToString();
                Console.WriteLine($"SqliteStoreAdapter: Post-migration sample row: id={idv}, name={namev}, email={emailv}, json_name={jsonnv}");
              }
            }
            catch { }
          }
          catch { }
          Console.WriteLine($"SqliteStoreAdapter: Migration helper completed for {tableName}");
        }
      }
      catch { }
    }
    finally { if (connCreated) conn.Dispose(); }
  }

  private void MigrateRemoveDataColumnToTypedColumns(string tableName, ModelDefinition modelDef, SqliteConnection conn)
  {
    // Build new table name
    var tempTable = tableName + "_tmp";
    // Build column definitions excluding 'data'
    var colDefs = new List<string>();
    // primary key: id
    var idField = modelDef.Fields?.FirstOrDefault(f => string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase));
    if (idField != null && idField.AutoIncrement && idField.Type == FieldType.Number)
    {
      colDefs.Add("id INTEGER PRIMARY KEY AUTOINCREMENT");
    }
    else colDefs.Add("id TEXT PRIMARY KEY");
    // typed columns
    if (modelDef.Fields != null)
    {
      foreach (var f in modelDef.Fields)
      {
        if (string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase)) continue;
        var fname = SqlProtection.ValidateFieldName(f.Name);
        var sqlType = FieldTypeToSql(f.Type);
        var def = $"\"{fname}\" {sqlType}";
        if (f.Required) def += " NOT NULL";
        colDefs.Add(def);
      }
    }
    colDefs.Add("created_at TEXT");
    colDefs.Add("updated_at TEXT");

    using var cmd = conn.CreateCommand();
    PrepareCommand(cmd);
    cmd.CommandText = $"CREATE TABLE IF NOT EXISTS \"{tempTable}\" ({string.Join(", ", colDefs)});";
    cmd.ExecuteNonQuery();

    // Build select expression from old 'data' column
    var selectExprs = new List<string> { "id" };
    if (modelDef.Fields != null)
    {
      foreach (var f in modelDef.Fields)
      {
        if (string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase)) continue;
        var fname = SqlProtection.ValidateFieldName(f.Name);
        var expr = $"json_extract(data, '$.{fname}')";
        // cast to numeric for Number and Boolean
        if (f.Type == FieldType.Number) expr = $"CAST({expr} AS REAL)";
        if (f.Type == FieldType.Boolean) expr = $"CAST({expr} AS INTEGER)";
        selectExprs.Add(expr + " AS \"" + fname + "\"");
      }
    }
    selectExprs.Add("created_at");
    selectExprs.Add("updated_at");

    using var insertCmd = conn.CreateCommand();
    PrepareCommand(insertCmd);
    // The above builds column list by extracting alias parts. Simpler approach: use explicit names
    var colNames = new List<string> { "id" };
    if (modelDef.Fields != null)
    {
      foreach (var f in modelDef.Fields) if (!string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase)) colNames.Add($"\"{SqlProtection.ValidateFieldName(f.Name)}\"");
    }
    colNames.Add("created_at"); colNames.Add("updated_at");
    var selectSql = new List<string> { "id" };
    if (modelDef.Fields != null)
    {
      foreach (var f in modelDef.Fields)
      {
        if (string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase)) continue;
        var fname = SqlProtection.ValidateFieldName(f.Name);
        var expr = $"json_extract(data, '$.{fname}')";
        if (f.Type == FieldType.Number) expr = $"CAST({expr} AS REAL)";
        if (f.Type == FieldType.Boolean) expr = $"CAST({expr} AS INTEGER)";
        selectSql.Add(expr + " AS \"" + fname + "\"");
      }
    }
    selectSql.Add("created_at"); selectSql.Add("updated_at");
    insertCmd.CommandText = $"INSERT INTO \"{tempTable}\" ({string.Join(", ", colNames)}) SELECT {string.Join(", ", selectSql)} FROM \"{tableName}\";";
    insertCmd.ExecuteNonQuery();

    // Drop old table and rename
    using var dropCmd = conn.CreateCommand();
    PrepareCommand(dropCmd);
    dropCmd.CommandText = $"DROP TABLE IF EXISTS \"{tableName}\";";
    dropCmd.ExecuteNonQuery();
    using var renameCmd = conn.CreateCommand();
    PrepareCommand(renameCmd);
    renameCmd.CommandText = $"ALTER TABLE \"{tempTable}\" RENAME TO \"{tableName}\";";
    renameCmd.ExecuteNonQuery();
  }

  private static string FieldTypeToSql(FieldType type)
  {
    return type switch
    {
      FieldType.String => "TEXT",
      FieldType.Text => "TEXT",
      FieldType.Selection => "TEXT",
      FieldType.Relation => "TEXT",
      FieldType.Json => "TEXT",
      FieldType.Date => "TEXT",
      FieldType.Boolean => "INTEGER",
      FieldType.Number => "REAL",
      _ => "TEXT",
    };
  }

  private static object? GetSqlParamForJsonNode(JsonNode? node, FieldType fType)
  {
    if (node == null) return null;
    if (node is JsonValue jv)
    {
      try
      {
        if (fType == FieldType.Boolean && jv.TryGetValue(out bool bv)) return bv ? 1 : 0;
        if (fType == FieldType.Number && jv.TryGetValue(out int iv)) return iv;
        if (fType == FieldType.Number && jv.TryGetValue(out long lv)) return lv;
        if (fType == FieldType.Number && jv.TryGetValue(out double dv)) return dv;
        if (fType == FieldType.Boolean && jv.TryGetValue(out int ib)) return ib;
        if (fType == FieldType.String || fType == FieldType.Text || fType == FieldType.Relation || fType == FieldType.Selection || fType == FieldType.Json || fType == FieldType.Date)
        {
          if (jv.TryGetValue(out string? sv)) return sv;
        }
        // Fallback: return the raw JSON textual representation
        return jv.GetValue<object?>()?.ToString();
      }
      catch { }
    }
    // If node is JsonObject/Array, return its JSON string
    return node.ToJsonString();
  }

  public ModelDefinition? GetManagedSchema(string modelName)
  {
    SqlProtection.ValidateModelName(modelName);
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      cmd.CommandText = "SELECT module, metadata FROM schema_manager WHERE model_name = @name;";
      cmd.Parameters.AddWithValue("@name", modelName);
      using var reader = cmd.ExecuteReader();
      if (!reader.Read()) return null;
      var moduleVal = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
      var val = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
      var md = new ModelDefinition { Name = modelName, Module = moduleVal };
      if (!string.IsNullOrWhiteSpace(val))
      {
        try { md.Metadata = JsonNode.Parse(val) as JsonObject ?? new JsonObject(); }
        catch { md.Metadata = new JsonObject(); }
        if (md.Metadata.TryGetPropertyValue("fields", out var fnode) && fnode is JsonArray farr)
        {
          md.Fields = new List<FieldDefinition>();
          foreach (var el in farr)
          {
            try
            {
              var fd = JsonSerializer.Deserialize<FieldDefinition>(el!.ToJsonString(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
              if (fd != null) md.Fields.Add(fd);
            }
            catch { }
          }
        }
      }
      return md;
    }
    finally
    {
      if (connCreated) conn.Dispose();
    }
  }

  public void LogSchemaChange(string modelName, JsonObject change, ModuleInfo module, string operation)
  {
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      cmd.CommandText = "INSERT INTO schema_history (model_name, change, module, operation, applied_at) VALUES (@name, @change, @module, @op, @applied);";
      cmd.Parameters.AddWithValue("@name", modelName);
      cmd.Parameters.AddWithValue("@change", change?.ToJsonString() ?? string.Empty);
      cmd.Parameters.AddWithValue("@module", module?.Name ?? string.Empty);
      cmd.Parameters.AddWithValue("@op", operation);
      cmd.Parameters.AddWithValue("@applied", DateTime.UtcNow.ToString("o"));
      cmd.ExecuteNonQuery();
    }
    finally
    {
      if (connCreated) conn.Dispose();
    }
  }

  public bool ModelExists(string modelName)
  {
    SqlProtection.ValidateModelName(modelName);
    using var conn = new SqliteConnection(_connectionString);
    conn.Open();
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT COUNT(1) FROM schema_manager WHERE model_name = @name;";
    cmd.Parameters.AddWithValue("@name", modelName);
    var v = cmd.ExecuteScalar();
    return Convert.ToInt32(v) > 0;
  }

  public ModelDefinition? GetModelDefinition(string modelName)
  {
    SqlProtection.ValidateModelName(modelName);
    using var conn = new SqliteConnection(_connectionString);
    conn.Open();
    using var cmd = conn.CreateCommand();
    // Use schema_manager alone (no legacy models table)
    cmd.CommandText = "SELECT module, metadata FROM schema_manager WHERE model_name = @name;";
    cmd.Parameters.AddWithValue("@name", modelName);
    using var reader = cmd.ExecuteReader();
    if (!reader.Read()) return null;
    var moduleVal = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
    var val = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
    JsonObject obj;
    if (string.IsNullOrWhiteSpace(val)) obj = new JsonObject();
    else
    {
      try { obj = JsonNode.Parse(val) as JsonObject ?? new JsonObject(); }
      catch { obj = new JsonObject(); }
    }
    var md = new ModelDefinition { Name = modelName, Module = moduleVal, Metadata = obj };
    // If metadata contains fields array, deserialize them into ModelDefinition.Fields
    if (obj != null && obj.TryGetPropertyValue("fields", out var fnode) && fnode is JsonArray farr)
    {
      md.Fields = new System.Collections.Generic.List<FieldDefinition>();
      foreach (var el in farr)
      {
        try
        {
          var fd = JsonSerializer.Deserialize<FieldDefinition>(el!.ToJsonString(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
          if (fd != null) md.Fields.Add(fd);
        }
        catch { }
      }
    }
    return md;
  }

  public DynamicRecord CreateRecord(string modelName, JsonObject data)
  {
    SqlProtection.ValidateModelName(modelName);
    // if model has any auto-increment field, use counters to generate a numerical id
    var md = GetModelDefinition(modelName);
    bool hasAuto = md != null && md.Fields != null && md.Fields.Any(f => f.AutoIncrement);
    string id = Guid.NewGuid().ToString("N");
    if (hasAuto)
    {
      var connc = GetOrCreateConnection(out var conncCreated);
      try
      {
        using var cmdcnt = connc.CreateCommand();
        PrepareCommand(cmdcnt);
        cmdcnt.CommandText = "INSERT OR IGNORE INTO schema_counters (model_name, counter) VALUES (@model, 0);";
        cmdcnt.Parameters.AddWithValue("@model", modelName);
        cmdcnt.ExecuteNonQuery();
        using var cmdup = connc.CreateCommand();
        PrepareCommand(cmdup);
        cmdup.CommandText = "UPDATE schema_counters SET counter = counter + 1 WHERE model_name = @model;";
        cmdup.Parameters.AddWithValue("@model", modelName);
        cmdup.ExecuteNonQuery();
        using var cmdsel = connc.CreateCommand();
        PrepareCommand(cmdsel);
        cmdsel.CommandText = "SELECT counter FROM schema_counters WHERE model_name = @model;";
        cmdsel.Parameters.AddWithValue("@model", modelName);
        var v = cmdsel.ExecuteScalar();
        id = Convert.ToInt64(v).ToString();
      }
      finally { if (conncCreated) connc.Dispose(); }
    }
    var now = DateTime.UtcNow.ToString("o");
    var conn = GetOrCreateConnection(out var connCreated);
    conn.Open();
    using var cmd = conn.CreateCommand();
    PrepareCommand(cmd);
    var targetTable = GetModelTableName(modelName);
    try
    {
      if (!_modelTableExists.TryGetValue(modelName, out var exists))
      {
        exists = ModelTableExists(modelName, conn);
        // If not exists, create the table using model definition
        if (!exists)
        {
          EnsureModelTableExists(modelName);
          exists = true;
        }
        _modelTableExists[modelName] = exists;
      }
    }
    catch { }
    if (_modelTableExists.TryGetValue(modelName, out var existsFlag) && existsFlag)
    {
      targetTable = GetModelTableName(modelName);
      // Insert into typed columns as well as keep raw JSON 'data'
      var colNames = new List<string> { "id" };
      var paramNames = new List<string> { "@id" };
      if (md != null && md.Fields != null && md.Fields.Any())
      {
        foreach (var f in md.Fields)
        {
          var fname = SqlProtection.ValidateFieldName(f.Name);
          colNames.Add($"\"{fname}\"");
          paramNames.Add("@f_" + fname);
          // Add param value from JSON
          var paramVal = GetSqlParamForJsonNode(data.ContainsKey(f.Name) ? data[f.Name] : null, f.Type);
          cmd.Parameters.AddWithValue("@f_" + fname, paramVal ?? DBNull.Value);
        }
      }
      // Check if table has 'data' column
      var tableHasData = false;
      try { tableHasData = ColumnExists(targetTable, "data", conn); } catch { }
      if (tableHasData)
      {
        colNames.Add("data"); paramNames.Add("@data");
      }
      colNames.Add("created_at"); paramNames.Add("@created");
      colNames.Add("updated_at"); paramNames.Add("@updated");
      cmd.CommandText = $"INSERT INTO \"{targetTable}\" ({string.Join(", ", colNames)}) VALUES ({string.Join(", ", paramNames)});";
    }
    else
    {
      // As we no longer use the legacy `records` table, ensure per-model table and insert there
      EnsureModelTableExists(modelName);
      var t = GetModelTableName(modelName);
      // Rebuild param lists similar to the earlier branch
      var colNames2 = new List<string> { "id" };
      var paramNames2 = new List<string> { "@id" };
      if (md != null && md.Fields != null && md.Fields.Any())
      {
        foreach (var f in md.Fields)
        {
          var fname = SqlProtection.ValidateFieldName(f.Name);
          colNames2.Add($"\"{fname}\"");
          paramNames2.Add("@f_" + fname);
          var paramVal = GetSqlParamForJsonNode(data.ContainsKey(f.Name) ? data[f.Name] : null, f.Type);
          cmd.Parameters.AddWithValue("@f_" + fname, paramVal ?? DBNull.Value);
        }
      }
      var tableHasData2 = false;
      try { tableHasData2 = ColumnExists(t, "data", conn); } catch { }
      if (tableHasData2) { colNames2.Add("data"); paramNames2.Add("@data"); }
      colNames2.Add("created_at"); paramNames2.Add("@created");
      colNames2.Add("updated_at"); paramNames2.Add("@updated");
      cmd.CommandText = $"INSERT INTO \"{t}\" ({string.Join(", ", colNames2)}) VALUES ({string.Join(", ", paramNames2)});";
    }
    cmd.Parameters.AddWithValue("@id", id);
    if (md == null || md.Fields == null || !md.Fields.Any() || ColumnExists(GetModelTableName(modelName), "data", conn))
      cmd.Parameters.AddWithValue("@data", data?.ToJsonString() ?? string.Empty);
    cmd.Parameters.AddWithValue("@created", now);
    cmd.Parameters.AddWithValue("@updated", now);
    cmd.ExecuteNonQuery();
    // If persisted successfully, update managed schema counters; already done in RegisterModel

    return new DynamicRecord { Id = id, Data = data ?? new JsonObject(), CreatedAt = DateTime.UtcNow, UpdatedAt = DateTime.UtcNow };
  }

  public DynamicRecord? GetRecordById(string id)
  {
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);

      // If not found in generic records table (or generic disabled), try searching in per-model tables
      using var cmd2 = conn.CreateCommand();
      PrepareCommand(cmd2);
      cmd2.CommandText = "SELECT model_name FROM schema_manager";
      using var r = cmd2.ExecuteReader();
      while (r.Read())
      {
        var modelName = r.GetString(0);
        var table = GetModelTableName(modelName);
        using var cmd3 = conn.CreateCommand();
        PrepareCommand(cmd3);
        cmd3.CommandText = $"SELECT * FROM \"{table}\" WHERE id = @id;";
        cmd3.Parameters.AddWithValue("@id", id);
        try
        {
          using var r2 = cmd3.ExecuteReader();
          if (r2.Read())
          {
            var obj = new JsonObject();
            for (var i = 0; i < r2.FieldCount; i++)
            {
              var colName = r2.GetName(i);
              if (string.Equals(colName, "id", StringComparison.OrdinalIgnoreCase)) continue;
              if (string.Equals(colName, "created_at", StringComparison.OrdinalIgnoreCase) || string.Equals(colName, "updated_at", StringComparison.OrdinalIgnoreCase)) continue;
              try
              {
                if (r2.IsDBNull(i)) { obj[colName] = null; continue; }
                var val = r2.GetValue(i);
                // Attach primitive values
                if (val is long || val is int || val is double || val is float || val is decimal)
                {
                  obj[colName] = JsonNode.Parse(JsonSerializer.Serialize(val));
                }
                else if (val is int b && (r2.GetFieldType(i) == typeof(int) || r2.GetFieldType(i) == typeof(long)))
                {
                  obj[colName] = JsonNode.Parse(JsonSerializer.Serialize(val));
                }
                else
                {
                  obj[colName] = JsonNode.Parse(JsonSerializer.Serialize(val));
                }
              }
              catch
              {
                obj[colName] = null;
              }
            }
            // If there is a 'data' column, merge it
            try
            {
              var dt = r2.GetOrdinal("data");
              if (!r2.IsDBNull(dt))
              {
                var dataText = r2.GetString(dt);
                var dataObj = JsonNode.Parse(dataText) as JsonObject ?? new JsonObject();
                foreach (var kv in dataObj) obj[kv.Key] = kv.Value != null ? JsonNode.Parse(kv.Value!.ToJsonString()) : null;
              }
            }
            catch { }
            var created = r2.GetOrdinal("created_at");
            var updated = r2.GetOrdinal("updated_at");
            var createdVal = r2.IsDBNull(created) ? null : (string)r2.GetString(created);
            var updatedVal = r2.IsDBNull(updated) ? null : (string)r2.GetString(updated);
            var createdDt = createdVal != null ? DateTime.Parse(createdVal) : DateTime.UtcNow;
            var updatedDt = updatedVal != null ? DateTime.Parse(updatedVal) : DateTime.UtcNow;
            return new DynamicRecord { Id = id, Data = obj, CreatedAt = createdDt, UpdatedAt = updatedDt };
          }
        }
        catch { }
      }
      return null;
    }
    finally { if (connCreated) conn.Dispose(); }
  }

  public IEnumerable<DynamicRecord> GetRecords(string modelName, QueryOptions? options = null)
  {
    SqlProtection.ValidateModelName(modelName);
    var list = new List<DynamicRecord>();
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      var whereClause = "1=1";
      if (options != null && options.Where != null && options.Where.Any())
      {
        // Only support simple equality on top-level fields for POC
        var idx = 0;
        foreach (var c in options.Where)
        {
          if (c.Op != FilterOp.Eq) continue;
          var fieldName = SqlProtection.ValidateFieldName(c.Field);
          // If the per-model table has the column, compare directly; otherwise, try json_extract(data, '$.<field>')
          try
          {
            if (ColumnExists(GetModelTableName(modelName), fieldName, conn))
            {
              whereClause += $" AND \"{fieldName}\" = @v{idx}";
            }
            else
            {
              whereClause += $" AND json_extract(data, '$.{fieldName}') = @v{idx}";
            }
          }
          catch { whereClause += $" AND json_extract(data, '$.{fieldName}') = @v{idx}"; }
          cmd.Parameters.AddWithValue($"@v{idx}", c.Value?.ToString() ?? string.Empty);
          idx++;
        }
      }
      // Build using per-model tables (no legacy records fallback)
      var targetTable = GetModelTableName(modelName);
      if (options != null && options.Joins != null && options.Joins.Any())
      {
        // Only support a single join and equality on top-level fields for POC using per-model tables
        var join = options.Joins.First();
        SqlProtection.ValidateModelName(join.TargetModel);
        var aTable = GetModelTableName(modelName);
        var bTable = GetModelTableName(join.TargetModel);
        string FieldExpr(string tableName, string alias, string field)
        {
          if (field == "id") return alias + ".id";
          if (ColumnExists(tableName, field, conn)) return alias + ".\"" + SqlProtection.ValidateFieldName(field) + "\"";
          return $"json_extract({alias}.data, '$.{SqlProtection.ValidateFieldName(field)}')";
        }
        var targetField = SqlProtection.ValidateFieldName(join.TargetField);
        var sourceField = SqlProtection.ValidateFieldName(join.SourceField);
        var targetExpr = FieldExpr(bTable, "b", join.TargetField);
        var sourceExpr = FieldExpr(aTable, "a", join.SourceField);
        var sb = new System.Text.StringBuilder();
        var mdA = GetModelDefinition(modelName);
        var mdB = GetModelDefinition(join.TargetModel);
        var selectItems = new System.Collections.Generic.List<string>();
        // Include a.id always
        selectItems.Add("a.id");
        // Add a fields
        if (mdA != null && mdA.Fields != null)
        {
          foreach (var f in mdA.Fields)
          {
            var fname = SqlProtection.ValidateFieldName(f.Name);
            selectItems.Add($"a.\"{fname}\"");
          }
        }
        selectItems.Add("a.created_at");
        selectItems.Add("a.updated_at");
        // Add b fields as prefixed columns to avoid collisions
        if (mdB != null && mdB.Fields != null)
        {
          foreach (var f in mdB.Fields)
          {
            var fname = SqlProtection.ValidateFieldName(f.Name);
            var alias = SqlProtection.SanitizeIndexName(join.TargetModel, fname); // re-use sanitization helper to build alias
            // alias with dot as required by client merge
            var qual = join.TargetModel + "." + fname;
            selectItems.Add($"b.\"{fname}\" AS \"{qual}\"");
          }
        }
        selectItems.Add($"b.created_at AS \"{join.TargetModel}.created_at\"");
        selectItems.Add($"b.updated_at AS \"{join.TargetModel}.updated_at\"");
        sb.Append($"SELECT {string.Join(", ", selectItems)} FROM \"{aTable}\" a");
        sb.Append($" JOIN \"{bTable}\" b ON {targetExpr} = {sourceExpr} ");
        sb.Append($" WHERE 1=1 ");
        cmd.CommandText = sb.ToString();
      }
      else
      {
        // If a per-model table exists, use it (no model_name column); otherwise, return empty
        var tableName = GetModelTableName(modelName);
        var hasTable = false;
        try { hasTable = _modelTableExists.TryGetValue(modelName, out var ex) ? ex : ModelTableExists(modelName, conn); } catch { }
        if (!hasTable)
        {
          // Table not present: return no records
          return list;
        }
        cmd.CommandText = $"SELECT * FROM \"{tableName}\" WHERE {whereClause};";
      }
      cmd.Parameters.AddWithValue("@model", modelName);
      // GroupBy/Having/Offset support
      if (options != null && options.GroupBy != null && options.GroupBy.Any())
      {
        var opts = options!;
        var groupExprs = new List<string>();
        var aTableForGroup = GetModelTableName(modelName);
        var bTableForGroup = opts.Joins != null && opts.Joins.Any() ? GetModelTableName(opts.Joins.First().TargetModel) : string.Empty;
        Func<string, string> fieldToExpr = (field) =>
        {
          if (field == "count") return "COUNT(*)";
          if (field.Contains('.'))
          {
            var parts = field.Split('.', 2);
            var modelPart = parts[0];
            var fld = parts[1];
            if (opts.Joins != null && opts.Joins.Any() && opts.Joins.First().TargetModel == modelPart)
            {
              if (fld == "id") return "b.id";
              try { if (ColumnExists(bTableForGroup, fld, conn)) return $"b.\"{SqlProtection.ValidateFieldName(fld)}\""; } catch { }
              return $"json_extract(b.data, '$.{SqlProtection.ValidateFieldName(fld)}')";
            }
          }
          if (field == "id") return "a.id";
          try { if (ColumnExists(aTableForGroup, field, conn)) return $"a.\"{SqlProtection.ValidateFieldName(field)}\""; } catch { }
          return $"json_extract(a.data, '$.{SqlProtection.ValidateFieldName(field)}')";
        };
        int gi = 0;
        var selectList = new List<string>();
        foreach (var g in opts.GroupBy)
        {
          var expr = fieldToExpr(g);
          selectList.Add(expr + " AS g" + gi);
          groupExprs.Add(expr);
          gi++;
        }
        if (groupExprs.Count == 0) throw new ArgumentException("GroupBy specified but no valid fields");
        var sbg = new System.Text.StringBuilder();
        sbg.Append("SELECT ");
        sbg.Append(string.Join(",", selectList));
        sbg.Append($", COUNT(*) AS \"count\" FROM \"{aTableForGroup}\" a");
        if (opts.Joins != null && opts.Joins.Any())
        {
          var join = opts.Joins.First();
          var tField = SqlProtection.ValidateFieldName(join.TargetField);
          var sField = SqlProtection.ValidateFieldName(join.SourceField);
          string tExpr;
          string sExpr;
          try { if (tField == "id") tExpr = "b.id"; else if (ColumnExists(bTableForGroup, tField, conn)) tExpr = $"b.\"{tField}\""; else tExpr = $"json_extract(b.data, '$.{tField}')"; } catch { tExpr = $"json_extract(b.data, '$.{tField}')"; }
          try { if (sField == "id") sExpr = "a.id"; else if (ColumnExists(aTableForGroup, sField, conn)) sExpr = $"a.\"{sField}\""; else sExpr = $"json_extract(a.data, '$.{sField}')"; } catch { sExpr = $"json_extract(a.data, '$.{sField}')"; }
          sbg.Append($" JOIN \"{bTableForGroup}\" b ON {tExpr} = {sExpr} WHERE 1=1 ");
        }
        else
        {
          sbg.Append(" WHERE 1=1");
        }
        sbg.Append(" GROUP BY ");
        // SQLite does not allow aggregates inside GROUP BY; ensure groupExprs do not contain aggregate functions
        sbg.Append(string.Join(",", groupExprs.Select(e => e.Replace("COUNT(*)", "COUNTX()"))));
        // Having clauses must go after GROUP BY
        List<string> havingParts = new();
        if (opts.Having != null && opts.Having.Any())
        {
          var idx = 0;
          foreach (var c in opts.Having)
          {
            string left;
            if (c.Field == "count") left = "COUNT(*)";
            else left = fieldToExpr(c.Field);
            string op = c.Op switch
            {
              FilterOp.Eq => "=",
              FilterOp.Neq => "!=",
              FilterOp.Gt => ">",
              FilterOp.Gte => ">=",
              FilterOp.Lt => "<",
              FilterOp.Lte => "<=",
              FilterOp.Contains => "LIKE",
              _ => "="
            };
            var param = "@h" + idx;
            if (c.Op == FilterOp.Contains) havingParts.Add($"{left} {op} '%' || {param} || '%'");
            else havingParts.Add($"{left} {op} {param}");
            cmd.Parameters.AddWithValue(param, c.Value ?? string.Empty);
            idx++;
          }
          // DEBUG: print parameters
          // No parameter debug
        }
        // Order by
        if (!string.IsNullOrWhiteSpace(opts.OrderBy))
        {
          var ob = opts.OrderBy == "count" ? "\"count\"" : fieldToExpr(opts.OrderBy);
          sbg.Append($" ORDER BY {ob} {(opts.OrderDesc ? "DESC" : "ASC")} ");
        }
        if (opts.Limit != null) sbg.Append(" LIMIT " + opts.Limit.Value);
        if (opts.Offset != null) sbg.Append(" OFFSET " + opts.Offset.Value);
        if (havingParts.Count > 0) sbg.Append(" HAVING " + string.Join(" AND ", havingParts));
        cmd.CommandText = sbg.ToString();
        // GROUP SQL debug removed
        if (opts.Joins != null && opts.Joins.Any()) cmd.Parameters.AddWithValue("@targetModel", opts.Joins.First().TargetModel);
        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
          var rd = new JsonObject();
          for (var i = 0; i < groupExprs.Count; i++)
          {
            var val = r.IsDBNull(i) ? null : r.GetString(i);
            rd[opts.GroupBy[i]] = val != null ? JsonNode.Parse(JsonSerializer.Serialize(val)) : null;
          }
          var cnt = r.IsDBNull(groupExprs.Count) ? 0 : r.GetInt32(groupExprs.Count);
          // GROUP ROW debug removed
          rd["count"] = cnt;
          list.Add(new DynamicRecord { Id = Guid.NewGuid().ToString("N"), Data = rd, CreatedAt = DateTime.UtcNow, UpdatedAt = DateTime.UtcNow });
        }
        return list;
      }
      using var reader = cmd.ExecuteReader();
      var isModelTable = true; // per-model tables only; records legacy removed
      while (reader.Read())
      {
        if (isModelTable)
        {
          var recId = reader.GetString(reader.GetOrdinal("id"));
          var obj = new JsonObject();
          for (var i = 0; i < reader.FieldCount; i++)
          {
            var colName = reader.GetName(i);
            if (string.Equals(colName, "id", StringComparison.OrdinalIgnoreCase)) continue;
            if (string.Equals(colName, "created_at", StringComparison.OrdinalIgnoreCase) || string.Equals(colName, "updated_at", StringComparison.OrdinalIgnoreCase)) continue;
            try
            {
              if (reader.IsDBNull(i)) { obj[colName] = null; continue; }
              var val = reader.GetValue(i);
              obj[colName] = JsonNode.Parse(JsonSerializer.Serialize(val));
            }
            catch { obj[colName] = null; }
          }
          // merge data column if present
          try { var dataIdx = reader.GetOrdinal("data"); if (!reader.IsDBNull(dataIdx)) { var dataTxt = reader.GetString(dataIdx); var innerDataObj = JsonNode.Parse(dataTxt) as JsonObject ?? new JsonObject(); foreach (var kv in innerDataObj) obj[kv.Key] = kv.Value != null ? JsonNode.Parse(kv.Value!.ToJsonString()) : null; } } catch { }
          var createdIdx = reader.GetOrdinal("created_at"); var updatedIdx = reader.GetOrdinal("updated_at");
          var createdVal = reader.IsDBNull(createdIdx) ? null : (string)reader.GetString(createdIdx);
          var updatedVal = reader.IsDBNull(updatedIdx) ? null : (string)reader.GetString(updatedIdx);
          var createdDt = createdVal != null ? DateTime.Parse(createdVal) : DateTime.UtcNow;
          var updatedDt = updatedVal != null ? DateTime.Parse(updatedVal) : DateTime.UtcNow;
          list.Add(new DynamicRecord { Id = recId, Data = obj, CreatedAt = createdDt, UpdatedAt = updatedDt });
          continue;
        }
        var id = reader.GetString(0);
        var dataText = reader.GetString(1);
        string bDataText = string.Empty;
        if (options != null && options.Joins != null && options.Joins.Any())
        {
          try { bDataText = !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty; } catch { bDataText = string.Empty; }
        }
        var created = reader.GetString(2);
        var updated = reader.GetString(3);
        JsonObject? dataObj = null;
        if (!string.IsNullOrWhiteSpace(dataText))
        {
          try
          {
            var jsonNode = JsonNode.Parse(dataText);
            dataObj = jsonNode as JsonObject ?? new JsonObject();
          }
          catch { dataObj = new JsonObject(); }
        }

        var merged = dataObj ?? new JsonObject();
        if (!string.IsNullOrWhiteSpace(bDataText))
        {
          try
          {
            var jsonNode2 = JsonNode.Parse(bDataText) as JsonObject ?? new JsonObject();
            foreach (var kv in jsonNode2)
            {
              merged[$"{options!.Joins!.First().TargetModel}.{kv.Key}"] = kv.Value != null ? JsonNode.Parse(kv.Value!.ToJsonString()) : null;
            }
          }
          catch { }
        }
        list.Add(new DynamicRecord { Id = id, Data = merged, CreatedAt = DateTime.Parse(created), UpdatedAt = DateTime.Parse(updated) });
      }

      return list;
    }
    finally { if (connCreated) conn.Dispose(); }
  }

  public DynamicRecord UpdateRecord(string id, JsonObject data)
  {
    var now = DateTime.UtcNow.ToString("o");
    using var conn = new SqliteConnection(_connectionString);
    conn.Open();
    using var cmd = conn.CreateCommand();
    // Try update in per-model tables
    using var cmd2 = conn.CreateCommand();
    cmd2.CommandText = "SELECT model_name FROM schema_manager";
    using var r = cmd2.ExecuteReader();
    while (r.Read())
    {
      var mname = r.GetString(0);
      var tname = GetModelTableName(mname);
      // Build update statement dynamically
      var sets = new List<string> { "data = @data", "updated_at = @updated" };
      cmd2.Parameters.Clear();
      var tempCmd = conn.CreateCommand();
      PrepareCommand(tempCmd);
      foreach (var f in GetModelDefinition(mname)?.Fields ?? new List<FieldDefinition>())
      {
        var fname = SqlProtection.ValidateFieldName(f.Name);
        if (data.ContainsKey(f.Name))
        {
          sets.Add($"\"{fname}\" = @f_{fname}");
          tempCmd.Parameters.AddWithValue($"@f_{fname}", GetSqlParamForJsonNode(data[f.Name], f.Type) ?? DBNull.Value);
        }
      }
      tempCmd.Parameters.AddWithValue("@data", data.ToJsonString());
      tempCmd.Parameters.AddWithValue("@updated", now);
      tempCmd.Parameters.AddWithValue("@id", id);
      tempCmd.CommandText = $"UPDATE \"{tname}\" SET {string.Join(", ", sets)} WHERE id = @id;";
      try
      {
        var rr = tempCmd.ExecuteNonQuery();
        if (rr > 0) return new DynamicRecord { Id = id, Data = data, UpdatedAt = DateTime.UtcNow, CreatedAt = DateTime.UtcNow };
      }
      catch { }
    }
    throw new KeyNotFoundException("Record not found");
  }

  public void DeleteRecord(string id)
  {
    using var conn = new SqliteConnection(_connectionString);
    conn.Open();
    using var cmd = conn.CreateCommand();
    // Try delete from per-model tables
    using var cmd2 = conn.CreateCommand();
    cmd2.CommandText = "SELECT model_name FROM schema_manager";
    using var r = cmd2.ExecuteReader();
    while (r.Read())
    {
      var modelName = r.GetString(0);
      var table = GetModelTableName(modelName);
      using var cmd3 = conn.CreateCommand();
      try
      {
        cmd3.CommandText = $"DELETE FROM \"{table}\" WHERE id = @id;";
        cmd3.Parameters.AddWithValue("@id", id);
        var rr = cmd3.ExecuteNonQuery();
        if (rr > 0) return;
      }
      catch { }
    }
    throw new KeyNotFoundException("Record not found");
  }

  // Adapter helper to drop per-model tables (for demos/tests)
  public void DropAllModelTables(bool dropManagedSchemas = true)
  {
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      cmd.CommandText = "SELECT model_name FROM schema_manager";
      using var r = cmd.ExecuteReader();
      var names = new List<string>();
      while (r.Read()) names.Add(r.GetString(0));
      foreach (var mname in names)
      {
        var tname = GetModelTableName(mname);
        Console.WriteLine($"Dropping attempts: {tname}");
        using var dcmd = conn.CreateCommand();
        PrepareCommand(dcmd);
        dcmd.CommandText = $"DROP TABLE IF EXISTS \"{tname}\";";
        dcmd.ExecuteNonQuery();
        if (dropManagedSchemas)
        {
          using var mcmd = conn.CreateCommand();
          mcmd.CommandText = "DELETE FROM schema_manager WHERE model_name = @m;";
          mcmd.Parameters.AddWithValue("@m", mname);
          mcmd.ExecuteNonQuery();
        }
      }
    }
    finally { if (connCreated) conn.Dispose(); }
  }

  public DynamicRecord UpsertRecord(string modelName, string? id, JsonObject data)
  {
    SqlProtection.ValidateModelName(modelName);
    if (!string.IsNullOrWhiteSpace(id))
    {
      var existing = GetRecordById(id);
      if (existing != null)
      {
        return UpdateRecord(id, data);
      }
    }
    return CreateRecord(modelName, data);
  }

  public void ApplyImpact(ModuleInfo module, JsonObject impact)
  {
    if (impact == null) throw new ArgumentNullException(nameof(impact));
    if (!impact.TryGetPropertyValue("action", out var actNode)) throw new ArgumentException("Impact missing action");
    var action = actNode?.GetValue<string?>() ?? string.Empty;
    if (string.IsNullOrWhiteSpace(action)) throw new ArgumentException("Impact action empty");

    switch (action)
    {
      case "createModelTable":
        if (!impact.TryGetPropertyValue("targetModel", out var tmc) || tmc == null) throw new ArgumentException("createModelTable impact missing targetModel");
        var targetModelCT = SqlProtection.ValidateModelName(tmc.GetValue<string>());
        EnsureModelTableExists(targetModelCT);
        break;
      case "addField":
        if (!impact.TryGetPropertyValue("targetModel", out var tm) || tm == null) throw new ArgumentException("addField impact missing targetModel");
        var targetModel = SqlProtection.ValidateModelName(tm.GetValue<string>());
        if (!impact.TryGetPropertyValue("field", out var fieldNode) || fieldNode == null) throw new ArgumentException("addField impact missing field definition");
        var fieldJson = fieldNode.ToJsonString();
        var field = System.Text.Json.JsonSerializer.Deserialize<FieldDefinition>(fieldJson, new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        if (field == null) throw new ArgumentException("Invalid field definition");
        SqlProtection.ValidateFieldName(field.Name);
        var md = GetModelDefinition(targetModel);
        if (md == null) throw new InvalidOperationException($"Target model not found: {targetModel}");
        if (md.Fields == null) md.Fields = new List<FieldDefinition>();
        var existing = md.Fields.FirstOrDefault(f => f.Name == field.Name);
        if (existing != null)
        {
          // If the field is equal, skip; if different, replace the definition to apply changes.
          var sOld = JsonSerializer.Serialize(existing, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
          var sNew = JsonSerializer.Serialize(field, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
          if (sOld != sNew)
          {
            md.Fields.Remove(existing);
            md.Fields.Add(field);
          }
        }
        else
        {
          md.Fields.Add(field);
        }
        RegisterModel(md);
        try
        {
          var conn = GetOrCreateConnection(out var connCreated);
          try
          {
            if (ModelTableExists(targetModel, conn)) EnsureModelColumnExists(targetModel, field, conn);
          }
          finally { if (connCreated) conn.Dispose(); }
        }
        catch { }
        break;
      case "addRelation":
        if (!impact.TryGetPropertyValue("targetModel", out var tmr) || tmr == null) throw new ArgumentException("addRelation impact missing targetModel");
        var targetModelR = SqlProtection.ValidateModelName(tmr.GetValue<string>());
        if (!impact.TryGetPropertyValue("field", out var fieldNodeR) || fieldNodeR == null) throw new ArgumentException("addRelation impact missing field definition");
        var fieldJsonR = fieldNodeR.ToJsonString();
        var relField = System.Text.Json.JsonSerializer.Deserialize<FieldDefinition>(fieldJsonR, new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        if (relField == null) throw new ArgumentException("Invalid relation field definition");
        SqlProtection.ValidateFieldName(relField.Name);
        var mdR = GetModelDefinition(targetModelR);
        if (mdR == null) throw new InvalidOperationException($"Target model not found: {targetModelR}");
        if (mdR.Fields == null) mdR.Fields = new List<FieldDefinition>();
        var existingR = mdR.Fields.FirstOrDefault(f => f.Name == relField.Name);
        if (existingR != null)
        {
          var sOldR = JsonSerializer.Serialize(existingR, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
          var sNewR = JsonSerializer.Serialize(relField, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
          if (sOldR != sNewR)
          {
            mdR.Fields.Remove(existingR);
            mdR.Fields.Add(relField);
          }
        }
        else
        {
          mdR.Fields.Add(relField);
        }
        RegisterModel(mdR);
        break;
      case "addIndex":
        {
          if (!impact.TryGetPropertyValue("targetModel", out var tmi) || tmi == null) throw new ArgumentException("addIndex impact missing targetModel");
          var targetModelIndex = tmi.GetValue<string>();
          if (!impact.TryGetPropertyValue("field", out var fi) || fi == null) throw new ArgumentException("addIndex impact missing field");
          var fieldNameIndex = fi.GetValue<string>();
          // sanitize index name
          // sanitize identifiers
          var tmSan = SqlProtection.ValidateModelName(targetModelIndex);
          var fnSan = SqlProtection.ValidateFieldName(fieldNameIndex);
          var idxName = SqlProtection.SanitizeIndexName(tmSan, fnSan);
          var conn = GetOrCreateConnection(out var connCreated);
          try
          {
            using var cmd = conn.CreateCommand();
            PrepareCommand(cmd);
            var tname = GetModelTableName(targetModelIndex);
            try
            {
              if (ColumnExists(tname, fnSan, conn)) cmd.CommandText = $"CREATE INDEX IF NOT EXISTS \"{idxName}\" ON \"{tname}\" (\"{fnSan}\");";
              else cmd.CommandText = $"CREATE INDEX IF NOT EXISTS \"{idxName}\" ON \"{tname}\" (json_extract(data, '$.{fnSan}'));";
            }
            catch { cmd.CommandText = $"CREATE INDEX IF NOT EXISTS \"{idxName}\" ON \"{tname}\" (json_extract(data, '$.{fnSan}'));"; }
            cmd.ExecuteNonQuery();
          }
          finally { if (connCreated) conn.Dispose(); }
          break;
        }
      case "extendEnum":
        if (!impact.TryGetPropertyValue("targetModel", out var tme) || tme == null) throw new ArgumentException("extendEnum impact missing targetModel");
        var targetModelEnum = SqlProtection.ValidateModelName(tme.GetValue<string>());
        if (!impact.TryGetPropertyValue("field", out var fne) || fne == null) throw new ArgumentException("extendEnum impact missing field");
        var fieldNameEnum = fne.GetValue<string>();
        if (!impact.TryGetPropertyValue("values", out var vals) || vals == null) throw new ArgumentException("extendEnum impact missing values array");
        var arr = vals as JsonArray;
        SqlProtection.ValidateFieldName(fieldNameEnum);
        var mdE = GetModelDefinition(targetModelEnum);
        if (mdE == null) throw new InvalidOperationException($"Target model not found: {targetModelEnum}");
        if (mdE.Metadata == null) mdE.Metadata = new JsonObject();
        if (!mdE.Metadata.TryGetPropertyValue("enums", out var enumsObj) || enumsObj == null)
        {
          mdE.Metadata["enums"] = new JsonObject();
          enumsObj = mdE.Metadata["enums"];
        }
        var enumsJson = enumsObj as JsonObject ?? new JsonObject();
        var existingArr = enumsJson.ContainsKey(fieldNameEnum) && enumsJson[fieldNameEnum] is JsonArray exArr ? exArr : new JsonArray();
        var set = new HashSet<string>(existingArr.Select(x => x!.GetValue<string>()));
        foreach (var v in arr!) set.Add(v!.GetValue<string>());
        var merged = new JsonArray();
        foreach (var s in set) merged.Add(s);
        enumsJson[fieldNameEnum] = merged;
        mdE.Metadata["enums"] = enumsJson;
        RegisterModel(mdE);
        break;
      default:
        throw new NotImplementedException($"Impact action not supported: {action}");
    }
  }

  public void DropModelTable(string modelName)
  {
    SqlProtection.ValidateModelName(modelName);
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      var table = GetModelTableName(modelName);
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      cmd.CommandText = $"DROP TABLE IF EXISTS \"{table}\";";
      cmd.ExecuteNonQuery();
      // Clean metadata rows
      using var cmd2 = conn.CreateCommand();
      cmd2.CommandText = "DELETE FROM schema_manager WHERE model_name = @m;";
      cmd2.Parameters.AddWithValue("@m", modelName);
      cmd2.ExecuteNonQuery();

      using var cmd5 = conn.CreateCommand();
      cmd5.CommandText = "DELETE FROM schema_counters WHERE model_name = @m;";
      cmd5.Parameters.AddWithValue("@m", modelName);
      cmd5.ExecuteNonQuery();
      _modelTableExists.Remove(modelName);
    }
    finally { if (connCreated) conn.Dispose(); }
  }

  public void DropAllModelTables()
  {
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      cmd.CommandText = "SELECT model_name FROM schema_manager";
      using var r = cmd.ExecuteReader();
      var names = new List<string>();
      while (r.Read()) names.Add(r.GetString(0));
      foreach (var n in names) DropModelTable(n);
    }
    finally { if (connCreated) conn.Dispose(); }
  }

}
