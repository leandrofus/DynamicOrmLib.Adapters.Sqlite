using System;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Linq;
using Microsoft.Data.Sqlite;
using System.Collections.Generic;

namespace DynamicOrmLib.Adapters.Sqlite;

public partial class SqliteStoreAdapter
{
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

  private void MigrateRemoveDataColumnToTypedColumns(string tableName, ModelDefinition modelDef, SqliteConnection conn)
  {
    var tempTable = tableName + "_tmp";
    var colDefs = new List<string>();
    var idField = modelDef.Fields?.FirstOrDefault(f => string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase));
    if (idField != null && idField.AutoIncrement && idField.Type == FieldType.Number) colDefs.Add("id INTEGER PRIMARY KEY AUTOINCREMENT");
    else colDefs.Add("id TEXT PRIMARY KEY");
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
    using var insertCmd = conn.CreateCommand();
    PrepareCommand(insertCmd);
    insertCmd.CommandText = $"INSERT INTO \"{tempTable}\" ({string.Join(", ", colNames)}) SELECT {string.Join(", ", selectSql)} FROM \"{tableName}\";";
    insertCmd.ExecuteNonQuery();
    using var dropCmd = conn.CreateCommand();
    PrepareCommand(dropCmd);
    dropCmd.CommandText = $"DROP TABLE IF EXISTS \"{tableName}\";";
    dropCmd.ExecuteNonQuery();
    using var renameCmd = conn.CreateCommand();
    PrepareCommand(renameCmd);
    renameCmd.CommandText = $"ALTER TABLE \"{tempTable}\" RENAME TO \"{tableName}\";";
    renameCmd.ExecuteNonQuery();
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
      // Reuse existing 'cmd' declared earlier
      var tableName = GetModelTableName(modelName);
      Console.WriteLine($"SqliteStoreAdapter: Creating/ensuring table {tableName}");
      // TODO: Create typed columns based on model definition stored in schema_manager (metadata.fields)
      // Retrieve the ModelDefinition from schema_manager metadata, if available
      var modelDef = GetModelDefinition(modelName);
      // Determine primary key column type: if model defines an 'id' numeric AutoIncrement field, make it the INTEGER PK AUTOINCREMENT
      var pkCol = "";
      var idField = modelDef?.Fields?.FirstOrDefault(f => string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase));
      if (idField != null && idField.AutoIncrement && idField.Type == FieldType.Number)
      {
        pkCol = "id INTEGER PRIMARY KEY AUTOINCREMENT";
      }
      else if (idField == null)
      {
        // Only add default id column if no 'id' field is defined
        pkCol = "id TEXT PRIMARY KEY";
      }
      var columns = new List<string>();
      if (!string.IsNullOrEmpty(pkCol)) columns.Add(pkCol);
      var pkFields = modelDef != null && modelDef.Fields != null ? modelDef.Fields.Where(f => f.PrimaryKey).Select(f => SqlProtection.ValidateFieldName(f.Name)).ToList() : new System.Collections.Generic.List<string>();
      if (modelDef != null && modelDef.Fields != null && modelDef.Fields.Any())
      {
        foreach (var f in modelDef.Fields)
        {
          try
          {
            // Skip 'id' because it is handled as primary key column separately
            if (string.Equals(f.Name, "id", StringComparison.OrdinalIgnoreCase))
            {
              continue;
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
      // Add table level primary key constraint if fields declare PrimaryKey (composite or non-id)
      if (pkFields != null && pkFields.Count > 0 && !(pkFields.Count == 1 && pkFields.Contains("id")))
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
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
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
            if (modelDef != null) MigrateRemoveDataColumnToTypedColumns(tableName, modelDef, conn);
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
}
