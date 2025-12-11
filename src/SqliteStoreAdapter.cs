using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.Sqlite;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Linq;
using DynamicOrmLib;

namespace DynamicOrmLib.Adapters.Sqlite;

public partial class SqliteStoreAdapter : IStoreProvider
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

  public class SqliteConnectionOptions
  {
    public string ConnectionString { get; set; } = null!;
    public string? Password { get; set; }
    public bool UseEncryption { get; set; }
    public bool OpenInReadonly { get; set; }
  }

  public SqliteStoreAdapter(SqliteConnectionOptions options, string tablePrefix = "records_") : this(options.ConnectionString, tablePrefix)
  {
    if (!string.IsNullOrWhiteSpace(options.Password)) _connectionString += $";Password={options.Password}";
    if (options.OpenInReadonly) _connectionString += ";Mode=ReadOnly";
    // Note: UseEncryption will depend on underlying SQLite provider (SQLCipher); password indicates encrypted DB
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





  // Convenience wrapper to construct SqliteStoreAdapter from options
  public static SqliteStoreAdapter FromOptions(SqliteConnectionOptions options)
  {
    return new SqliteStoreAdapter(options, "records_");
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
      var hasJoins = options != null && options.Joins != null && options.Joins.Any();
      if (!hasJoins && options != null && options.Where != null && options.Where.Any())
      {
        // Support various operators and logic
        var idx = 0;
        var logicOp = options.Where.First().LogicOp; // Assume all have same logic op
        var conditions = new List<string>();
        foreach (var c in options.Where)
        {
          var fieldName = SqlProtection.ValidateFieldName(c.Field);
          string op;
          switch (c.Op)
          {
            case FilterOp.Eq: op = "="; break;
            case FilterOp.Neq: op = "!="; break;
            case FilterOp.Gt: op = ">"; break;
            case FilterOp.Gte: op = ">="; break;
            case FilterOp.Lt: op = "<"; break;
            case FilterOp.Lte: op = "<="; break;
            case FilterOp.Contains: op = "LIKE"; break;
            default: op = "="; break;
          }
          string condition;
          try
          {
            if (ColumnExists(GetModelTableName(modelName), fieldName, conn))
            {
              condition = $"\"{fieldName}\" {op} @v{idx}";
            }
            else
            {
              condition = $"json_extract(data, '$.{fieldName}') {op} @v{idx}";
            }
          }
          catch { condition = $"json_extract(data, '$.{fieldName}') {op} @v{idx}"; }
          conditions.Add(condition);
          cmd.Parameters.AddWithValue($"@v{idx}", c.Value?.ToString() ?? string.Empty);
          idx++;
        }
        var joinOp = logicOp == LogicOp.Or ? " OR " : " AND ";
        whereClause += " AND (" + string.Join(joinOp, conditions) + ")";
      }
      // Build using per-model tables (no legacy records fallback)
      var targetTable = GetModelTableName(modelName);
      // Convert Includes to Joins if needed
      if (options != null && options.Includes != null && options.Includes.Any() && (options.Joins == null || !options.Joins.Any()))
      {
        options.Joins = ConvertIncludesToJoins(modelName, options.Includes, options);
      }
      if (options != null && options.Joins != null && options.Joins.Any())
      {
        // Support multiple joins using per-model tables
        var joinParams = new List<KeyValuePair<string, object>>();
        var joinSql = BuildJoinSelectSql(modelName, options.Joins, options!, conn, joinParams);
        cmd.CommandText = joinSql;
        foreach (var p in joinParams) cmd.Parameters.AddWithValue(p.Key, p.Value);
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
        var simpleParams = new List<KeyValuePair<string, object>>();
        var simpleSql = BuildSimpleSelectSql(modelName, options, whereClause, conn, simpleParams);
        cmd.CommandText = simpleSql;
        foreach (var p in simpleParams) cmd.Parameters.AddWithValue(p.Key, p.Value);
      }
      cmd.Parameters.AddWithValue("@model", modelName);
      // GroupBy/Having/Offset support
      if (options != null && options.GroupBy != null && options.GroupBy.Any())
      {
        var opts = options!;
        var grpParams = new List<KeyValuePair<string, object>>();
        var grpSql = BuildGroupBySql(opts, modelName, whereClause, conn, grpParams);
        cmd.CommandText = grpSql;
        foreach (var p in grpParams) cmd.Parameters.AddWithValue(p.Key, p.Value);
        if (opts.Joins != null && opts.Joins.Any()) cmd.Parameters.AddWithValue("@targetModel", opts.Joins.First().TargetModel);
        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
          var rd = new JsonObject();
          for (var i = 0; i < opts.GroupBy.Count; i++)
          {
            var val = r.IsDBNull(i) ? null : r.GetString(i);
            rd[opts.GroupBy[i]] = val != null ? JsonNode.Parse(JsonSerializer.Serialize(val)) : null;
          }
          var cnt = r.IsDBNull(opts.GroupBy.Count) ? 0 : r.GetInt32(opts.GroupBy.Count);
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

  public void DeleteRecords(string modelName, QueryOptions options)
  {
    SqlProtection.ValidateModelName(modelName);
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      var whereClause = "1=1";
      if (options != null && options.Where != null && options.Where.Any())
      {
        // Only support simple equality on top-level fields
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

      var tableName = GetModelTableName(modelName);
      cmd.CommandText = $"DELETE FROM \"{tableName}\" WHERE {whereClause};";
      cmd.ExecuteNonQuery();
    }
    finally
    {
      if (connCreated) conn.Dispose();
    }
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

  private List<JoinDefinition> ConvertIncludesToJoins(string baseModel, List<IncludeDefinition> includes, QueryOptions options)
  {
    var joins = new List<JoinDefinition>();
    foreach (var include in includes)
    {
      ConvertIncludeToJoins(baseModel, include, joins, options);
    }
    return joins;
  }

  private void ConvertIncludeToJoins(string sourceModel, IncludeDefinition include, List<JoinDefinition> joins, QueryOptions options)
  {
    var join = new JoinDefinition
    {
      Type = include.Required ? JoinType.Inner : JoinType.Left,
      SourceModel = sourceModel,
      TargetModel = include.Model,
      SourceField = include.ForeignKey ?? $"{include.Model}_id", // default foreign key
      TargetField = include.TargetKey ?? "id",
      Alias = include.As ?? include.Model,
      Comparator = FilterOp.Eq
    };
    joins.Add(join);

    // Add where conditions for this include
    if (include.Where != null && include.Where.Any())
    {
      var alias = include.As ?? include.Model;
      foreach (var cond in include.Where)
      {
        var aliasedCondition = new FilterCondition
        {
          Field = $"{alias}.{cond.Field}",
          Op = cond.Op,
          Value = cond.Value
        };
        options.Where.Add(aliasedCondition);
      }
    }

    // Add nested includes
    if (include.Include != null && include.Include.Any())
    {
      foreach (var nested in include.Include)
      {
        ConvertIncludeToJoins(include.Model, nested, joins, options);
      }
    }
  }

}
