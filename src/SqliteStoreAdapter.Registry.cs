using System;
using System.Text.Json;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace DynamicOrmLib.Adapters.Sqlite;

public partial class SqliteStoreAdapter
{
  public void RegisterModel(ModelDefinition model)
  {
    var conn = GetOrCreateConnection(out var connCreated);
    try
    {
      // Read existing persisted model definition (if any) before updating schema_manager
      var existingModelDef = GetModelDefinition(model.Name);
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
      if (persistAsTable)
      {
        // Attempt to detect field renames and migrate existing table columns before creating/ensuring table
        try
        {
          MigrateRenamedFieldsIfAny(existingModelDef, model, conn);
        }
        catch { }
        EnsureModelTableExists(model.Name);
      }
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
}
