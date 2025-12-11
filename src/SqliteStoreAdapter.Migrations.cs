using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DynamicOrmLib.Adapters.Sqlite;

public partial class SqliteStoreAdapter
{
  private void MigrateRenamedFieldsIfAny(ModelDefinition? existing, ModelDefinition model, SqliteConnection conn)
  {
    if (model == null || model.Fields == null || !model.Fields.Any()) return;
    if (existing == null || existing.Fields == null || !existing.Fields.Any()) return;

    var existingNames = existing.Fields.Select(f => f.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
    // For each new field that doesn't exist, try to find an existing field with same signature
    foreach (var newField in model.Fields)
    {
      if (existingNames.Contains(newField.Name)) continue;
      FieldDefinition? candidate = null;
      foreach (var oldField in existing.Fields)
      {
        if (existingNames.Contains(newField.Name)) break;
        if (string.Equals(oldField.Name, newField.Name, StringComparison.OrdinalIgnoreCase)) { candidate = null; break; }
        // Heuristic: same type, same length (if provided), same required flag, same relation target
        var sameType = oldField.Type == newField.Type;
        var sameLen = (oldField.Length == null && newField.Length == null) || (oldField.Length != null && newField.Length != null && oldField.Length == newField.Length);
        var sameReq = oldField.Required == newField.Required;
        var sameRel = (oldField.Relation == null && newField.Relation == null) || (oldField.Relation != null && newField.Relation != null && string.Equals(oldField.Relation.Model, newField.Relation.Model, StringComparison.OrdinalIgnoreCase));
        if (sameType && sameLen && sameReq && sameRel)
        {
          candidate = oldField; break;
        }
      }
      if (candidate != null)
      {
        try
        {
          RenameModelColumn(model.Name, candidate.Name, newField.Name, conn);
        }
        catch { }
      }
    }
  }

  private void RenameModelColumn(string modelName, string oldName, string newName, SqliteConnection conn)
  {
    var tableName = GetModelTableName(modelName);
    try
    {
      // Validate column existence
      if (!ColumnExists(tableName, oldName, conn)) return;
      if (ColumnExists(tableName, newName, conn)) return; // target exists, skip
      using var cmd = conn.CreateCommand();
      PrepareCommand(cmd);
      cmd.CommandText = $"ALTER TABLE \"{tableName}\" RENAME COLUMN \"{SqlProtection.ValidateFieldName(oldName)}\" TO \"{SqlProtection.ValidateFieldName(newName)}\";";
      cmd.ExecuteNonQuery();
      Console.WriteLine($"SqliteStoreAdapter: Renamed column {oldName} -> {newName} on {tableName}");
    }
    catch (Exception ex)
    {
      Console.WriteLine($"SqliteStoreAdapter: Rename column failed for {modelName}: {ex.Message}");
    }
  }
}
