using System;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
namespace DynamicOrmLib.Adapters.Sqlite;

public partial class SqliteStoreAdapter
{
  private string BuildJoinSelectSql(string modelName, List<JoinDefinition> joins, QueryOptions options, SqliteConnection conn, List<KeyValuePair<string, object>> parameters)
  {
    if (joins == null || !joins.Any()) return "";

    string GetFieldExpr(string tableName, string alias, string field)
    {
      if (field == "id") return $"{alias}.id";
      if (ColumnExists(tableName, field, conn)) return $"{alias}.\"{SqlProtection.ValidateFieldName(field)}\"";
      return $"json_extract({alias}.data, '$.{SqlProtection.ValidateFieldName(field)}')";
    }

    string GetFieldExprForWhere(string field, Dictionary<string, string> tableAliases, string baseTable, SqliteConnection conn)
    {
      if (field.Contains('.'))
      {
        var parts = field.Split('.', 2);
        string tableAlias;
        string tableName;

        // Check if parts[0] is an alias or a model name
        if (tableAliases.ContainsKey(parts[0]))
        {
          // parts[0] is a model name
          tableAlias = tableAliases[parts[0]];
          tableName = GetModelTableName(parts[0]);
        }
        else
        {
          // parts[0] might be an alias, find the corresponding model name
          var modelName = tableAliases.FirstOrDefault(kv => kv.Value == parts[0]).Key;
          if (modelName != null)
          {
            tableAlias = parts[0];
            tableName = GetModelTableName(modelName);
          }
          else
          {
            // Default to base table
            tableAlias = "a";
            tableName = baseTable;
          }
        }
        return GetFieldExpr(tableName, tableAlias, parts[1]);
      }
      else
      {
        return GetFieldExpr(baseTable, "a", field);
      }
    }

    var baseTable = GetModelTableName(modelName);
    var mdBase = GetModelDefinition(modelName);

    var selectItems = new List<string>();
    var joinClauses = new List<string>();
    var tableAliases = new Dictionary<string, string>();

    // Base table
    string baseAlias = "a";
    tableAliases[modelName] = baseAlias;
    selectItems.Add($"{baseAlias}.id");
    if (mdBase != null && mdBase.Fields != null)
    {
      foreach (var f in mdBase.Fields)
        selectItems.Add($"{baseAlias}.\"{SqlProtection.ValidateFieldName(f.Name)}\"");
    }
    selectItems.Add($"{baseAlias}.created_at");
    selectItems.Add($"{baseAlias}.updated_at");

    // Process each join
    char aliasChar = 'b';
    foreach (var join in joins)
    {
      SqlProtection.ValidateModelName(join.TargetModel);
      var targetTable = GetModelTableName(join.TargetModel);
      var targetAlias = join.Alias ?? aliasChar.ToString();
      tableAliases[join.TargetModel] = targetAlias;

      var mdTarget = GetModelDefinition(join.TargetModel);
      if (mdTarget != null && mdTarget.Fields != null)
      {
        foreach (var f in mdTarget.Fields)
        {
          var fname = SqlProtection.ValidateFieldName(f.Name);
          var qual = (join.Alias ?? join.TargetModel) + "." + fname;
          selectItems.Add($"{targetAlias}.\"{fname}\" AS \"{qual}\"");
        }
      }
      selectItems.Add($"{targetAlias}.created_at AS \"{(join.Alias ?? join.TargetModel)}.created_at\"");
      selectItems.Add($"{targetAlias}.updated_at AS \"{(join.Alias ?? join.TargetModel)}.updated_at\"");

      // Build join condition
      var sourceAlias = tableAliases[join.SourceModel];
      var sourceTable = GetModelTableName(join.SourceModel);
      var targetTableForExpr = GetModelTableName(join.TargetModel);

      var sourceExpr = GetFieldExpr(sourceTable, sourceAlias, join.SourceField);
      var targetExpr = GetFieldExpr(targetTableForExpr, targetAlias, join.TargetField);

      string op = join.Comparator switch
      {
        FilterOp.Eq => "=",
        FilterOp.Neq => "!=",
        FilterOp.Gt => ">",
        FilterOp.Gte => ">=",
        FilterOp.Lt => "<",
        FilterOp.Lte => "<=",
        _ => "="
      };

      var joinType = join.Type == JoinType.Inner ? "JOIN" : "LEFT JOIN";
      joinClauses.Add($"{joinType} \"{targetTable}\" {targetAlias} ON {sourceExpr} {op} {targetExpr}");

      aliasChar++;
    }

    var sb = new StringBuilder();
    sb.Append($"SELECT {string.Join(", ", selectItems)} FROM \"{baseTable}\" {baseAlias}");
    foreach (var joinClause in joinClauses)
    {
      sb.Append($" {joinClause}");
    }
    sb.Append(" WHERE 1=1");

    // Build WHERE clause for joins
    if (options != null && options.Where != null && options.Where.Any())
    {
      var idx = 0;
      foreach (var c in options.Where)
      {
        if (c.Op != FilterOp.Eq) continue; // For now, only support Eq
        var fieldExpr = GetFieldExprForWhere(c.Field, tableAliases, baseTable, conn);
        var paramName = $"@join_v{idx}";
        sb.Append($" AND {fieldExpr} = {paramName}");
        parameters.Add(new KeyValuePair<string, object>(paramName, c.Value?.ToString() ?? string.Empty));
        idx++;
      }
    }

    // If no GROUP BY, add ORDER/LIMIT/OFFSET
    if (options != null && (options.GroupBy == null || !options.GroupBy.Any()))
    {
      if (!string.IsNullOrWhiteSpace(options?.OrderBy))
      {
        var obField = options.OrderBy!;
        string obExpr;
        if (obField.Contains('.'))
        {
          var parts = obField.Split('.', 2);
          var tableAlias = tableAliases.ContainsKey(parts[0]) ? tableAliases[parts[0]] : baseAlias;
          var tableName = parts[0] == modelName ? baseTable : GetModelTableName(parts[0]);
          obExpr = GetFieldExpr(tableName, tableAlias, parts[1]);
        }
        else
        {
          obExpr = GetFieldExpr(baseTable, baseAlias, obField);
        }
        sb.Append($" ORDER BY {obExpr} {(options.OrderDesc ? "DESC" : "ASC")} ");
      }
      if (options?.Limit != null) sb.Append(" LIMIT " + options.Limit.Value);
      if (options?.Offset != null) sb.Append(" OFFSET " + options.Offset.Value);
    }
    return sb.ToString();
  }

  private string BuildSimpleSelectSql(string modelName, QueryOptions? options, string whereClause, SqliteConnection conn, List<KeyValuePair<string, object>> parameters)
  {
    var tableName = GetModelTableName(modelName);
    var sb = new StringBuilder();
    sb.Append($"SELECT * FROM \"{tableName}\" WHERE {whereClause} ");
    if (options != null)
    {
      if (!string.IsNullOrWhiteSpace(options.OrderBy))
      {
        var obField = options.OrderBy!;
        string obExpr;
        if (obField.Contains('.'))
        {
          var parts = obField.Split('.', 2);
          if (parts[0] == modelName) obExpr = ColumnExists(tableName, parts[1], conn) ? $"\"{SqlProtection.ValidateFieldName(parts[1])}\"" : $"json_extract(data, '$.{SqlProtection.ValidateFieldName(parts[1])}')";
          else obExpr = $"json_extract(data, '$.{SqlProtection.ValidateFieldName(parts[1])}')";
        }
        else
        {
          obExpr = ColumnExists(tableName, obField, conn) ? $"\"{SqlProtection.ValidateFieldName(obField)}\"" : $"json_extract(data, '$.{SqlProtection.ValidateFieldName(obField)}')";
        }
        sb.Append($" ORDER BY {obExpr} {(options.OrderDesc ? "DESC" : "ASC")} ");
      }
      if (options.Limit != null) sb.Append(" LIMIT " + options.Limit.Value);
      if (options.Offset != null) sb.Append(" OFFSET " + options.Offset.Value);
    }
    sb.Append(";");
    return sb.ToString();
  }

  private string BuildGroupBySql(QueryOptions opts, string modelName, string whereClause, SqliteConnection conn, List<KeyValuePair<string, object>> parameters)
  {
    var groupExprs = new List<string>();
    var aTableForGroup = GetModelTableName(modelName);
    var bTableForGroup = opts.Joins != null && opts.Joins.Any() ? GetModelTableName(opts.Joins.First().TargetModel) : string.Empty;
    var sbg = new StringBuilder();
    string FieldToExpr(string field)
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
    }
    var selectList = new List<string>();
    int gi = 0;
    foreach (var g in opts.GroupBy!)
    {
      var expr = FieldToExpr(g);
      selectList.Add(expr + " AS g" + gi);
      groupExprs.Add(expr);
      gi++;
    }
    if (groupExprs.Count == 0) throw new ArgumentException("GroupBy specified but no valid fields");
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
    else sbg.Append(" WHERE 1=1");
    sbg.Append(" GROUP BY ");
    sbg.Append(string.Join(",", groupExprs.Select(e => e.Replace("COUNT(*)", "COUNTX()"))));
    List<string> havingParts = new();
    if (opts.Having != null && opts.Having.Any())
    {
      var idx = 0;
      foreach (var c in opts.Having)
      {
        string left;
        if (c.Field == "count") left = "COUNT(*)"; else left = FieldToExpr(c.Field);
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
        parameters.Add(new KeyValuePair<string, object>(param, c.Value ?? string.Empty));
        idx++;
      }
    }
    if (havingParts.Count > 0) sbg.Append(" HAVING " + string.Join(" AND ", havingParts));
    // Order by (after HAVING)
    if (!string.IsNullOrWhiteSpace(opts.OrderBy))
    {
      var ob = opts.OrderBy == "count" ? "\"count\"" : FieldToExpr(opts.OrderBy);
      sbg.Append($" ORDER BY {ob} {(opts.OrderDesc ? "DESC" : "ASC")} ");
    }
    if (opts.Limit != null) sbg.Append(" LIMIT " + opts.Limit.Value);
    if (opts.Offset != null) sbg.Append(" OFFSET " + opts.Offset.Value);
    return sbg.ToString();
  }
}
