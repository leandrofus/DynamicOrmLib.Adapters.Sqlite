using Microsoft.Data.Sqlite;
using System;

namespace DynamicOrmLib.Adapters.Sqlite;

public partial class SqliteStoreAdapter
{
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
}
