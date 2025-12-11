# DynamicOrmLib SQLite Adapter

Simple SQLite adapter implementing `IStoreProvider` for DynamicOrmLib.

This adapter is designed as a separate project so it can be distributed as a NuGet package and installed as needed.

Basic usage:

```csharp
var adapter = new SqliteStoreAdapter("Data Source=./dynamic-orm.db");
adapter.Init();
var ctx = new DynamicContext(adapter);
```
