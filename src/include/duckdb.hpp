// C++ Interface to DuckDB

#pragma once

class DuckDB;
class DuckDBConnection;
class DuckDBResult;

class DuckDB {
  public:
	DuckDB(const char *path);
};

class DuckDBConnection {
  public:
	DuckDBConnection(DuckDB &database);

	DuckDBResult Query(const char *query);
};

class DuckDBResult {};
