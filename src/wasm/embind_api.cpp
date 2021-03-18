#include <emscripten/bind.h>
#include <duckdb.hpp>

using namespace emscripten;

std::string DuckDBSourceId() {
  return duckdb::DuckDB::SourceID();
}

std::string DuckDBLibraryVersion() {
  return duckdb::DuckDB::LibraryVersion();
}

uint32_t QueryResultColumnCount(duckdb::QueryResult& self) {
  return self.ColumnCount();
}

std::string QueryResultGetColumnName(duckdb::QueryResult& self, uint32_t col_idx) {
  return self.names[col_idx];
}

duckdb::LogicalTypeId QueryResultGetColumnTypeId(duckdb::QueryResult& self, uint32_t col_idx) {
  return self.types[col_idx].id();
}

uint32_t DataChunkColumnCount(duckdb::DataChunk& self) {
  return self.ColumnCount();
}

uint32_t DataChunkSize(duckdb::DataChunk& self) {
  return self.size();
}

duckdb::Value DataChunkGetValue(duckdb::DataChunk& self, uint32_t col_idx, uint32_t index) {
  return self.GetValue(col_idx, index);
}

int32_t DataChunkGetValueAsInteger(const duckdb::DataChunk& self, uint32_t col_idx, uint32_t index) {
  return self.GetValue(col_idx, index).GetValue<int32_t>();
}

double DataChunkGetValueAsNumber(const duckdb::DataChunk& self, uint32_t col_idx, uint32_t index) {
  return self.GetValue(col_idx, index).GetValue<double>();
}

std::string DataChunkGetValueAsString(const duckdb::DataChunk& self, uint32_t col_idx, uint32_t index) {
  return self.GetValue(col_idx, index).GetValue<std::string>();
}

EMSCRIPTEN_BINDINGS(duckdb_api) {
  register_vector<duckdb::LogicalTypeId>("VectorLogicalTypeId");

  class_<duckdb::DuckDB>("DuckDB")
    .constructor<>()
    .class_function("SourceID", &DuckDBSourceId)
    .class_function("LibraryVersion", &DuckDBLibraryVersion)
    ;

  class_<duckdb::Connection>("Connection")
    .constructor<duckdb::DuckDB&>()
    // .function("query", select_overload<std::unique_ptr<duckdb::MaterializedQueryResult>(const std::string&)>(&duckdb::Connection::Query))
    .function("sendQuery", &duckdb::Connection::SendQuery)
    ;

  class_<duckdb::QueryResult>("QueryResult")
    .function("isSuccess", optional_override([](duckdb::QueryResult& self) { return self.success; }))
    .function("getError", optional_override([](duckdb::QueryResult& self) { return self.error; }))
    .function("ToString", &duckdb::QueryResult::ToString)
    .function("columnCount", &QueryResultColumnCount)
    .function("fetch", &duckdb::QueryResult::Fetch)
    .function("getColumnName", &QueryResultGetColumnName)
    .function("getColumnTypeId", &QueryResultGetColumnTypeId)
    ;

  class_<duckdb::DataChunk>("DataChunk")
    .function("size", &DataChunkSize)
    .function("ColumnCount", &DataChunkColumnCount)
    .function("GetValue", &DataChunkGetValue)
    .function("getValueAsInteger", &DataChunkGetValueAsInteger)
    .function("getValueAsNumber", &DataChunkGetValueAsNumber)
    .function("getValueAsString", &DataChunkGetValueAsString)
    ;

  class_<duckdb::Value>("Value")
    .function("type", &duckdb::Value::type)
    .function("getInteger", optional_override([](duckdb::Value& self) { return self.value_.integer; }))
    ;

  class_<duckdb::LogicalType>("LogicalType")
    .function("id", &duckdb::LogicalType::id)
    ;

  enum_<duckdb::LogicalTypeId>("LogicalTypeId")
    .value("INVALID", duckdb::LogicalTypeId::INVALID)
    .value("SQLNULL", duckdb::LogicalTypeId::SQLNULL)
    .value("UNKNOWN", duckdb::LogicalTypeId::UNKNOWN)
    .value("ANY", duckdb::LogicalTypeId::ANY)
    .value("BOOLEAN", duckdb::LogicalTypeId::BOOLEAN)
    .value("TINYINT", duckdb::LogicalTypeId::TINYINT)
    .value("SMALLINT", duckdb::LogicalTypeId::SMALLINT)
    .value("INTEGER", duckdb::LogicalTypeId::INTEGER)
    .value("BIGINT", duckdb::LogicalTypeId::BIGINT)
    .value("DATE", duckdb::LogicalTypeId::DATE)
    .value("TIME", duckdb::LogicalTypeId::TIME)
    .value("TIMESTAMP", duckdb::LogicalTypeId::TIMESTAMP)
    .value("DECIMAL", duckdb::LogicalTypeId::DECIMAL)
    .value("FLOAT", duckdb::LogicalTypeId::FLOAT)
    .value("DOUBLE", duckdb::LogicalTypeId::DOUBLE)
    .value("CHAR", duckdb::LogicalTypeId::CHAR)
    .value("VARCHAR", duckdb::LogicalTypeId::VARCHAR)
    .value("BLOB", duckdb::LogicalTypeId::BLOB)
    .value("INTERVAL", duckdb::LogicalTypeId::INTERVAL)
    .value("UTINYINT", duckdb::LogicalTypeId::UTINYINT)
    .value("USMALLINT", duckdb::LogicalTypeId::USMALLINT)
    .value("UINTEGER", duckdb::LogicalTypeId::UINTEGER)
    .value("UBIGINT", duckdb::LogicalTypeId::UBIGINT)
    .value("HUGEINT", duckdb::LogicalTypeId::HUGEINT)
    .value("POINTER", duckdb::LogicalTypeId::POINTER)
    .value("HASH", duckdb::LogicalTypeId::HASH)
    .value("STRUCT", duckdb::LogicalTypeId::STRUCT)
    .value("LIST", duckdb::LogicalTypeId::LIST)
    .value("MAP", duckdb::LogicalTypeId::MAP)
    ;
}
