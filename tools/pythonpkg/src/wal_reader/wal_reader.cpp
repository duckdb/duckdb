#include "duckdb_python/wal_reader.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

void DefineParseInfoTypes(py::handle &m) {
	auto parse_info_type = py::enum_<ParseInfoType>(m, "ParseInfoType", "Parse info types for DuckDB")
	                           .value("ALTER_INFO", ParseInfoType::ALTER_INFO)
	                           .value("ATTACH_INFO", ParseInfoType::ATTACH_INFO)
	                           .value("COPY_INFO", ParseInfoType::COPY_INFO)
	                           .value("CREATE_INFO", ParseInfoType::CREATE_INFO)
	                           .value("CREATE_SECRET_INFO", ParseInfoType::CREATE_SECRET_INFO)
	                           .value("DETACH_INFO", ParseInfoType::DETACH_INFO)
	                           .value("DROP_INFO", ParseInfoType::DROP_INFO)
	                           .value("BOUND_EXPORT_DATA", ParseInfoType::BOUND_EXPORT_DATA)
	                           .value("LOAD_INFO", ParseInfoType::LOAD_INFO)
	                           .value("PRAGMA_INFO", ParseInfoType::PRAGMA_INFO)
	                           .value("SHOW_SELECT_INFO", ParseInfoType::SHOW_SELECT_INFO)
	                           .value("TRANSACTION_INFO", ParseInfoType::TRANSACTION_INFO)
	                           .value("VACUUM_INFO", ParseInfoType::VACUUM_INFO)
	                           .value("COMMENT_ON_INFO", ParseInfoType::COMMENT_ON_INFO)
	                           .value("COMMENT_ON_COLUMN_INFO", ParseInfoType::COMMENT_ON_COLUMN_INFO)
	                           .value("COPY_DATABASE_INFO", ParseInfoType::COPY_DATABASE_INFO)
	                           .value("UPDATE_EXTENSIONS_INFO", ParseInfoType::UPDATE_EXTENSIONS_INFO)
	                           .value("SEQUENCE_VALUE_INFO", ParseInfoType::SEQUENCE_VALUE_INFO)
	                           .value("TABLE_DATA_INFO", ParseInfoType::TABLE_DATA_INFO)
	                           .value("VERSION_INFO", ParseInfoType::VERSION_INFO)
	                           .value("FLUSH_INFO", ParseInfoType::FLUSH_INFO)
	                           .value("CHECKPOINT_INFO", ParseInfoType::CHECKPOINT_INFO);

	py::class_<ParseInfo>(m, "ParseInfo", "Base class for parse information")
	    .def(py::init<ParseInfoType>(), py::arg("info_type"))
	    .def_readonly("info_type", &ParseInfo::info_type)
	    .def_static("QualifierToString", &ParseInfo::QualifierToString, py::arg("catalog"), py::arg("schema"),
	                py::arg("name"))
	    .def_static("TypeToString", &ParseInfo::TypeToString, py::arg("type"));
}

void DuckDBPyWalReader::Initialize(py::module_ &parent) {
	auto m = parent.def_submodule("wal_reader", "This module contains classes and methods related to WAL reader");

	DefineParseInfoTypes(m);
}

} // namespace duckdb
