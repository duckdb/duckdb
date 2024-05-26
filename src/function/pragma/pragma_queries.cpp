#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/export_statement.hpp"

namespace duckdb {

string PragmaTableInfo(ClientContext &context, const FunctionParameters &parameters) {
	return StringUtil::Format("SELECT * FROM pragma_table_info(%s);",
	                          KeywordHelper::WriteQuoted(parameters.values[0].ToString(), '\''));
}

string PragmaShowTables() {
	// clang-format off
	return R"EOF(
	with "tables" as
	(
		SELECT table_name as "name"
		FROM duckdb_tables
		where in_search_path(database_name, schema_name)
	), "views" as
	(
		SELECT view_name as "name"
		FROM duckdb_views
		where in_search_path(database_name, schema_name)
	), db_objects as
	(
		SELECT "name" FROM "tables"
		UNION ALL
		SELECT "name" FROM "views"
	)
	SELECT "name"
	FROM db_objects
	ORDER BY "name";)EOF";
	// clang-format on
}
string PragmaShowTables(ClientContext &context, const FunctionParameters &parameters) {
	return PragmaShowTables();
}

string PragmaShowTablesExpanded() {
	return R"(
	SELECT
		t.database_name AS database,
		t.schema_name AS schema,
		t.table_name AS name,
		LIST(c.column_name order by c.column_index) AS column_names,
		LIST(c.data_type order by c.column_index) AS column_types,
		FIRST(t.temporary) AS temporary,
	FROM duckdb_tables t
	JOIN duckdb_columns c
	USING (table_oid)
	GROUP BY database, schema, name

	UNION ALL

	SELECT
		v.database_name AS database,
		v.schema_name AS schema,
		v.view_name AS name,
		LIST(c.column_name order by c.column_index) AS column_names,
		LIST(c.data_type order by c.column_index) AS column_types,
		FIRST(v.temporary) AS temporary,
	FROM duckdb_views v
	JOIN duckdb_columns c
	ON (v.view_oid=c.table_oid)
	GROUP BY database, schema, name

	ORDER BY database, schema, name
	)";
}

string PragmaShowTablesExpanded(ClientContext &context, const FunctionParameters &parameters) {
	return PragmaShowTablesExpanded();
}

string PragmaShowDatabases() {
	return "SELECT database_name FROM duckdb_databases() WHERE NOT internal ORDER BY database_name;";
}

string PragmaShowDatabases(ClientContext &context, const FunctionParameters &parameters) {
	return PragmaShowDatabases();
}

string PragmaAllProfiling(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_last_profiling_output() JOIN pragma_detailed_profiling_output() ON "
	       "(pragma_last_profiling_output.operator_id);";
}

string PragmaDatabaseList(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_database_list;";
}

string PragmaCollations(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_collations() ORDER BY 1;";
}

string PragmaFunctionsQuery(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT function_name AS name, upper(function_type) AS type, parameter_types AS parameters, varargs, "
	       "return_type, has_side_effects AS side_effects"
	       " FROM duckdb_functions()"
	       " WHERE function_type IN ('scalar', 'aggregate')"
	       " ORDER BY 1;";
}

string PragmaShow(const string &table_name) {
	return StringUtil::Format("SELECT * FROM pragma_show(%s);", KeywordHelper::WriteQuoted(table_name, '\''));
}

string PragmaShow(ClientContext &context, const FunctionParameters &parameters) {
	return PragmaShow(parameters.values[0].ToString());
}

string PragmaVersion(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_version();";
}

string PragmaPlatform(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_platform();";
}

string PragmaImportDatabase(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Import is disabled through configuration");
	}
	auto &fs = FileSystem::GetFileSystem(context);

	string final_query;
	// read the "shema.sql" and "load.sql" files
	vector<string> files = {"schema.sql", "load.sql"};
	for (auto &file : files) {
		auto file_path = fs.JoinPath(parameters.values[0].ToString(), file);
		auto handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
		auto fsize = fs.GetFileSize(*handle);
		auto buffer = make_unsafe_uniq_array<char>(UnsafeNumericCast<size_t>(fsize));
		fs.Read(*handle, buffer.get(), fsize);
		auto query = string(buffer.get(), UnsafeNumericCast<uint32_t>(fsize));
		// Replace the placeholder with the path provided to IMPORT
		if (file == "load.sql") {
			Parser parser;
			parser.ParseQuery(query);
			auto copy_statements = std::move(parser.statements);
			query.clear();
			for (auto &statement_p : copy_statements) {
				D_ASSERT(statement_p->type == StatementType::COPY_STATEMENT);
				auto &statement = statement_p->Cast<CopyStatement>();
				auto &info = *statement.info;
				auto file_name = fs.ExtractName(info.file_path);
				info.file_path = fs.JoinPath(parameters.values[0].ToString(), file_name);
				query += statement.ToString() + ";";
			}
		}
		final_query += query;
	}
	return final_query;
}

string PragmaCopyDatabase(ClientContext &context, const FunctionParameters &parameters) {
	string copy_stmt = "COPY FROM DATABASE ";
	copy_stmt += KeywordHelper::WriteOptionallyQuoted(parameters.values[0].ToString());
	copy_stmt += " TO ";
	copy_stmt += KeywordHelper::WriteOptionallyQuoted(parameters.values[1].ToString());
	string final_query;
	final_query += copy_stmt + " (SCHEMA);\n";
	final_query += copy_stmt + " (DATA);";
	return final_query;
}

string PragmaDatabaseSize(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_database_size();";
}

string PragmaStorageInfo(ClientContext &context, const FunctionParameters &parameters) {
	return StringUtil::Format("SELECT * FROM pragma_storage_info('%s');", parameters.values[0].ToString());
}

string PragmaMetadataInfo(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_metadata_info();";
}

string PragmaUserAgent(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_user_agent()";
}

void PragmaQueries::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(PragmaFunction::PragmaCall("table_info", PragmaTableInfo, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("storage_info", PragmaStorageInfo, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("metadata_info", PragmaMetadataInfo, {}));
	set.AddFunction(PragmaFunction::PragmaStatement("show_tables", PragmaShowTables));
	set.AddFunction(PragmaFunction::PragmaStatement("show_tables_expanded", PragmaShowTablesExpanded));
	set.AddFunction(PragmaFunction::PragmaStatement("show_databases", PragmaShowDatabases));
	set.AddFunction(PragmaFunction::PragmaStatement("database_list", PragmaDatabaseList));
	set.AddFunction(PragmaFunction::PragmaStatement("collations", PragmaCollations));
	set.AddFunction(PragmaFunction::PragmaCall("show", PragmaShow, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("version", PragmaVersion));
	set.AddFunction(PragmaFunction::PragmaStatement("platform", PragmaPlatform));
	set.AddFunction(PragmaFunction::PragmaStatement("database_size", PragmaDatabaseSize));
	set.AddFunction(PragmaFunction::PragmaStatement("functions", PragmaFunctionsQuery));
	set.AddFunction(PragmaFunction::PragmaCall("import_database", PragmaImportDatabase, {LogicalType::VARCHAR}));
	set.AddFunction(
	    PragmaFunction::PragmaCall("copy_database", PragmaCopyDatabase, {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("all_profiling_output", PragmaAllProfiling));
	set.AddFunction(PragmaFunction::PragmaStatement("user_agent", PragmaUserAgent));
}

} // namespace duckdb
