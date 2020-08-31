#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

string pragma_table_info(ClientContext &context, vector<Value> parameters) {
	return StringUtil::Format("SELECT * FROM pragma_table_info('%s')", parameters[0].ToString());
}

string pragma_show_tables(ClientContext &context, vector<Value> parameters) {
	return "SELECT name FROM sqlite_master() ORDER BY name";
}

string pragma_database_list(ClientContext &context, vector<Value> parameters) {
	return "SELECT * FROM pragma_database_list() ORDER BY 1";
}

string pragma_collations(ClientContext &context, vector<Value> parameters) {
	return "SELECT * FROM pragma_collations() ORDER BY 1";
}

string pragma_show(ClientContext &context, vector<Value> parameters) {
	// PRAGMA table_info but with some aliases
	return StringUtil::Format(
		"SELECT name AS \"Field\", type as \"Type\", CASE WHEN \"notnull\" THEN 'NO' ELSE 'YES' END AS \"Null\", "
		"NULL AS \"Key\", dflt_value AS \"Default\", NULL AS \"Extra\" FROM pragma_table_info('%s')",
		parameters[0].ToString());
}

string pragma_version(ClientContext &context, vector<Value> parameters) {
	return "SELECT * FROM pragma_version()";
}

string pragma_import_database(ClientContext &context, vector<Value> parameters) {
	auto &fs = FileSystem::GetFileSystem(context);
	string query;
	// read the "shema.sql" and "load.sql" files
	vector<string> files = {"schema.sql", "load.sql"};
	for (auto &file : files) {
		auto file_path = fs.JoinPath(parameters[0].ToString(), file);
		auto handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
		auto fsize = fs.GetFileSize(*handle);
		auto buffer = unique_ptr<char[]>(new char[fsize]);
		fs.Read(*handle, buffer.get(), fsize);

		query += string(buffer.get(), fsize);
	}
	return query;}

void PragmaQueries::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(PragmaFunction::PragmaCall("table_info", pragma_table_info, { LogicalType::VARCHAR }));
	set.AddFunction(PragmaFunction::PragmaStatement("show_tables", pragma_show_tables));
	set.AddFunction(PragmaFunction::PragmaStatement("database_list", pragma_database_list));
	set.AddFunction(PragmaFunction::PragmaStatement("collations", pragma_collations));
	set.AddFunction(PragmaFunction::PragmaCall("show", pragma_show, { LogicalType::VARCHAR }));
	set.AddFunction(PragmaFunction::PragmaStatement("version", pragma_version));
	set.AddFunction(PragmaFunction::PragmaCall("import_database", pragma_import_database, { LogicalType::VARCHAR }));
}

}