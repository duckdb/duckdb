#include "duckdb/parser/pragma_handler.hpp"

#include "duckdb/parser/parsed_data/pragma_info.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {
using namespace std;

string PragmaHandler::HandlePragma(PragmaInfo &pragma) {
	string keyword = StringUtil::Lower(pragma.name);
	if (keyword == "table_info") {
		if (pragma.pragma_type != PragmaType::CALL) {
			throw ParserException("Invalid PRAGMA table_info: expected table name");
		}
		if (pragma.parameters.size() != 1) {
			throw ParserException("Invalid PRAGMA table_info: table_info takes exactly one argument");
		}
		return StringUtil::Format("SELECT * FROM pragma_table_info('%s')", pragma.parameters[0].ToString());
	} else if (keyword == "show_tables") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Invalid PRAGMA show_tables: cannot be called");
		}
		// turn into SELECT name FROM sqlite_master();
		return "SELECT name FROM sqlite_master() ORDER BY name";
	} else if (keyword == "database_list") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Invalid PRAGMA database_list: cannot be called");
		}
		// turn into SELECT * FROM pragma_collations();
		return "SELECT * FROM pragma_database_list() ORDER BY 1";
	} else if (keyword == "collations") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Invalid PRAGMA collations: cannot be called");
		}
		// turn into SELECT * FROM pragma_collations();
		return "SELECT * FROM pragma_collations() ORDER BY 1";
	} else if (keyword == "show") {
		if (pragma.pragma_type != PragmaType::CALL) {
			throw ParserException("Invalid PRAGMA show_tables: expected a function call");
		}
		if (pragma.parameters.size() != 1) {
			throw ParserException("Invalid PRAGMA show_tables: show_tables does not take any arguments");
		}
		// PRAGMA table_info but with some aliases
		return StringUtil::Format(
		    "SELECT name AS \"Field\", type as \"Type\", CASE WHEN \"notnull\" THEN 'NO' ELSE 'YES' END AS \"Null\", "
		    "NULL AS \"Key\", dflt_value AS \"Default\", NULL AS \"Extra\" FROM pragma_table_info('%s')",
		    pragma.parameters[0].ToString());
	} else if (keyword == "version") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Invalid PRAGMA version: cannot be called");
		}
		return "SELECT * FROM pragma_version()";
	} else if (keyword == "show") {
		if (pragma.pragma_type != PragmaType::CALL) {
			throw ParserException("Invalid PRAGMA show_tables: expected a function call");
		}
		if (pragma.parameters.size() != 1) {
			throw ParserException("Invalid PRAGMA show: show takes a single argument");
		}
	} else if (keyword == "import_database") {
		if (pragma.pragma_type != PragmaType::CALL) {
			throw ParserException("Invalid PRAGMA import_database: expected a function call");
		}
		if (pragma.parameters.size() != 1) {
			throw ParserException("Invalid PRAGMA import_database: import_database takes a single argument");
		}
		FileSystem fs;
		string query;
		// read the "shema.sql" and "load.sql" files
		vector<string> files = {"schema.sql", "load.sql"};
		for (auto &file : files) {
			auto file_path = fs.JoinPath(pragma.parameters[0].ToString(), file);
			auto handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
			auto fsize = fs.GetFileSize(*handle);
			auto buffer = unique_ptr<char[]>(new char[fsize]);
			fs.Read(*handle, buffer.get(), fsize);

			query += string(buffer.get(), fsize);
		}
		return query;
	}
	return string();
}

} // namespace duckdb
