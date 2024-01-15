#include "connect.hpp"
#include "duckdb/main/db_instance_cache.hpp"

using namespace duckdb;

//! The database instance cache, used so that multiple connections to the same file point to the same database object
duckdb::DBInstanceCache instance_cache;

bool Connect::SetSuccessWithInfo(SQLRETURN ret) {
	if (SQL_SUCCEEDED(ret)) {
		if (ret == SQL_SUCCESS_WITH_INFO) {
			success_with_info = true;
		}
		return true;
	}
	return false;
}

static bool FindSubstrInSubstr(const std::string &s1, const std::string &s2) {
	std::string longest = s1.length() >= s2.length() ? s1 : s2;
	std::string shortest = s1.length() >= s2.length() ? s2 : s1;

	idx_t longest_match = 0;
	for (int i = 0; i < longest.length(); i++) {
		for (int j = 0; j < shortest.length(); j++) {
			if (longest[i] == shortest[j]) {
				idx_t match = 1;
				while (i + match < longest.length() && j + match < shortest.length() &&
				       longest[i + match] == shortest[j + match]) {
					match++;
				}
				if (match > longest_match) {
					longest_match = match;
				}
			}
		}
	}

	if (longest_match > 4) {
		return true;
	}
	return false;
}

bool Connect::FindSimilar(const std::string &input, std::string &match) {
	duckdb::vector<std::string> keys;
	keys.reserve(conn_str_keynames.size());
	for (auto &key_pair : conn_str_keynames) {
		if (input.find(key_pair.second) != std::string::npos || key_pair.second.find(input) != std::string::npos ||
		    FindSubstrInSubstr(input, key_pair.second)) {
			match = key_pair.second;
			return true;
		}
		keys.push_back(key_pair.second);
	}

	auto result = duckdb::StringUtil::TopNLevenshtein(keys, input);
	return false;
}

SQLRETURN Connect::FindMatchingKey(const std::string &input, ODBCConnStrKey &key) {
	for (auto &key_pair : conn_str_keynames) {
		if (key_pair.second == input) {
			key = key_pair.first;
			return SQL_SUCCESS;
		}
	}

	std::string match;
	// If the input doesn't match a keyname, find a similar keyname
	if (FindSimilar(input, match)) {
		// If there is a similar keyname, populate a diagnostic record with a suggestion
		return duckdb::SetDiagnosticRecord(dbc, SQL_SUCCESS_WITH_INFO, "SQLDriverConnect",
		                                   "Invalid keyword: '" + input + "', Did you mean '" + match + "'?",
		                                   SQLStateType::ST_01S09, "");
	}
	return duckdb::SetDiagnosticRecord(dbc, SQL_SUCCESS_WITH_INFO, "SQLDriverConnect", "Invalid keyword",
	                                   SQLStateType::ST_01S09, "");
}

SQLRETURN Connect::FindKeyValPair(const std::string &row) {
	ODBCConnStrKey key;

	size_t val_pos = row.find(KEY_VAL_DEL);
	if (val_pos == std::string::npos) {
		// an equal '=' char must be present (syntax error)
		return (duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", "Invalid connection string",
		                                    SQLStateType::ST_HY000, ""));
	}

	SQLRETURN ret = FindMatchingKey(duckdb::StringUtil::Lower(row.substr(0, val_pos)), key);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	SetVal(key, row.substr(val_pos + 1));
	return SQL_SUCCESS;
}

SQLRETURN Connect::SetVal(ODBCConnStrKey key, const std::string &val) {
	if (CheckSet(key)) {
		return SQL_SUCCESS;
	}
	return (this->*handle_functions.at(key))(val);
}

SQLRETURN Connect::ParseInputStr() {
	size_t row_pos;
	std::string row;

	if (input_str.empty()) {
		return SQL_SUCCESS;
	}

	while ((row_pos = input_str.find(ROW_DEL)) != std::string::npos) {
		row = input_str.substr(0, row_pos);
		SQLRETURN ret = FindKeyValPair(row);
		if (ret != SQL_SUCCESS) {
			return ret;
		}
		input_str.erase(0, row_pos + 1);
	}

	if (input_str.empty()) {
		return SQL_SUCCESS;
	}

	SQLRETURN ret = FindKeyValPair(input_str);
	if (ret != SQL_SUCCESS) {
		return ret;
	}
	return SQL_SUCCESS;
}

#if defined ODBC_LINK_ODBCINST || defined WIN32
SQLRETURN Connect::ReadFromIniFile() {
	duckdb::unique_ptr<duckdb::FileSystem> fs = duckdb::FileSystem::CreateLocal();
	std::string home_directory = fs->GetHomeDirectory();

	std::string odbc_file = home_directory + "/.odbc.ini";

	if (!fs->FileExists(odbc_file)) {
		return SQL_SUCCESS;
	}

	if (dbc->dsn.empty()) {
		return SQL_SUCCESS;
	}

	auto converted_odbc_file = OdbcUtils::ConvertStringToLPCSTR(odbc_file);
	auto converted_dsn = OdbcUtils::ConvertStringToLPCSTR(dbc->dsn);
	for (auto &key_pair : conn_str_keynames) {
		if (CheckSet(key_pair.first)) {
			continue;
		}
		const int max_val_len = 256;
		char char_val[max_val_len];
		auto converted_key = key_pair.second.c_str();
		int read_size =
		    SQLGetPrivateProfileString(converted_dsn, converted_key, "", char_val, max_val_len, converted_odbc_file);
		if (read_size == 0) {
			continue;
		} else if (read_size < 0) {
			return duckdb::SetDiagnosticRecord(dbc, SQL_ERROR, "SQLDriverConnect", "Error reading from .odbc.ini",
			                                   SQLStateType::ST_01S09, "");
		}
		SQLRETURN ret = SetVal(key_pair.first, string(char_val));
		if (ret != SQL_SUCCESS) {
			return ret;
		}
	}
	return SQL_SUCCESS;
}
#endif

SQLRETURN Connect::HandleDsn(const string &val) {
	dbc->dsn = val;
	set_keys[DSN] = true;
	return SQL_SUCCESS;
}

SQLRETURN Connect::HandleDatabase(const string &val) {
	std::string new_db_name = val;
	// given preference for the connection attribute
	if (!dbc->sql_attr_current_catalog.empty() && new_db_name.empty()) {
		new_db_name = dbc->sql_attr_current_catalog;
	}

	dbc->SetDatabaseName(new_db_name);
	set_keys[DATABASE] = true;
	return SQL_SUCCESS;
}

SQLRETURN Connect::HandleAllowUnsignedExtensions(const string &val) {
	if (duckdb::StringUtil::Lower(val) == "true") {
		config.options.allow_unsigned_extensions = true;
	}
	set_keys[UNSIGNED] = true;
	return SQL_SUCCESS;
}

SQLRETURN Connect::HandleAccessMode(const string &val) {
	std::string val_lower = duckdb::StringUtil::Lower(val);
	if (val_lower == "read_only") {
		dbc->sql_attr_access_mode = SQL_MODE_READ_ONLY;
	} else if (val_lower == "read_write") {
		dbc->sql_attr_access_mode = SQL_MODE_READ_WRITE;
	} else {
		return duckdb::SetDiagnosticRecord(dbc, SQL_SUCCESS_WITH_INFO, "SQLDriverConnect",
		                                   "Invalid access mode: '" + val +
		                                       "'.  Accepted values are 'READ_ONLY' and 'READ_WRITE'",
		                                   SQLStateType::ST_01S09, "");
	}
	config.options.access_mode = OdbcUtils::ConvertSQLAccessModeToDuckDBAccessMode(dbc->sql_attr_access_mode);
	set_keys[ACCESS_MODE] = true;
	return SQL_SUCCESS;
}

SQLRETURN Connect::HandleCustomUserAgent(const string &val) {
	if (!val.empty()) {
		config.options.custom_user_agent = val;
	}
	set_keys[CUSTOM_USER_AGENT] = true;
	return SQL_SUCCESS;
}

SQLRETURN Connect::SetConnection() {
#if defined ODBC_LINK_ODBCINST || defined WIN32
	ReadFromIniFile();
#endif

	std::string database = dbc->GetDatabaseName();
	config.SetOptionByName("duckdb_api", "odbc");

	bool cache_instance = database != ":memory:" && !database.empty();
	dbc->env->db = instance_cache.GetOrCreateInstance(database, config, cache_instance);

	if (!dbc->conn) {
		dbc->conn = duckdb::make_uniq<duckdb::Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}
	return SQL_SUCCESS;
}
