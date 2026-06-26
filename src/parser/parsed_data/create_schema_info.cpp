#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

CreateSchemaInfo::CreateSchemaInfo() : CreateInfo(CatalogType::SCHEMA_ENTRY) {
}

const Identifier &CreateSchemaInfo::SchemaName() const {
	return GetQualifiedName().SchemaPath().back();
}

const Identifier &CreateSchemaInfo::SchemaCatalog() const {
	static const Identifier EMPTY;
	auto &path = GetQualifiedName().SchemaPath();
	// the catalog is the leading component, but only when the path also carries the schema name
	return path.size() >= 2 ? path[0] : EMPTY;
}

vector<Identifier> CreateSchemaInfo::ParentSchemas() const {
	auto &path = GetQualifiedName().SchemaPath();
	vector<Identifier> result;
	// everything between the (optional) catalog and the new schema is a parent schema
	for (idx_t i = 1; i + 1 < path.size(); i++) {
		result.push_back(path[i]);
	}
	return result;
}

bool CreateSchemaInfo::IsNested() const {
	return GetQualifiedName().SchemaPath().size() > 2;
}

unique_ptr<CreateInfo> CreateSchemaInfo::Copy() const {
	auto result = make_uniq<CreateSchemaInfo>();
	CopyProperties(*result);
	return std::move(result);
}

string CreateSchemaInfo::ToString() const {
	string qualified;
	auto &path = GetQualifiedName().SchemaPath();
	for (idx_t i = 0; i < path.size(); i++) {
		// hide the temp catalog when it is the leading (catalog) component
		if (i == 0 && path.size() >= 2 && path[i] == TEMP_CATALOG) {
			continue;
		}
		if (!qualified.empty()) {
			qualified += ".";
		}
		qualified += SQLIdentifier(path[i]);
	}

	string ret = "";
	switch (on_conflict) {
	case OnCreateConflict::ALTER_ON_CONFLICT: {
		ret += "CREATE SCHEMA " + qualified + " ON CONFLICT INSERT OR REPLACE;";
		break;
	}
	case OnCreateConflict::IGNORE_ON_CONFLICT: {
		ret += "CREATE SCHEMA IF NOT EXISTS " + qualified + ";";
		break;
	}
	case OnCreateConflict::REPLACE_ON_CONFLICT: {
		ret += "CREATE OR REPLACE SCHEMA " + qualified + ";";
		break;
	}
	case OnCreateConflict::ERROR_ON_CONFLICT: {
		ret += "CREATE SCHEMA " + qualified + ";";
		break;
	}
	}
	return ret;
}

} // namespace duckdb
