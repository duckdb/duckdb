#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

CreateSchemaInfo::CreateSchemaInfo() : CreateInfo(CatalogType::SCHEMA_ENTRY) {
}

const Identifier &CreateSchemaInfo::SchemaName() const {
	// the new schema is stored in the Schema() slot (the element before the empty trailing name)
	return GetQualifiedName().Schema();
}

const Identifier &CreateSchemaInfo::SchemaCatalog() const {
	static const Identifier EMPTY;
	auto &path = GetQualifiedName().Path();
	// the catalog is the leading component once the path carries [catalog, schema, <empty name>]
	return path.size() >= 3 ? path[0] : EMPTY;
}

vector<Identifier> CreateSchemaInfo::ParentSchemas() const {
	auto &path = GetQualifiedName().Path();
	vector<Identifier> result;
	// everything between the catalog and the new schema is a parent schema (the last two slots are
	// the new schema and the empty trailing name)
	for (idx_t i = 1; i + 2 < path.size(); i++) {
		result.push_back(path[i]);
	}
	return result;
}

bool CreateSchemaInfo::IsNested() const {
	return GetQualifiedName().Path().size() > 3;
}

unique_ptr<CreateInfo> CreateSchemaInfo::Copy() const {
	auto result = make_uniq<CreateSchemaInfo>();
	CopyProperties(*result);
	return std::move(result);
}

string CreateSchemaInfo::ToString() const {
	string qualified;
	auto &path = GetQualifiedName().Path();
	// the last element is the (empty) trailing name slot - the schema itself is the element before it
	for (idx_t i = 0; i + 1 < path.size(); i++) {
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
