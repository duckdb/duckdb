#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

//! PRIMARY KEY, UNIQUE and FOREIGN KEY constraints are backed by indexes
static bool HasIndexConstraint(const CreateTableInfo &info) {
	for (auto &constraint : info.constraints) {
		if (constraint->type != ConstraintType::NOT_NULL && constraint->type != ConstraintType::CHECK) {
			return true;
		}
	}
	return false;
}

DatabaseModificationType CreateInfo::GetModifications() const {
	switch (type) {
	case CatalogType::TABLE_ENTRY: {
		auto &table_info = Cast<CreateTableInfo>();
		// FIXME: temporary, until tables with indexes run alongside checkpoints as well
		if (HasIndexConstraint(table_info)) {
			return DatabaseModificationType::CREATE_CATALOG_ENTRY;
		}
		// no checkpoint lock, but like INSERT ... SELECT, CREATE TABLE AS keeps checkpoints from vacuuming meanwhile
		if (table_info.query) {
			return DatabaseModificationType::INSERT_DATA;
		}
		return DatabaseModificationType();
	}
	case CatalogType::INDEX_ENTRY:
		return DatabaseModificationType::CREATE_CATALOG_ENTRY;
	default:
		return DatabaseModificationType();
	}
}

void CreateInfo::CopyProperties(CreateInfo &other) const {
	other.type = type;
	other.SetQualifiedName(GetQualifiedName());
	other.on_conflict = on_conflict;
	other.temporary = temporary;
	other.internal = internal;
	other.extension_name = extension_name;
	other.sql = sql;
	other.dependencies = dependencies;
	other.comment = comment;
	other.tags = tags;
}

unique_ptr<AlterInfo> CreateInfo::GetAlterInfo() const {
	throw NotImplementedException("GetAlterInfo not implemented for this type");
}

void CreateInfo::StripCatalogQualification() {
	qualified_name.StripCatalog();
}

string CreateInfo::QualifiedNameToString() const {
	if (!temporary) {
		return qualified_name.ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA);
	}
	// for temporary entries the catalog is implied, so it is omitted from the rendered name
	auto name = qualified_name;
	name.StripCatalog();
	return name.ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA);
}

string CreateInfo::GetCreatePrefix(const string &entry) const {
	string prefix = "CREATE";
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		prefix += " OR REPLACE";
	}
	if (temporary) {
		prefix += " TEMP";
	}
	prefix += " " + entry + " ";

	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		prefix += " IF NOT EXISTS ";
	}
	return prefix;
}

} // namespace duckdb
