#include "duckdb/parser/parsed_data/alter_database_info.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

AlterDatabaseInfo::AlterDatabaseInfo(AlterDatabaseType alter_database_type)
    : AlterInfo(AlterType::ALTER_DATABASE, string(), "", "", OnEntryNotFound::THROW_EXCEPTION),
      alter_database_type(alter_database_type) {
}

AlterDatabaseInfo::AlterDatabaseInfo(AlterDatabaseType alter_database_type, string catalog_p,
                                     OnEntryNotFound if_not_found)
    : AlterInfo(AlterType::ALTER_DATABASE, std::move(catalog_p), "", "", if_not_found),
      alter_database_type(alter_database_type) {
}

AlterDatabaseInfo::~AlterDatabaseInfo() {
}

CatalogType AlterDatabaseInfo::GetCatalogType() const {
	return CatalogType::DATABASE_ENTRY;
}

RenameDatabaseInfo::RenameDatabaseInfo() : AlterDatabaseInfo(AlterDatabaseType::RENAME_DATABASE) {
}

RenameDatabaseInfo::RenameDatabaseInfo(string catalog_p, string new_name_p, OnEntryNotFound if_not_found)
    : AlterDatabaseInfo(AlterDatabaseType::RENAME_DATABASE, std::move(catalog_p), if_not_found),
      new_name(std::move(new_name_p)) {
}

unique_ptr<AlterInfo> RenameDatabaseInfo::Copy() const {
	return make_uniq<RenameDatabaseInfo>(catalog, new_name, if_not_found);
}

string RenameDatabaseInfo::ToString() const {
	string result;
	result = "ALTER DATABASE ";
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		result += "IF EXISTS ";
	}
	result += StringUtil::Format("%s RENAME TO %s", SQLIdentifier(catalog), SQLIdentifier(new_name));
	return result;
}

} // namespace duckdb
