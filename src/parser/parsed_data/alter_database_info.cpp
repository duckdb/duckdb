#include "duckdb/parser/parsed_data/alter_database_info.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

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

RenameDatabaseInfo::RenameDatabaseInfo(string catalog_p, string new_name_p, OnEntryNotFound if_not_found)
    : AlterDatabaseInfo(AlterDatabaseType::RENAME_DATABASE, std::move(catalog_p), if_not_found),
      new_name(std::move(new_name_p)) {
}

unique_ptr<AlterInfo> RenameDatabaseInfo::Copy() const {
	return make_uniq<RenameDatabaseInfo>(catalog, new_name, if_not_found);
}

string RenameDatabaseInfo::ToString() const {
	return StringUtil::Format("ALTER DATABASE %s RENAME TO %s", SQLIdentifier(catalog), SQLIdentifier(new_name));
}

} // namespace duckdb
