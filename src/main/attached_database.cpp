#include "duckdb/main/attached_database.hpp"

namespace duckdb {

AttachedDatabase::AttachedDatabase(DatabaseInstance &db) {
	this->catalog = make_unique<Catalog>(db);
}

AttachedDatabase::~AttachedDatabase() {
}

} // namespace duckdb
