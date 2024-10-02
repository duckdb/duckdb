#include "duckdb/storage/index.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

Index::Index(const vector<column_t> &column_ids, TableIOManager &table_io_manager, AttachedDatabase &db)

    : column_ids(column_ids), table_io_manager(table_io_manager), db(db) {

	if (!Radix::IsLittleEndian()) {
		throw NotImplementedException("indexes are not supported on big endian architectures");
	}
	// create the column id set
	column_id_set.insert(column_ids.begin(), column_ids.end());
}

} // namespace duckdb
