#include "duckdb/storage/index.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

Index::Index(const string &name, const string &index_type, IndexConstraintType index_constraint_type,
             const vector<column_t> &column_ids, TableIOManager &table_io_manager, AttachedDatabase &db)

    : name(name), index_type(index_type), index_constraint_type(index_constraint_type), column_ids(column_ids),
      table_io_manager(table_io_manager), db(db) {

	if (!Radix::IsLittleEndian()) {
		throw NotImplementedException("indexes are not supported on big endian architectures");
	}
	// create the column id set
	column_id_set.insert(column_ids.begin(), column_ids.end());
}

} // namespace duckdb
