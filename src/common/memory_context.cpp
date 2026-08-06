#include "duckdb/common/memory_context.hpp"

#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

string MemoryContextId::ToString() const {
	return UUID::ToString(uuid);
}

} // namespace duckdb
