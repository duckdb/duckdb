#include "duckdb/common/serializer/serialization_data.hpp"

// Full definition of CompressionInfo, so the stack<const_reference<CompressionInfo>> member destructor is instantiated
// here (where the type is complete) rather than in every TU that merely reaches serialization_data.hpp.
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

SerializationData::~SerializationData() = default;

} // namespace duckdb
