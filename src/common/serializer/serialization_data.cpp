#include "duckdb/common/serializer/serialization_data.hpp"

// Full definition of CompressionInfo, so the special members touching the stack<const_reference<CompressionInfo>>
// member are instantiated here (where the type is complete) rather than in every TU that reaches the header.
#include "duckdb/function/compression_info.hpp"

namespace duckdb {

SerializationData::SerializationData() = default;
SerializationData::SerializationData(const SerializationData &) = default;
SerializationData::SerializationData(SerializationData &&) noexcept = default;
SerializationData &SerializationData::operator=(const SerializationData &) = default;
SerializationData &SerializationData::operator=(SerializationData &&) noexcept = default;
SerializationData::~SerializationData() = default;

} // namespace duckdb
