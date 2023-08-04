#include "duckdb/common/serializer/format_serializer.hpp"

namespace duckdb {

template <>
void FormatSerializer::WriteValue(const vector<bool> &vec) {
	auto count = vec.size();
	OnListBegin(count);
	for (auto item : vec) {
		WriteValue(item);
	}
	OnListEnd(count);
}

} // namespace duckdb
