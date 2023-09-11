#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

template <>
void Serializer::WriteValue(const vector<bool> &vec) {
	auto count = vec.size();
	OnListBegin(count);
	for (auto item : vec) {
		WriteValue(item);
	}
	OnListEnd();
}

} // namespace duckdb
