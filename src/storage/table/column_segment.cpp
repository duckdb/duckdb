#include "duckdb/storage/table/column_segment.hpp"
#include <cstring>

using namespace duckdb;
using namespace std;

ColumnSegment::ColumnSegment(TypeId type, ColumnSegmentType segment_type, idx_t start, idx_t count)
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type)), segment_type(segment_type),
      stats(type, type_size) {
}

SegmentStatistics::SegmentStatistics(TypeId type, idx_t type_size) : type(type), type_size(type_size) {
	Reset();
}

template <class T> void initialize_max_min(data_ptr_t min, data_ptr_t max) {
	*((T *)min) = std::numeric_limits<T>::min();
	*((T *)max) = std::numeric_limits<T>::max();
}

void SegmentStatistics::Reset() {
	minimum = unique_ptr<data_t[]>(new data_t[type_size]);
	maximum = unique_ptr<data_t[]>(new data_t[type_size]);
	memset(minimum.get(), 0, type_size);
	memset(maximum.get(), 0, type_size);
	has_null = false;
	max_string_length = 0;
	has_overflow_strings = false;
	switch (type) {
	case TypeId::INT8:
		initialize_max_min<int8_t>(minimum.get(), maximum.get());
		break;
	case TypeId::INT16:
		initialize_max_min<int16_t>(minimum.get(), maximum.get());
		break;
	case TypeId::INT32:
		initialize_max_min<int32_t>(minimum.get(), maximum.get());
		break;
	case TypeId::INT64:
		initialize_max_min<int64_t>(minimum.get(), maximum.get());
		break;
	case TypeId::FLOAT:
		initialize_max_min<float>(minimum.get(), maximum.get());
		break;
	case TypeId::DOUBLE:
		initialize_max_min<double>(minimum.get(), maximum.get());
		break;
	default:
		break;
	}
}
