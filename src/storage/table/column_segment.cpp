#include "duckdb/storage/table/column_segment.hpp"
#include <cstring>

using namespace duckdb;
using namespace std;

ColumnSegment::ColumnSegment(TypeId type, ColumnSegmentType segment_type, idx_t start, idx_t count)
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type)), segment_type(segment_type),
      stats(type, type_size) {
}

ColumnSegment::ColumnSegment(TypeId type, ColumnSegmentType segment_type, idx_t start, idx_t count, data_t stats_min[],
                             data_t stats_max[])
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type)), segment_type(segment_type),
      stats(type, type_size, stats_min, stats_max) {
}

SegmentStatistics::SegmentStatistics(TypeId type, idx_t type_size) : type(type), type_size(type_size) {
	Reset();
}

template <class T>
static void set_min_max(data_t min_value_p[], data_t max_value_p[], data_ptr_t min_p, data_ptr_t max_p) {
	memcpy(min_p, min_value_p, sizeof(T));
	memcpy(max_p, max_value_p, sizeof(T));
}

SegmentStatistics::SegmentStatistics(TypeId type, idx_t type_size, data_t stats_min[], data_t stats_max[])
    : type(type), type_size(type_size) {
	Reset();
	switch (type) {
	case TypeId::INT8: {
		set_min_max<int8_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case TypeId::INT16: {
		set_min_max<int16_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case TypeId::INT32: {
		set_min_max<int32_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case TypeId::INT64: {
		set_min_max<int64_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case TypeId::FLOAT: {
		set_min_max<float>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case TypeId::DOUBLE: {
		set_min_max<double>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case TypeId::VARCHAR: {
		set_min_max<char[8]>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}

	default:
		break;
	}
}

template <class T> void initialize_max_min(data_ptr_t min, data_ptr_t max) {
	*((T *)min) = std::numeric_limits<T>::max();
	*((T *)max) = std::numeric_limits<T>::min();
}

void SegmentStatistics::Reset() {
	idx_t min_max_size = type_size > 8 ? 8 : type_size;
	minimum = unique_ptr<data_t[]>(new data_t[min_max_size]);
	maximum = unique_ptr<data_t[]>(new data_t[min_max_size]);
	has_null = false;
	max_string_length = 0;
	has_overflow_strings = false;
	char padding = '\0';
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
	case TypeId::VARCHAR: {
		//! This marks the min/max was not initialized
		char marker = '1';
		memset(minimum.get(), padding, min_max_size);
		memset(maximum.get(), padding, min_max_size);
		minimum.get()[1] = marker;
		maximum.get()[1] = marker;
		break;
	}
	default:
		break;
	}
}
