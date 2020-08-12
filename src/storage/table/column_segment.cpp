#include "duckdb/storage/table/column_segment.hpp"
#include <cstring>

namespace duckdb {
using namespace std;

ColumnSegment::ColumnSegment(PhysicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count)
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type)), segment_type(segment_type),
      stats(type, type_size) {
}

ColumnSegment::ColumnSegment(PhysicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count, data_t stats_min[],
                             data_t stats_max[])
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type)), segment_type(segment_type),
      stats(type, type_size, stats_min, stats_max) {
}

SegmentStatistics::SegmentStatistics(PhysicalType type, idx_t type_size) : type(type), type_size(type_size) {
	Reset();
}

template <class T>
static void set_min_max(data_t min_value_p[], data_t max_value_p[], data_ptr_t min_p, data_ptr_t max_p) {
	memcpy(min_p, min_value_p, sizeof(T));
	memcpy(max_p, max_value_p, sizeof(T));
}

SegmentStatistics::SegmentStatistics(PhysicalType type, idx_t type_size, data_t stats_min[], data_t stats_max[])
    : type(type), type_size(type_size) {
	Reset();
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8: {
		set_min_max<int8_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case PhysicalType::INT16: {
		set_min_max<int16_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case PhysicalType::INT32: {
		set_min_max<int32_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case PhysicalType::INT64: {
		set_min_max<int64_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case PhysicalType::INT128: {
		set_min_max<hugeint_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case PhysicalType::FLOAT: {
		set_min_max<float>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case PhysicalType::DOUBLE: {
		set_min_max<double>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case PhysicalType::INTERVAL: {
		set_min_max<interval_t>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	case PhysicalType::VARCHAR: {
		set_min_max<char[8]>(stats_min, stats_max, minimum.get(), maximum.get());
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for SEGMENT statistics");
	}
}

template <class T> void initialize_max_min(data_ptr_t min, data_ptr_t max) {
	*((T *)min) = NumericLimits<T>::Maximum();
	*((T *)max) = NumericLimits<T>::Minimum();
}

void SegmentStatistics::Reset() {
	idx_t min_max_size = type == PhysicalType::VARCHAR ? 8 : type_size;
	minimum = unique_ptr<data_t[]>(new data_t[min_max_size]);
	maximum = unique_ptr<data_t[]>(new data_t[min_max_size]);
	has_null = false;
	max_string_length = 0;
	has_overflow_strings = false;
	char padding = '\0';
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		initialize_max_min<int8_t>(minimum.get(), maximum.get());
		break;
	case PhysicalType::INT16:
		initialize_max_min<int16_t>(minimum.get(), maximum.get());
		break;
	case PhysicalType::INT32:
		initialize_max_min<int32_t>(minimum.get(), maximum.get());
		break;
	case PhysicalType::INT64:
		initialize_max_min<int64_t>(minimum.get(), maximum.get());
		break;
	case PhysicalType::INT128:
		initialize_max_min<hugeint_t>(minimum.get(), maximum.get());
		break;
	case PhysicalType::FLOAT:
		initialize_max_min<float>(minimum.get(), maximum.get());
		break;
	case PhysicalType::DOUBLE:
		initialize_max_min<double>(minimum.get(), maximum.get());
		break;
	case PhysicalType::VARCHAR: {
		//! This marks the min/max was not initialized
		char marker = '1';
		memset(minimum.get(), padding, min_max_size);
		memset(maximum.get(), padding, min_max_size);
		minimum.get()[1] = marker;
		maximum.get()[1] = marker;
		break;
	}
	case PhysicalType::INTERVAL: {
		auto min = (interval_t *)minimum.get();
		auto max = (interval_t *)maximum.get();
		min->months = NumericLimits<int32_t>::Maximum();
		min->days = NumericLimits<int32_t>::Maximum();
		min->msecs = NumericLimits<int64_t>::Maximum();

		max->months = NumericLimits<int32_t>::Minimum();
		max->days = NumericLimits<int32_t>::Minimum();
		max->msecs = NumericLimits<int64_t>::Minimum();
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for SEGMENT statistics");
	}
}

} // namespace duckdb
