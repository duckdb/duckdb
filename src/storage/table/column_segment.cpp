#include "duckdb/storage/table/column_segment.hpp"
#include <cstring>

using namespace duckdb;
using namespace std;

ColumnSegment::ColumnSegment(TypeId type, ColumnSegmentType segment_type, idx_t start, idx_t count)
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type)), segment_type(segment_type),
      stats(type, type_size) {
}

ColumnSegment::ColumnSegment(TypeId type, ColumnSegmentType segment_type, idx_t start, idx_t count,uint64_t* stats_min, uint64_t* stats_max)
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type)), segment_type(segment_type),
      stats(type, type_size,stats_min,stats_max) {
}

SegmentStatistics::SegmentStatistics(TypeId type, idx_t type_size) : type(type), type_size(type_size) {
	Reset();
}

template <class T> static void set_min_max(T min_value, T max_value,  T *__restrict min, T *__restrict max) {
    *min = min_value;
    *max = max_value;
}

SegmentStatistics::SegmentStatistics(TypeId type, idx_t type_size, uint64_t* stats_min, uint64_t* stats_max) : type(type), type_size(type_size){
    Reset();
    switch (type) {
    case TypeId::INT8:{
        auto min = (int8_t *)minimum.get();
        auto max = (int8_t *)maximum.get();
        set_min_max<int8_t>((int8_t)(size_t)stats_min,(int8_t)(size_t)stats_max,min, max);
        break;
    }

    case TypeId::INT16:{
        auto min = (int16_t *)minimum.get();
        auto max = (int16_t *)maximum.get();
        set_min_max<int16_t>((int16_t)(size_t)stats_min,(int16_t)(size_t)stats_max,min, max);
        break;
    }
    case TypeId::INT32:{
        auto min = (int32_t *)minimum.get();
        auto max = (int32_t *)maximum.get();
        set_min_max<int32_t>((int32_t)(size_t)stats_min,(int32_t)(size_t)stats_max,min, max);
        break;
    }
    case TypeId::INT64:{
        auto min = (int64_t *)minimum.get();
        auto max = (int64_t *)maximum.get();
        set_min_max<int64_t>((int64_t)stats_min,(int64_t)stats_max,min, max);
        break;
    }
    case TypeId::FLOAT:{
        auto min = (float *)minimum.get();
        auto max = (float *)maximum.get();
        set_min_max<float>((float)(size_t)stats_min,(float)(size_t)stats_max,min, max);
        break;
    }
    case TypeId::DOUBLE:{
        auto min = (double *)minimum.get();
        auto max = (double *)maximum.get();
        set_min_max<double>((double)(size_t)stats_min,(double)(size_t)stats_max,min, max);
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
