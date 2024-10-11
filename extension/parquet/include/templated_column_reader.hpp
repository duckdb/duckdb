//===----------------------------------------------------------------------===//
//                         DuckDB
//
// templated__column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

template <class VALUE_TYPE>
struct TemplatedParquetValueConversion {
	static VALUE_TYPE DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
		D_ASSERT(offset < dict.len / sizeof(VALUE_TYPE));
		return ((VALUE_TYPE *)dict.ptr)[offset];
	}

	static VALUE_TYPE PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		return plain_data.read<VALUE_TYPE>();
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.inc(sizeof(VALUE_TYPE));
	}

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return plain_data.check_available(count * sizeof(VALUE_TYPE));
	}

	static VALUE_TYPE UnsafePlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		return plain_data.unsafe_read<VALUE_TYPE>();
	}

	static void UnsafePlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.unsafe_inc(sizeof(VALUE_TYPE));
	}
};

template <class VALUE_TYPE, class VALUE_CONVERSION>
class TemplatedColumnReader : public ColumnReader {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::INVALID;

public:
	TemplatedColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                      idx_t max_define_p, idx_t max_repeat_p)
	    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p) {};

	shared_ptr<ResizeableBuffer> dict;

public:
	void AllocateDict(idx_t size) {
		if (!dict) {
			dict = make_shared_ptr<ResizeableBuffer>(GetAllocator(), size);
		} else {
			dict->resize(GetAllocator(), size);
		}
	}

	void Dictionary(shared_ptr<ResizeableBuffer> data, idx_t num_entries) override {
		dict = std::move(data);
	}

	void Offsets(uint32_t *offsets, uint8_t *defines, uint64_t num_values, parquet_filter_t &filter,
	             idx_t result_offset, Vector &result) override {
		if (HasDefines()) {
			OffsetsInternal<true>(*dict, offsets, defines, num_values, filter, result_offset, result);
		} else {
			OffsetsInternal<false>(*dict, offsets, defines, num_values, filter, result_offset, result);
		}
	}

	void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, uint64_t num_values, parquet_filter_t &filter,
	           idx_t result_offset, Vector &result) override {
		PlainTemplated<VALUE_TYPE, VALUE_CONVERSION>(std::move(plain_data), defines, num_values, filter, result_offset,
		                                             result);
	}

private:
	template <bool HAS_DEFINES>
	void OffsetsInternal(ResizeableBuffer &dict_ref, uint32_t *__restrict offsets, const uint8_t *__restrict defines,
	                     const uint64_t num_values, const parquet_filter_t &filter, const idx_t result_offset,
	                     Vector &result) {
		const auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		auto &result_mask = FlatVector::Validity(result);
		idx_t offset_idx = 0;
		for (idx_t row_idx = result_offset; row_idx < result_offset + num_values; row_idx++) {
			if (HAS_DEFINES && defines[row_idx] != max_define) {
				result_mask.SetInvalid(row_idx);
				continue;
			}
			if (filter.test(row_idx)) {
				result_ptr[row_idx] = VALUE_CONVERSION::DictRead(dict_ref, offsets[offset_idx++], *this);
			} else {
				offset_idx++;
			}
		}
	}
};

template <class PARQUET_PHYSICAL_TYPE, class DUCKDB_PHYSICAL_TYPE,
          DUCKDB_PHYSICAL_TYPE (*FUNC)(const PARQUET_PHYSICAL_TYPE &input)>
struct CallbackParquetValueConversion {
	static DUCKDB_PHYSICAL_TYPE DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
		return TemplatedParquetValueConversion<DUCKDB_PHYSICAL_TYPE>::DictRead(dict, offset, reader);
	}

	static DUCKDB_PHYSICAL_TYPE PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		return FUNC(plain_data.read<PARQUET_PHYSICAL_TYPE>());
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.inc(sizeof(PARQUET_PHYSICAL_TYPE));
	}

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return plain_data.check_available(count * sizeof(PARQUET_PHYSICAL_TYPE));
	}

	static DUCKDB_PHYSICAL_TYPE UnsafePlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		return FUNC(plain_data.unsafe_read<PARQUET_PHYSICAL_TYPE>());
	}

	static void UnsafePlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.unsafe_inc(sizeof(PARQUET_PHYSICAL_TYPE));
	}
};

} // namespace duckdb
