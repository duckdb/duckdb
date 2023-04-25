//===----------------------------------------------------------------------===//
//                         DuckDB
//
// boolean_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"

namespace duckdb {

struct BooleanParquetValueConversion;

class BooleanColumnReader : public TemplatedColumnReader<bool, BooleanParquetValueConversion> {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::BOOL;

public:
	BooleanColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                    idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<bool, BooleanParquetValueConversion>(reader, std::move(type_p), schema_p, schema_idx_p,
	                                                                 max_define_p, max_repeat_p),
	      byte_pos(0) {};

	uint8_t byte_pos;

	void InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns, TProtocol &protocol_p) override {
		byte_pos = 0;
		TemplatedColumnReader<bool, BooleanParquetValueConversion>::InitializeRead(row_group_idx_p, columns,
		                                                                           protocol_p);
	}

	void ResetPage() override {
		byte_pos = 0;
	}
};

struct BooleanParquetValueConversion {
	static bool DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
		throw std::runtime_error("Dicts for booleans make no sense");
	}

	static bool PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.available(1);
		auto &byte_pos = reader.Cast<BooleanColumnReader>().byte_pos;
		bool ret = (*plain_data.ptr >> byte_pos) & 1;
		byte_pos++;
		if (byte_pos == 8) {
			byte_pos = 0;
			plain_data.inc(1);
		}
		return ret;
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		PlainRead(plain_data, reader);
	}
};

} // namespace duckdb
