#include "column_writer.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// ColumnWriter
//===--------------------------------------------------------------------===//
ColumnWriter::ColumnWriter() {}

ColumnWriter::~ColumnWriter(){}

//===--------------------------------------------------------------------===//
// Standard Column Writer
//===--------------------------------------------------------------------===//
struct ParquetCastOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return TGT(input);
	}
};

struct ParquetTimestampNSOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Timestamp::FromEpochNanoSeconds(input).value;
	}
};

struct ParquetTimestampSOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Timestamp::FromEpochSeconds(input).value;
	}
};

struct ParquetHugeintOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Hugeint::Cast<double>(input);
	}
};

template <class SRC, class TGT, class OP = ParquetCastOperator>
static void TemplatedWritePlain(Vector &col, idx_t chunk_start, idx_t chunk_end, ValidityMask &mask, Serializer &ser) {
	auto *ptr = FlatVector::GetData<SRC>(col);
	for (idx_t r = chunk_start; r < chunk_end; r++) {
		if (mask.RowIsValid(r)) {
			ser.Write<TGT>(OP::template Operation<SRC, TGT>(ptr[r]));
		}
	}
}

template <class SRC, class TGT, class OP = ParquetCastOperator>
class StandardColumnWriter : public ColumnWriter {
public:
	StandardColumnWriter() = default;
	~StandardColumnWriter() override = default;

public:
	void WriteVector(Serializer &temp_writer, DataChunk &input, idx_t col_idx, idx_t chunk_start,
	                 idx_t chunk_end) override {
		auto &input_column = input.data[col_idx];
		auto &mask = FlatVector::Validity(input_column);
		TemplatedWritePlain<SRC, TGT, OP>(input_column, chunk_start, chunk_end, mask, temp_writer);
	}
};

//===--------------------------------------------------------------------===//
// Boolean Column Writer
//===--------------------------------------------------------------------===//
class BooleanColumnWriter : public ColumnWriter {
public:
	BooleanColumnWriter() = default;
	~BooleanColumnWriter() override = default;

public:
	void WriteVector(Serializer &temp_writer, DataChunk &input, idx_t col_idx, idx_t chunk_start,
	                 idx_t chunk_end) override {
		auto &input_column = input.data[col_idx];
		auto &mask = FlatVector::Validity(input_column);

#if STANDARD_VECTOR_SIZE < 64
		throw InternalException("Writing booleans to Parquet not supported for vsize < 64");
#endif
		auto *ptr = FlatVector::GetData<bool>(input_column);
		uint8_t byte = 0;
		uint8_t byte_pos = 0;
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			if (mask.RowIsValid(r)) { // only encode if non-null
				byte |= (ptr[r] & 1) << byte_pos;
				byte_pos++;

				if (byte_pos == 8) {
					temp_writer.Write<uint8_t>(byte);
					byte = 0;
					byte_pos = 0;
				}
			}
		}
		// flush last byte if req
		if (byte_pos > 0) {
			temp_writer.Write<uint8_t>(byte);
		}
	}
};

//===--------------------------------------------------------------------===//
// Decimal Column Writer
//===--------------------------------------------------------------------===//
class DecimalColumnWriter : public ColumnWriter {
public:
	DecimalColumnWriter() = default;
	~DecimalColumnWriter() override = default;

public:
	void WriteVector(Serializer &temp_writer, DataChunk &input, idx_t col_idx, idx_t chunk_start,
	                 idx_t chunk_end) override {
		auto &input_column = input.data[col_idx];
		auto &mask = FlatVector::Validity(input_column);

		// FIXME: fixed length byte array...
		Vector double_vec(LogicalType::DOUBLE);
		VectorOperations::Cast(input_column, double_vec, input.size());
		TemplatedWritePlain<double, double>(double_vec, chunk_start, chunk_end, mask, temp_writer);
	}
};

//===--------------------------------------------------------------------===//
// String Column Writer
//===--------------------------------------------------------------------===//
class StringColumnWriter : public ColumnWriter {
public:
	StringColumnWriter() = default;
	~StringColumnWriter() override = default;

public:
	void WriteVector(Serializer &temp_writer, DataChunk &input, idx_t col_idx, idx_t chunk_start,
	                 idx_t chunk_end) override {
		auto &input_column = input.data[col_idx];
		auto &mask = FlatVector::Validity(input_column);

		auto *ptr = FlatVector::GetData<string_t>(input_column);
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			if (mask.RowIsValid(r)) {
				temp_writer.Write<uint32_t>(ptr[r].GetSize());
				temp_writer.WriteData((const_data_ptr_t)ptr[r].GetDataUnsafe(), ptr[r].GetSize());
			}
		}
	}
};

//===--------------------------------------------------------------------===//
// Create Column Writer
//===--------------------------------------------------------------------===//
unique_ptr<ColumnWriter> ColumnWriter::CreateWriterRecursive(const LogicalType &type) {
	switch(type.id()) {
	case LogicalTypeId::BOOLEAN:
		return make_unique<BooleanColumnWriter>();
	case LogicalTypeId::TINYINT:
		return make_unique<StandardColumnWriter<int8_t, int32_t>>();
	case LogicalTypeId::SMALLINT:
		return make_unique<StandardColumnWriter<int16_t, int32_t>>();
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return make_unique<StandardColumnWriter<int32_t, int32_t>>();
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
		return make_unique<StandardColumnWriter<int64_t, int64_t>>();
	case LogicalTypeId::HUGEINT:
		return make_unique<StandardColumnWriter<hugeint_t, double, ParquetHugeintOperator>>();
	case LogicalTypeId::TIMESTAMP_NS:
		return make_unique<StandardColumnWriter<int64_t, int64_t, ParquetTimestampNSOperator>>();
	case LogicalTypeId::TIMESTAMP_SEC:
		return make_unique<StandardColumnWriter<int64_t, int64_t, ParquetTimestampSOperator>>();
	case LogicalTypeId::UTINYINT:
		return make_unique<StandardColumnWriter<uint8_t, int32_t>>();
	case LogicalTypeId::USMALLINT:
		return make_unique<StandardColumnWriter<uint16_t, int32_t>>();
	case LogicalTypeId::UINTEGER:
		return make_unique<StandardColumnWriter<uint32_t, uint32_t>>();
	case LogicalTypeId::UBIGINT:
		return make_unique<StandardColumnWriter<uint64_t, uint64_t>>();
	case LogicalTypeId::FLOAT:
		return make_unique<StandardColumnWriter<float, float>>();
	case LogicalTypeId::DOUBLE:
		return make_unique<StandardColumnWriter<double, double>>();
	case LogicalTypeId::DECIMAL:
		return make_unique<DecimalColumnWriter>();
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return make_unique<StringColumnWriter>();
	default:
		throw InternalException("eek");
	}
}

}
