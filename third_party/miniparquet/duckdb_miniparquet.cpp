#include "duckdb_miniparquet.hpp"

#include "miniparquet.h"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb.hpp"


using namespace duckdb;
using namespace miniparquet;

struct ParquetScanFunctionData : public TableFunctionData {
	ParquetScanFunctionData(string filename)
	    : position(0), pf(filename) {
	}
	vector<SQLType> sql_types;
	idx_t position;
	ParquetFile pf;
	ResultChunk rc;
	ScanState s;
};

struct ParquetScanFunction : public TableFunction {
	ParquetScanFunction()
	    : TableFunction("parquet_scan", {SQLType::VARCHAR}, parquet_scan_bind, parquet_scan_function, nullptr){};

	static unique_ptr<FunctionData> parquet_scan_bind(ClientContext &context, vector<Value> inputs,
	                                                 vector<SQLType> &return_types, vector<string> &names) {

		auto file_name = inputs[0].GetValue<string>();
		auto res = make_unique<ParquetScanFunctionData>(file_name);
		res->pf.initialize_result(res->rc);

		for (auto& col : res->pf.columns) {
			names.push_back(col->name);
			SQLType type;
			switch (col->type) {
			case parquet::format::Type::BOOLEAN:
				type = SQLType::BOOLEAN;
				break;
			case parquet::format::Type::INT32:
				type = SQLType::INTEGER;
				break;
			case parquet::format::Type::INT64:
				type = SQLType::BIGINT;
				break;
			case parquet::format::Type::INT96: // always a timestamp?
				type = SQLType::TIMESTAMP;
				break;
			case parquet::format::Type::FLOAT:
				type = SQLType::FLOAT;
				break;
			case parquet::format::Type::DOUBLE:
				type = SQLType::DOUBLE;
				break;
//			case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: {
				// todo some decimals yuck
			case parquet::format::Type::BYTE_ARRAY:
				type = SQLType::VARCHAR;
				break;

			default:
				throw NotImplementedException("Invalid type");
				break;
			}
			return_types.push_back(type);
			res->sql_types.push_back(type);
		}


		return move(res);
	}

	template <class T> static void scan_parquet_column(ResultColumn& parquet_col, idx_t count, idx_t offset, Vector &out) {
		auto src_ptr = (T *)parquet_col.data.ptr;
		auto tgt_ptr = FlatVector::GetData<T>(out);

		for (idx_t row = 0; row < count; row++) {
			tgt_ptr[row] = src_ptr[row + offset];
		}
	}

	// surely they are joking
	static constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
	static constexpr int64_t kMillisecondsInADay = 86400000LL;
	static constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

	static int64_t impala_timestamp_to_nanoseconds(const Int96 &impala_timestamp) {
		int64_t days_since_epoch = impala_timestamp.value[2]
				- kJulianToUnixEpochDays;
		int64_t nanoseconds =
				*(reinterpret_cast<const int64_t*>(&(impala_timestamp.value)));
		return days_since_epoch * kNanosecondsInADay + nanoseconds;
	}


	static void parquet_scan_function(ClientContext &context, vector<Value> &input, DataChunk &output,
	                                 FunctionData *dataptr) {
		auto &data = *((ParquetScanFunctionData *)dataptr);

		if (data.position >= data.rc.nrows) {
			if (!data.pf.scan(data.s, data.rc)) {
				return;
			}
			data.position = 0;
		}
		idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, data.rc.nrows - data.position);
		assert(this_count > 0);
		output.SetCardinality(this_count);

		for (idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
			auto& col = data.rc.cols[col_idx];

			for (idx_t row = 0; row < this_count; row++) {
				FlatVector::SetNull(output.data[col_idx], row, !((uint8_t*) col.defined.ptr)[row + data.position]);
			}

			switch (data.sql_types[col_idx].id) {
			case SQLTypeId::BOOLEAN:
				scan_parquet_column<bool>(col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::INTEGER:
				scan_parquet_column<int32_t>(col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::BIGINT:
				scan_parquet_column<int64_t>(col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::FLOAT:
				scan_parquet_column<float>(col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::DOUBLE:
				scan_parquet_column<double>(col, this_count, data.position, output.data[col_idx]);
				break;
			case SQLTypeId::TIMESTAMP: {
				auto tgt_ptr = (timestamp_t *)FlatVector::GetData(output.data[col_idx]);
				for (idx_t row = 0; row < this_count; row++) {
					auto impala_ns = impala_timestamp_to_nanoseconds(((Int96*) col.data.ptr)[row + data.position]);

					auto ms = impala_ns / 1000000; // nanoseconds
					auto ms_per_day = (int64_t)60 * 60 * 24 * 1000;
					date_t date = Date::EpochToDate(ms / 1000);
					dtime_t time = (dtime_t)(ms % ms_per_day);
					tgt_ptr[row] = Timestamp::FromDatetime(date, time);
					;
				}
				// ugh
				break;
			} break;
			case SQLTypeId::VARCHAR: {
				auto src_ptr = (char**)col.data.ptr;
				auto tgt_ptr = (string_t *)FlatVector::GetData(output.data[col_idx]);

				for (idx_t row = 0; row < this_count; row++) {
					auto val = src_ptr[row + data.position];
					if (!FlatVector::IsNull(output.data[col_idx], row)) {
						auto utf_type = Utf8Proc::Analyze(val);
						if (utf_type == UnicodeType::INVALID) {
							throw Exception("Invalid UTF in Parquet file");
						} else if (utf_type == UnicodeType::ASCII) {
							tgt_ptr[row] = StringVector::AddString(output.data[col_idx], val);
						} else {
							auto val_norm = Utf8Proc::Normalize(val);
							tgt_ptr[row] = StringVector::AddString(output.data[col_idx], val_norm);
							free(val_norm);
						}
					}
				}
				break;
			}
			default:
				throw NotImplementedException("Unsupported type " + SQLTypeToString(data.sql_types[col_idx]));
			}
		}
		data.position += this_count;
	}
};

void Parquet::Init(DuckDB& db) {
	ParquetScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);

	Connection conn(db);
	conn.context->transaction.BeginTransaction();
	conn.context->catalog.CreateTableFunction(*conn.context, &info);
	conn.context->transaction.Commit();
}
