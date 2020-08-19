#include "duckdb.hpp"
//#include "duckdb/common/arrow_cdata.hpp"
#include "arrow/c/bridge.h"
#include "arrow/api.h"
#include "arrow/io/file.h"
#include <iostream>
#include "parquet/arrow/reader.h"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"

#include "utf8proc_wrapper.hpp"

// see https://github.com/apache/arrow/blob/master/docs/source/format/CDataInterface.rst
// see also https://arrow.apache.org/docs/format/Columnar.html

struct MyArrowTable {
	const char *tag = "THIS_IS_SPARTAA";

	MyArrowTable(duckdb::idx_t ncol) {
		schemas.resize(ncol);
		columns.resize(ncol);
	}
	~MyArrowTable() {
		for (duckdb::idx_t col_idx = 0; col_idx < schemas.size(); col_idx++) {
			schemas[col_idx].release(&schemas[col_idx]);
			for (duckdb::idx_t chunk_idx = 0; chunk_idx < columns[col_idx].size(); chunk_idx++) {
				columns[col_idx][chunk_idx].release(&columns[col_idx][chunk_idx]);
			}
		}
	}
	vector<ArrowSchema> schemas;
	vector<vector<ArrowArray>> columns; // with chunks
};

struct ArrowScanFunctionData : public duckdb::TableFunctionData {
	MyArrowTable *table;
	duckdb::idx_t chunk_idx = 0;
	duckdb::idx_t chunk_offset = 0;
};

class ArrowScanFunction : public duckdb::TableFunction {
public:
	ArrowScanFunction()
	    : duckdb::TableFunction("parquet_scan", {duckdb::LogicalType::POINTER}, arrow_scan_bind, arrow_scan_function,
	                            nullptr) {
		supports_projection = true;
	}

private:
	static unique_ptr<duckdb::FunctionData> arrow_scan_bind(duckdb::ClientContext &context,
	                                                        vector<duckdb::Value> &inputs,
	                                                        unordered_map<string, duckdb::Value> &named_parameters,
	                                                        vector<duckdb::LogicalType> &return_types,
	                                                        vector<string> &names) {

		auto res = duckdb::make_unique<ArrowScanFunctionData>();
		res->table = (MyArrowTable *)inputs[0].GetValue<uintptr_t>();

		int col_idx = 0;
		for (auto &schema : res->table->schemas) {
			auto format = string(schema.format);
			if (format == "c") {
				return_types.push_back(duckdb::LogicalType::TINYINT);
			} else if (format == "s") {
				return_types.push_back(duckdb::LogicalType::SMALLINT);
			} else if (format == "i") {
				return_types.push_back(duckdb::LogicalType::INTEGER);
			} else if (format == "l") {
				return_types.push_back(duckdb::LogicalType::BIGINT);
			} else if (format == "f") {
				return_types.push_back(duckdb::LogicalType::FLOAT);
			} else if (format == "g") {
				return_types.push_back(duckdb::LogicalType::DOUBLE);
			} else if (format == "u") {
				return_types.push_back(duckdb::LogicalType::VARCHAR);
			} else if (format == "tsn:") {
				return_types.push_back(duckdb::LogicalType::TIMESTAMP);
			} else {
				throw duckdb::NotImplementedException("Invalid Arrow type %s", format);
			}
			auto name = string(schema.name);
			if (name.empty()) {
				name = string("v") + to_string(col_idx);
			}
			names.push_back(name);
			col_idx++;
		}

		return move(res);
	}

	static void arrow_scan_function(duckdb::ClientContext &context, vector<duckdb::Value> &input,
	                                duckdb::DataChunk &output, duckdb::FunctionData *dataptr) {
		auto &data = *((ArrowScanFunctionData *)dataptr);

		// have we run out of data on the current chunk? move to next one
		if (data.chunk_offset >= data.table->columns[0][data.chunk_idx].length) {
			data.chunk_idx++;
			data.chunk_offset = 0;
		}

		// have we run out of chunks? we done
		if (data.chunk_idx >= data.table->columns[0].size()) {
			return;
		}

		output.SetCardinality(std::min((int64_t)STANDARD_VECTOR_SIZE,
		                               (int64_t)(data.table->columns[0][data.chunk_idx].length - data.chunk_offset)));
		for (duckdb::idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
			auto array = data.table->columns[col_idx][data.chunk_idx];

			// just memcpy nullmask
			if (array.buffers[0]) {
				auto n_bitmask_bytes = (output.size() + 8 - 1) / 8;
				auto bytes_to_skip = (data.chunk_offset + 8 - 1) / 8;

				duckdb::nullmask_t new_nullmask;
				memcpy(&new_nullmask, (uint8_t *)array.buffers[0] + bytes_to_skip, n_bitmask_bytes);
				duckdb::FlatVector::SetNullmask(output.data[col_idx], new_nullmask.flip());
			}

			switch (output.data[col_idx].type.id()) {
			case duckdb::LogicalTypeId::TINYINT:
			case duckdb::LogicalTypeId::SMALLINT:
			case duckdb::LogicalTypeId::INTEGER:
			case duckdb::LogicalTypeId::FLOAT:
			case duckdb::LogicalTypeId::DOUBLE:
			case duckdb::LogicalTypeId::BIGINT:
				duckdb::FlatVector::SetData(output.data[col_idx],
				                            (duckdb::data_ptr_t)array.buffers[1] +
				                                duckdb::GetTypeIdSize(output.data[col_idx].type.InternalType()) *
				                                    data.chunk_offset);
				break;

			case duckdb::LogicalTypeId::VARCHAR: {
				auto offsets = (uint32_t *)array.buffers[1] + data.chunk_offset;
				auto cdata = (char *)array.buffers[2];

				for (int i = 0; i < output.size(); i++) {
					auto cptr = cdata + offsets[i];
					auto str_len = offsets[i + 1] - offsets[i];

					auto utf_type = duckdb::Utf8Proc::Analyze(cptr, str_len);
					switch (utf_type) {
					case duckdb::UnicodeType::ASCII:
						duckdb::FlatVector::GetData<duckdb::string_t>(output.data[col_idx])[i] =
						    duckdb::StringVector::AddString(output.data[col_idx], cptr, str_len);
						break;
					case duckdb::UnicodeType::UNICODE:
						// this regrettably copies to normalize
						duckdb::FlatVector::GetData<duckdb::string_t>(output.data[col_idx])[i] =
						    duckdb::StringVector::AddString(output.data[col_idx],
						                                    duckdb::Utf8Proc::Normalize(string(cptr, str_len)));

						break;
					case duckdb::UnicodeType::INVALID:
						throw runtime_error("Invalid UTF8 string encoding");
					}
				}

				break;
			} // TODO timestamps in duckdb are subject to change
			case duckdb::LogicalTypeId::TIMESTAMP: {
				auto src_ptr = (uint64_t *)array.buffers[1] + data.chunk_offset;
				auto tgt_ptr = (duckdb::timestamp_t *)duckdb::FlatVector::GetData(output.data[col_idx]);

				for (duckdb::idx_t row = 0; row < output.size(); row++) {
					auto source_idx = data.chunk_offset + row;

					auto ms = src_ptr[source_idx] / 1000000; // nanoseconds
					auto ms_per_day = (int64_t)60 * 60 * 24 * 1000;
					duckdb::date_t date = duckdb::Date::EpochToDate(ms / 1000);
					duckdb::dtime_t time = (duckdb::dtime_t)(ms % ms_per_day);
					tgt_ptr[row] = duckdb::Timestamp::FromDatetime(date, time);
				}
				break;
			}
			default:
				throw runtime_error("Unsupported type " + output.data[col_idx].type.ToString());
			}
		}
		output.Verify();
		data.chunk_offset += output.size();
	}
};

// FIXME this is a test
int main(int argc, char *argv[]) {
	duckdb::DuckDB db;

	ArrowScanFunction scan_fun;
	duckdb::CreateTableFunctionInfo cinfo(scan_fun);
	cinfo.name = "read_arrow";

	duckdb::Connection conn(db);
	conn.context->transaction.BeginTransaction();
	db.catalog->CreateTableFunction(*conn.context, &cinfo);
	conn.context->transaction.Commit();

	arrow::Status st;
	arrow::MemoryPool *pool = arrow::default_memory_pool();
	auto input =
	    arrow::io::ReadableFile::Open("/Users/hannes/source/duckdb/test/sql/copy/parquet/data/userdata1.parquet")
	        .ValueOrDie();

	// Open Parquet file reader
	std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
	st = parquet::arrow::OpenFile(input, pool, &arrow_reader);
	if (!st.ok()) {
		throw runtime_error(st.message());
	}

	// Read entire file as a single Arrow table
	std::shared_ptr<arrow::Table> table;
	st = arrow_reader->ReadTable(&table);
	if (!st.ok()) {
		throw runtime_error(st.message());
	}

	duckdb::idx_t col_idx = 0;
	// cout << table->ToString() << "\n";
	MyArrowTable arrow_table(table->num_columns());

	for (auto &col : table->columns()) {
		st = arrow::ExportField(*table->schema()->field(col_idx), &arrow_table.schemas[col_idx]);
		if (!st.ok()) {
			throw duckdb::InternalException("arrow schema export failed: ", st.message());
		}
		arrow_table.columns[col_idx].resize(col->num_chunks());
		duckdb::idx_t chunk_idx = 0;
		for (auto &chunk : col->chunks()) {
			st = arrow::ExportArray(*chunk, &arrow_table.columns[col_idx][chunk_idx]);
			if (!st.ok()) {
				throw duckdb::InternalException("arrow array export failed: ", st.message());
			}
			chunk_idx++;
		}
		col_idx++;
	}

	auto res = conn.TableFunction("read_arrow", {duckdb::Value::POINTER((uintptr_t)&arrow_table)})
	               ->Query("arrow", "SELECT * FROM arrow");
	res->Print();

	// now do something with table

	return 0;
}
