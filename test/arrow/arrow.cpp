#include "duckdb.hpp"
//#include "duckdb/common/arrow_cdata.hpp"
#include "arrow/c/bridge.h"
#include "arrow/api.h"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"

// see https://github.com/apache/arrow/blob/master/docs/source/format/CDataInterface.rst
// see also https://arrow.apache.org/docs/format/Columnar.html

struct MyArrowTable {
	const char *tag = "THIS_IS_SPARTAA";
	vector<ArrowSchema *> schemas;
	vector<ArrowArray *> columns;
};

struct ArrowScanFunctionData : public duckdb::TableFunctionData {
	bool finished;
	MyArrowTable *table;
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
			if (string(schema->format) == "l") {
				return_types.push_back(duckdb::LogicalType::BIGINT);
				auto name = string(schema->name);
				if (name.empty()) {
					name = string("v") + to_string(col_idx);
				}
				names.push_back(name);
			} else {
				throw std::runtime_error("Invalid type");
			}
			col_idx++;
		}

		res->finished = false;
		return move(res);
	}

	static void arrow_scan_function(duckdb::ClientContext &context, vector<duckdb::Value> &input,
	                                duckdb::DataChunk &output, duckdb::FunctionData *dataptr) {
		auto &data = *((ArrowScanFunctionData *)dataptr);

		if (data.finished) {
			return;
		}

		// FIXME support scanning with offsets
		assert(data.table->columns[0]->length < STANDARD_VECTOR_SIZE);

		output.SetCardinality(data.table->columns[0]->length);
		for (duckdb::idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
			switch (output.data[col_idx].type.id()) {
			case duckdb::LogicalTypeId::BIGINT: {
				duckdb::Vector bigint_vec(duckdb::LogicalType::BIGINT,
				                          (duckdb::data_ptr_t)data.table->columns[col_idx]->buffers[1]);
				output.data[col_idx].Reference(bigint_vec);
				break;
			}
			default:
				throw runtime_error("Unsupported type " + output.data[col_idx].type.ToString());
			}
		}
		data.finished = true;
	}
};

static string ptr_to_string(void const *ptr) {
	std::ostringstream address;
	address << ptr;
	return address.str();
}

int main(int argc, char *argv[]) {

	arrow::Int64Builder builder;
	builder.Append(1);
	builder.Append(2);
	builder.Append(3);
	builder.AppendNull();
	builder.Append(5);
	builder.Append(6);
	builder.Append(7);
	builder.Append(8);

	std::shared_ptr<arrow::Array> array;
	arrow::Status st = builder.Finish(&array);
	if (!st.ok()) {
		// ... do something on array building failure
	}

	// c-land from here on out

	duckdb::DuckDB db;

	ArrowScanFunction scan_fun;
	duckdb::CreateTableFunctionInfo cinfo(scan_fun);
	cinfo.name = "read_arrow";

	duckdb::Connection conn(db);
	conn.context->transaction.BeginTransaction();
	db.catalog->CreateTableFunction(*conn.context, &cinfo);
	conn.context->transaction.Commit();

	ArrowArray c_array;
	ArrowSchema c_schema;
	arrow::ExportArray(*array, &c_array, &c_schema);
	// printf("%s %lld %d \n", c_schema.format, c_array.length, c_array.n_buffers);

	assert(c_schema.children == nullptr);
	assert(c_schema.n_children == 0);

	assert(string(c_schema.format) == "l"); // TODO support other types

	assert(c_array.n_children == 0);
	assert(c_array.children == 0);

	assert(c_array.dictionary == nullptr); // TODO support this
	assert(c_array.offset == 0);           // TODO this can be non-zero

	// if c_array.null_count == 0, we might have only one n_buffers, in which case the data is in buffers[0] instead of
	// buffers[1]

	assert(c_array.n_buffers == 2);
	// lets try to read those buffers
	auto int64_buf = (int64_t *)c_array.buffers[1];

	for (int i = 0; i < c_array.length; i++) {
		printf("%d %lld\n", i, int64_buf[i]);
	}

	MyArrowTable arrow_table;
	arrow_table.schemas.push_back(&c_schema);
	arrow_table.columns.push_back(&c_array);

	auto res = conn.TableFunction("read_arrow", {duckdb::Value::POINTER((uintptr_t)&arrow_table)})
	               ->Query("arrow", "SELECT * FROM arrow");
	res->Print();

	c_schema.release(&c_schema);
	c_array.release(&c_array);

	return 0;
}
