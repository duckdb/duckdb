#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

using namespace duckdb;
using namespace std;

struct MyScanFunctionData : public TableFunctionData {
	MyScanFunctionData() : nrow(100) {
	}

	size_t nrow;
};

FunctionData *my_scan_function_init(ClientContext &context) {
	// initialize the function data structure
	return new MyScanFunctionData();
}

void my_scan_function(ClientContext &context, DataChunk &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = *((MyScanFunctionData *)dataptr);

	if (data.nrow < 1) {
		return;
	}

	// generate data for two output columns
	size_t this_rows = std::min(data.nrow, (size_t)1024);
	data.nrow -= this_rows;

	auto int_data = (int32_t *)output.data[0].GetData();
	auto group_data = (int32_t *)output.data[4].GetData();

	for (size_t row = 0; row < this_rows; row++) {
		int_data[row] = row % 10;
		group_data[row] = row % 4;

	}
	output.data[0].count = this_rows;
	output.data[4].count = this_rows;

	// TODO: the nested stuff should probably live in the data chunks's data area as well (?)
	auto &sv = output.data[1];
	auto cv1 = make_unique<Vector>(TypeId::INT32);
	cv1->Initialize(TypeId::INT32, true);
	auto cv2 = make_unique<Vector>(TypeId::DOUBLE);
	cv2->Initialize(TypeId::DOUBLE, true);

	auto cv1_data = (int32_t *)cv1->GetData();
	auto cv2_data = (double *)cv2->GetData();

	for (size_t row = 0; row < this_rows; row++) {
		// need to construct struct stuff here
		cv1_data[row] = row;
		cv2_data[row] = row;
		sv.nullmask[row] = row % 2 == 0;
	}

	cv1->count = this_rows;
	cv2->count = this_rows;
	cv1->vector_type = VectorType::FLAT_VECTOR;
	cv2->vector_type = VectorType::FLAT_VECTOR;

	// TODO we need to verify the schema here
	sv.children.push_back(pair<string, unique_ptr<Vector>>("first", move(cv1)));
	sv.children.push_back(pair<string, unique_ptr<Vector>>("second", move(cv2)));

	sv.count = this_rows;

	auto &lv = output.data[2];
	auto lc = make_unique<Vector>(TypeId::INT32);
	lc->count = STANDARD_VECTOR_SIZE; // TODO allow nullmask to be bigger
	lc->Initialize(TypeId::INT32, true, lc->count);

	auto lc_data = (int32_t *)lc->GetData();
	auto lcv_data = (list_entry_t *)lv.GetData();

	lc->count = this_rows * 2;

	for (size_t i = 0; i < lc->count; i++) {
		lc_data[i] = i;
	}

	for (size_t row = 0; row < this_rows; row++) {

		lv.nullmask[row] = row % 5 == 0;
		lc->nullmask[row] = row % 7 == 0;

		lcv_data[row].length = 2;
		lcv_data[row].offset = row * 2;
	}

	lv.children.push_back(pair<string, unique_ptr<Vector>>("", move(lc)));
	lv.count = this_rows;
	lv.nullmask.all();

	// list<struct<int, double>> ^^
	auto &list_struct = output.data[3];
	list_struct.count = this_rows;

	// need a vector for the struct as child
	auto list_struct_child = make_unique<Vector>();
	list_struct_child->type = TypeId::STRUCT;
	list_struct_child->count = this_rows * 2;

	auto list_struct_child_1 = make_unique<Vector>(TypeId::INT32);
	list_struct_child_1->Initialize(TypeId::INT32, true, list_struct_child->count);
	auto list_struct_child_2 = make_unique<Vector>(TypeId::DOUBLE);
	list_struct_child_2->Initialize(TypeId::DOUBLE, true, list_struct_child->count);

	auto list_struct_child_1_data = (int32_t *)list_struct_child_1->GetData();
	auto list_struct_child_2_data = (double *)list_struct_child_2->GetData();

	// dummy data
	for (size_t i = 0; i < list_struct_child->count; i++) {
		// need to construct struct stuff here
		list_struct_child_1_data[i] = i;
		list_struct_child_2_data[i] = i;
		list_struct_child->nullmask[i] = i % 13 == 0;
		list_struct_child_1->nullmask[i] = i % 7 == 0;
		list_struct_child_2->nullmask[i] = i % 5 == 0;
	}

	list_struct_child->children.push_back(pair<string, unique_ptr<Vector>>("a", move(list_struct_child_1)));
	list_struct_child->children.push_back(pair<string, unique_ptr<Vector>>("b", move(list_struct_child_2)));

	auto list_struct_data = (list_entry_t *)list_struct.GetData();
	for (size_t row = 0; row < this_rows; row++) {
		list_struct_data[row].length = 2;
		list_struct_data[row].offset = row * 2;
	}
	list_struct.children.push_back(pair<string, unique_ptr<Vector>>("", move(list_struct_child)));
}

class MyScanFunction : public TableFunction {
public:
	MyScanFunction() : TableFunction(MyScanConstruct()){};

private:
	TableFunction MyScanConstruct() { // TODO is this the simplest way of doing this?
		SQLType struct_type(SQLTypeId::STRUCT);
		struct_type.child_type.push_back(pair<string, SQLType>("first", SQLType::INTEGER));
		struct_type.child_type.push_back(pair<string, SQLType>("second", SQLType::DOUBLE));
		SQLType list_type(SQLTypeId::LIST);
		list_type.child_type.push_back(pair<string, SQLType>("", SQLType::INTEGER));

		SQLType struct_type2(SQLTypeId::STRUCT);
		struct_type2.child_type.push_back(pair<string, SQLType>("a", SQLType::INTEGER));
		struct_type2.child_type.push_back(pair<string, SQLType>("b", SQLType::DOUBLE));

		SQLType list_struct_type(SQLTypeId::LIST);
		list_struct_type.child_type.push_back(pair<string, SQLType>("", struct_type2));

		return TableFunction("my_scan", {}, {SQLType::INTEGER, struct_type, list_type, list_struct_type, SQLType::INTEGER},
		                     {"some_int", "some_struct", "some_list", "some_list_struct", "some_group_key"}, my_scan_function_init,
		                     my_scan_function, nullptr);
	}
};

// TODO this needs versions for the different return types, essentially all types.
// TODO should move to the binder
static void extract_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count == 2);
	auto &input1 = input.data[0];
	auto &input2 = input.data[1];
	assert(input1.type == TypeId::STRUCT);
	assert(input2.type == TypeId::VARCHAR);

	// TODO input2 might be a vector too
	auto key = input2.GetValue(0).str_value;
	for (auto &child : input1.children) {
		if (child.first == key) {
			result.Reference(*child.second.get());
			result.count = input1.count;
			result.sel_vector = input1.sel_vector;
			result.nullmask = input1.nullmask;
			return;
		}
	}
	throw Exception("Could not find struct key");
}

class StructExtractFunction : public ScalarFunction {
public:
	StructExtractFunction()
	    : ScalarFunction("struct_extract", {SQLType::STRUCT, SQLType::VARCHAR}, SQLType::INTEGER, extract_function){};
};



static index_t list_payload_size(TypeId return_type) {
	return sizeof(Vector);
}

// NB: the result of this is copied around
static void list_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, sizeof(Vector));
	auto v = (Vector*) payload;
	v->type = TypeId::INVALID;
}

static void list_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);
	inputs[0].Normalify();

	auto states = (Vector**)state.GetData();
	auto input_data = (int32_t *)inputs[0].GetData();

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		auto state = states[i];
		if (state->type == TypeId::INVALID) {
			state->Initialize(TypeId::INT32, true, 100);  // FIXME size? needs to grow this!
			state->count = 0;
		}
		state->count++;
		auto v = Value();
		if (!inputs[0].nullmask[i]) {
			v = Value::INTEGER(input_data[i]);
		}
		state->SetValue(state->count-1, v); // FIXME this is evil and slow
	});
}

static void list_combine(Vector &state, Vector &combined) {
	throw Exception("eek");
	// TODO should be rather straightforward, copy vectors together
}

static void list_finalize(Vector &state, Vector &result) {
	auto states = (Vector**)state.GetData();

	result.Initialize(TypeId::LIST, false, state.count);
	auto list_struct_data = (list_entry_t *)result.GetData();

	// first get total len of child vec
	size_t total_len = 0;
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];
		list_struct_data[i].length = state_ptr->count;
		list_struct_data[i].offset = total_len;
		total_len += state_ptr->count;
	});

	auto list_child = make_unique<Vector>();
	list_child->Initialize(TypeId::INT32, false, total_len);
	list_child->count = 0;
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];
		list_child->Append(*state_ptr);
	});
	assert(list_child->count == total_len);
	result.children.push_back(pair<string, unique_ptr<Vector>>("", move(list_child)));
}



TEST_CASE("Test filter and projection of nested struct", "[nested]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	MyScanFunction scan_fun;
	CreateTableFunctionInfo scan_info(scan_fun);
	StructExtractFunction extract_fun;
	CreateScalarFunctionInfo extract_info(extract_fun);

	SQLType list_type(SQLTypeId::LIST);
	list_type.child_type.push_back(pair<string, SQLType>("", SQLType::INTEGER));


	auto agg = AggregateFunction("list", {SQLType::INTEGER}, list_type, list_payload_size, list_initialize,
		                                  list_update, list_combine, list_finalize);

	CreateAggregateFunctionInfo agg_info(agg);

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();
	auto &trans = con.context->transaction.ActiveTransaction();
	con.context->catalog.CreateTableFunction(trans, &scan_info);
	con.context->catalog.CreateFunction(trans, &extract_info);
	con.context->catalog.CreateFunction(trans, &agg_info);


	// auto result = con.Query("SELECT some_int, some_struct, struct_extract(some_struct, 'first'), some_list FROM
	// my_scan() WHERE some_int > 7 ORDER BY some_int LIMIT 100 ");
	auto result = con.Query("SELECT some_int, some_list_struct FROM my_scan() WHERE some_int > 7 ");
	result->Print();


	con.Query("CREATE TABLE list_data (g INTEGER, e INTEGER)");
	con.Query("INSERT INTO list_data VALUES (1, 1), (1, 2), (2, 3), (2, 4), (2, 5), (3, 6)");

	result = con.Query("SELECT g, LIST(e) from list_data GROUP BY g ");
	result->Print();

	// TODO project into struct (ez!)
	// TODO flatten list in join

	// TODO map
	// TODO aggr/join
	// TODO ?

}
