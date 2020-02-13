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

static index_t list_payload_size(TypeId return_type) {
	return sizeof(Vector);
}

// NB: the result of this is copied around
static void list_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, sizeof(Vector));
	auto v = (Vector *)payload;
	v->type = TypeId::INVALID;
}

static void list_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);
	inputs[0].Normalify();

	auto states = (Vector **)state.GetData();

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		auto state = states[i];
		if (state->type == TypeId::INVALID) {
			state->Initialize(inputs[0].type, true, 100); // FIXME size? needs to grow this!
			state->count = 0;
			// TODO need to init child vectors, too
			// TODO need sqltype for this
		}
		state->count++;
		for (auto &child : state->GetChildren()) {
			child.second->count++;
		}
		state->SetValue(state->count - 1, inputs[0].GetValue(i)); // FIXME this is evil and slow.
		// We could alternatively collect all values for the same vector in this input chunk and assign with selection
		// vectors map<ptr, sel_vec>! worst case, one entry per input value, but meh todo: could abort?
	});
}

static void list_combine(Vector &state, Vector &combined) {
	throw Exception("eek");
	// TODO should be rather straightforward, copy vectors together
}

static void list_finalize(Vector &state, Vector &result) {
	auto states = (Vector **)state.GetData();

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
	list_child->Initialize(states[0]->type, false, total_len);
	list_child->count = 0;
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];
		list_child->Append(*state_ptr);
	});
	assert(list_child->count == total_len);
	result.AddChild(move(list_child));
}

struct ListBindData : public FunctionData {
	SQLType sql_type;

	ListBindData(SQLType sql_type) : sql_type(sql_type) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<ListBindData>(sql_type);
	}
};

unique_ptr<FunctionData> list_bind(BoundAggregateExpression &expr, ClientContext &context) {
	assert(expr.children.size() == 1);
	expr.sql_return_type = SQLType::LIST;
	expr.sql_return_type.child_type.push_back(make_pair("", expr.arguments[0]));
	return make_unique<ListBindData>(expr.sql_return_type);
}

TEST_CASE("Test filter and projection of nested lists", "[nested]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	unique_ptr<QueryResult> result;

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();
	auto &trans = con.context->transaction.ActiveTransaction();

	// TODO this should live elsewhere and be more complete
	auto agg = AggregateFunction("list", {SQLType::ANY}, SQLType::LIST, list_payload_size, list_initialize, list_update,
	                             list_combine, list_finalize, nullptr, list_bind);
	CreateAggregateFunctionInfo agg_info(agg);
	con.context->catalog.CreateFunction(trans, &agg_info);


	con.Query("CREATE TABLE list_data (g INTEGER, e INTEGER)");
	con.Query("INSERT INTO list_data VALUES (1, 1), (1, 2), (2, 3), (2, 4), (2, 5), (3, 6), (5, NULL)");

	result = con.Query("SELECT LIST(e) from list_data");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value()})}));

	result = con.Query("SELECT UNNEST(LIST(e)) ue from list_data ORDER BY ue");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4, 5, 6}));

	result = con.Query("SELECT LIST(e), LIST(g) from list_data");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value()})}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2), Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)})}));

	// FIXME
//	result = con.Query("SELECT UNNEST(LIST(e)) ue, LIST(g) from list_data");
//	result->Print();

	result = con.Query("SELECT g, LIST(e) from list_data GROUP BY g");
	result->Print();


	result = con.Query("SELECT g, LIST(e) l1, LIST(e) l2 from list_data GROUP BY g");
	result->Print();

	// FIXME
	//	result = con.Query("SELECT g, LIST(STRUCT_PACK(a := e, b := e+1)) from list_data GROUP BY g");
	//	result->Print();

	result = con.Query("SELECT g, LIST(CAST(e AS VARCHAR)) from list_data GROUP BY g");
	result->Print();

	result = con.Query("SELECT g, LIST(e/2.0) from list_data GROUP BY g");
	result->Print();

	result = con.Query("SELECT LIST(42)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({Value::INTEGER(42)})}));

	result = con.Query("SELECT LIST(42) FROM list_data");
	result->Print();


	// FIXME
	// omg omg
//	result = con.Query("SELECT g2, LIST(le) FROM (SELECT g % 2 g2, LIST(e) le from list_data GROUP BY g) sq GROUP BY g2");
//	result->Print();

	result = con.Query("SELECT UNNEST(LIST(42))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// unlist is alias of unnest for symmetry reasons
	result = con.Query("SELECT UNLIST(LIST(42))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("SELECT UNNEST(LIST(e)) ue, UNNEST(LIST(g)) ug from list_data ORDER BY ue");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4, 5, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 1, 1, 2, 2, 2, 3}));

	result = con.Query("SELECT g, UNNEST(LIST(e)) ue, UNNEST(LIST(e+1)) ue2 from list_data GROUP BY g ORDER BY ue");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 1, 1, 2, 2, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3, 4, 5, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 2, 3, 4, 5, 6, 7}));

	result = con.Query("SELECT g, UNNEST(l) u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1 ORDER BY u");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 1, 1, 2, 2, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3, 4, 5, 6}));

	result = con.Query("SELECT g, UNNEST(l)+1 u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1 ORDER BY u");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 1, 1, 2, 2, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4, 5, 6, 7}));


	// you're holding it wrong

	REQUIRE_FAIL(con.Query("SELECT LIST()"));
	REQUIRE_FAIL(con.Query("SELECT LIST() FROM list_data"));
	REQUIRE_FAIL(con.Query("SELECT LIST(e, g) FROM list_data"));

	REQUIRE_FAIL(con.Query("SELECT g, UNNEST(l+1) u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1"));
	REQUIRE_FAIL(con.Query("SELECT g, UNNEST(g) u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1"));
	REQUIRE_FAIL(con.Query("SELECT g, UNNEST() u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1"));

	REQUIRE_FAIL(con.Query("SELECT UNNEST(42)"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST()"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST(42) from list_data"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST() from list_data"));
	REQUIRE_FAIL(con.Query("SELECT g FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1 where UNNEST(l) > 42"));

	// TODO scalar list constructor
	// TODO ordering of chunks with lists
	// vector::set with lists!
	// TODO ?
	// pass binddata to callbacks, add cleanup function
}
