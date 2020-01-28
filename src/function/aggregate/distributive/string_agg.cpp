#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <string>

using namespace std;

namespace duckdb {

typedef const char *string_agg_state_t;

void string_agg_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 2 && inputs[0].type == TypeId::VARCHAR && inputs[1].type == TypeId::VARCHAR);
	inputs[0].Normalify();
	inputs[1].Normalify();

	auto &strs = inputs[0];
	auto &seps = inputs[1];

	auto str_data = (const char **)strs.GetData();
	auto sep_data = (const char **)seps.GetData();
	auto states = (string_agg_state_t **) state.GetData();

	//  Share a reusable buffer for the block
	std::string buffer;

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if (strs.nullmask[i] || seps.nullmask[i]) {
			return;
		}

		auto state_ptr = states[i];
		auto str = str_data[i];
		auto sep = sep_data[i];
		if (IsNullValue(*state_ptr)) {
			*state_ptr = strs.AddString(str);
		} else {
			buffer = *state_ptr;
			buffer += sep;
			buffer += str;
			*state_ptr = strs.AddString(buffer.c_str());
		}
	});
}

void StringAggFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("string_agg", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR,
	                                  get_return_type_size, null_state_initialize, string_agg_update, nullptr,
	                                  gather_finalize));
}

} // namespace duckdb
