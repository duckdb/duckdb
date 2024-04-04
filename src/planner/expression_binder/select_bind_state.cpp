#include "duckdb/planner/expression_binder/select_bind_state.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> SelectBindState::BindAlias(idx_t index) {
	return original_expressions[index]->Copy();
}

}
