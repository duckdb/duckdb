#include <memory>
#include <utility>

#include "duckdb/parser/tableref/bound_ref_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_statement.hpp"

namespace duckdb {

BoundStatement Binder::Bind(BoundRefWrapper &ref) {
	if (!ref.binder || !ref.bound_ref.plan) {
		throw InternalException("Rebinding bound ref that was already bound");
	}
	bind_context.AddContext(std::move(ref.binder->bind_context));
	return std::move(ref.bound_ref);
}

} // namespace duckdb
