#include "duckdb/parser/tableref/bound_ref_wrapper.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(BoundRefWrapper &ref) {
	if (!ref.binder || !ref.bound_ref) {
		throw InternalException("Rebinding bound ref that was already bound");
	}
	bind_context.AddContext(std::move(ref.binder->bind_context));
	return std::move(ref.bound_ref);
}

} // namespace duckdb
