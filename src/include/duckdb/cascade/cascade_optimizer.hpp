//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascade_optimizer.hpp
//
// Cascade experiment: an independent optimizer that owns the handoff from a
// bound logical plan to the physical plan generator.
//
// Phase 0 scope: prove that this code, and not duckdb::Optimizer, decides what
// the executor runs. The optimization performed here is deliberately a no-op so
// that the only change in behaviour is *who* owns the plan; correctness is then
// checkable against the unmodified engine.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class LogicalOperator;

class CascadeOptimizer {
public:
	CascadeOptimizer(Binder &binder, ClientContext &context);

	//! Take over optimization of a bound logical plan.
	//! The returned plan is handed straight to PhysicalPlanGenerator.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	ClientContext &GetContext();
	Binder &GetBinder();

private:
	ClientContext &context;
	Binder &binder;
	//! Only used for the mandatory lowering that any executable plan needs
	//! (aggregate rewrites) and for binding expressions when rules are added.
	Optimizer duck;
};

} // namespace duckdb
