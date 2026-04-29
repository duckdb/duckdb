#include "duckdb/execution/operator/scan/physical_positional_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <utility>

namespace duckdb {

PhysicalPositionalScan::PhysicalPositionalScan(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                               PhysicalOperator &left, PhysicalOperator &right)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::POSITIONAL_SCAN, std::move(types),
                       MaxValue(left.estimated_cardinality, right.estimated_cardinality)) {
	// Manage the children ourselves
	if (left.type == PhysicalOperatorType::TABLE_SCAN) {
		child_tables.emplace_back(left);
	} else if (left.type == PhysicalOperatorType::POSITIONAL_SCAN) {
		auto &left_scan = left.Cast<PhysicalPositionalScan>();
		child_tables = std::move(left_scan.child_tables);
	} else {
		throw InternalException("Invalid left input for PhysicalPositionalScan");
	}

	if (right.type == PhysicalOperatorType::TABLE_SCAN) {
		child_tables.emplace_back(right);
	} else if (right.type == PhysicalOperatorType::POSITIONAL_SCAN) {
		auto &right_scan = right.Cast<PhysicalPositionalScan>();
		auto &right_tables = right_scan.child_tables;
		child_tables.reserve(child_tables.size() + right_tables.size());
		std::move(right_tables.begin(), right_tables.end(), std::back_inserter(child_tables));
	} else {
		throw InternalException("Invalid right input for PhysicalPositionalScan");
	}
}

class PositionalScanGlobalSourceState : public GlobalSourceState {
public:
	PositionalScanGlobalSourceState(ClientContext &context, const PhysicalPositionalScan &op) {
		for (const auto &table : op.child_tables) {
			global_states.emplace_back(table.get().GetGlobalSourceState(context));
		}
	}

	vector<unique_ptr<GlobalSourceState>> global_states;

	idx_t MaxThreads() override {
		return 1;
	}
};

class PositionalTableScanner {
public:
	PositionalTableScanner(ExecutionContext &context, PhysicalOperator &table_p, GlobalSourceState &gstate_p)
	    : table(table_p), global_state(gstate_p), source_offset(0), exhausted(false) {
		local_state = table.GetLocalSourceState(context, gstate_p);
		source.Initialize(Allocator::Get(context.client), table.types);
	}

	SourceResultType Refill(ExecutionContext &context, InterruptState &interrupt_state, idx_t &available) {
		if (source_offset >= source.size()) {
			if (!exhausted) {
				source.Reset();

				OperatorSourceInput source_input {global_state, *local_state, interrupt_state};
				auto source_result = SourceResultType::HAVE_MORE_OUTPUT;
				while (source_result == SourceResultType::HAVE_MORE_OUTPUT && source.size() == 0) {
					source_result = table.GetData(context, source, source_input);
					if (source_result == SourceResultType::BLOCKED) {
						// Propagate suspension upward; the child source has registered its
						// wakeup on interrupt_state, and GetDataInternal will return BLOCKED
						// to the executor. On resume, this Refill is retried: source.size()
						// is still 0, so we re-enter the loop and call GetData again.
						available = 0;
						return SourceResultType::BLOCKED;
					}
				}
			}
			source_offset = 0;
		}

		available = source.size() - source_offset;
		if (!available) {
			if (!exhausted) {
				source.Reset();
				for (idx_t i = 0; i < source.ColumnCount(); ++i) {
					auto &vec = source.data[i];
					ConstantVector::SetNull(vec, count_t(STANDARD_VECTOR_SIZE));
				}
				exhausted = true;
			}
		}

		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	SourceResultType CopyData(ExecutionContext &context, InterruptState &interrupt_state, DataChunk &output,
	                          const idx_t count, const idx_t col_offset, idx_t &target_offset, idx_t &cols_copied) {
		if (target_offset == 0 && !source_offset && (source.size() >= count || exhausted)) {
			//	Fast track: aligned and has enough data
			for (idx_t i = 0; i < source.ColumnCount(); ++i) {
				output.data[col_offset + i].Reference(source.data[i]);
			}
			source_offset += count;
			target_offset = count;
		} else {
			// Copy data
			while (target_offset < count) {
				const auto needed = count - target_offset;
				const auto available = exhausted ? needed : (source.size() - source_offset);
				const auto copy_size = MinValue(needed, available);
				const auto source_count = source_offset + copy_size;
				for (idx_t i = 0; i < source.ColumnCount(); ++i) {
					VectorOperations::Copy(source.data[i], output.data[col_offset + i], source_count, source_offset,
					                       target_offset);
				}
				target_offset += copy_size;
				source_offset += copy_size;
				if (target_offset < count) {
					idx_t refill_available = 0;
					auto refill_result = Refill(context, interrupt_state, refill_available);
					if (refill_result == SourceResultType::BLOCKED) {
						cols_copied = 0;
						return SourceResultType::BLOCKED;
					}
				}
			}
		}

		cols_copied = source.ColumnCount();
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	ProgressData GetProgress(ClientContext &context) {
		return table.GetProgress(context, global_state);
	}

	PhysicalOperator &table;
	GlobalSourceState &global_state;
	unique_ptr<LocalSourceState> local_state;
	DataChunk source;
	idx_t source_offset;
	bool exhausted;
};

class PositionalScanLocalSourceState : public LocalSourceState {
public:
	PositionalScanLocalSourceState(ExecutionContext &context, PositionalScanGlobalSourceState &gstate,
	                               const PhysicalPositionalScan &op) {
		for (size_t i = 0; i < op.child_tables.size(); ++i) {
			auto &child = op.child_tables[i];
			auto &global_state = *gstate.global_states[i];
			scanners.emplace_back(make_uniq<PositionalTableScanner>(context, child, global_state));
		}
	}

	vector<unique_ptr<PositionalTableScanner>> scanners;

	// Resume state when GetDataInternal returns BLOCKED partway through a chunk.
	// All zero/false on a fresh call; populated when we propagate BLOCKED upward
	// so we can pick up where we left off when the executor calls us back.
	bool resuming = false;
	idx_t resume_phase = 0; // 0: refilling scanners; 1: copying data
	idx_t resume_count = 0;
	idx_t resume_scanner_index = 0;
	idx_t resume_col_offset = 0;
	idx_t resume_target_offset = 0;
};

unique_ptr<LocalSourceState> PhysicalPositionalScan::GetLocalSourceState(ExecutionContext &context,
                                                                         GlobalSourceState &gstate) const {
	return make_uniq<PositionalScanLocalSourceState>(context, gstate.Cast<PositionalScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalPositionalScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PositionalScanGlobalSourceState>(context, *this);
}

SourceResultType PhysicalPositionalScan::GetDataInternal(ExecutionContext &context, DataChunk &output,
                                                         OperatorSourceInput &input) const {
	auto &lstate = input.local_state.Cast<PositionalScanLocalSourceState>();

	idx_t count = lstate.resuming ? lstate.resume_count : 0;
	idx_t scanner_index = lstate.resuming ? lstate.resume_scanner_index : 0;
	idx_t col_offset = lstate.resuming ? lstate.resume_col_offset : 0;
	idx_t target_offset = lstate.resuming ? lstate.resume_target_offset : 0;
	const idx_t resume_phase = lstate.resuming ? lstate.resume_phase : 0;

	// Phase 0: find the longest source block by refilling each scanner.
	// Refill is idempotent w.r.t. BLOCKED — if a scanner returns BLOCKED we can
	// re-enter at the same scanner_index on resume; previously-refilled scanners
	// already have data cached in their `source` chunk and Refill becomes a no-op.
	if (!lstate.resuming || resume_phase == 0) {
		for (; scanner_index < lstate.scanners.size(); scanner_index++) {
			idx_t available = 0;
			auto refill_result = lstate.scanners[scanner_index]->Refill(context, input.interrupt_state, available);
			if (refill_result == SourceResultType::BLOCKED) {
				lstate.resuming = true;
				lstate.resume_phase = 0;
				lstate.resume_count = count;
				lstate.resume_scanner_index = scanner_index;
				return SourceResultType::BLOCKED;
			}
			count = MaxValue(count, available);
		}

		if (!count) {
			lstate.resuming = false;
			return SourceResultType::FINISHED;
		}

		// Phase 0 complete; fall through to phase 1 starting from the first scanner.
		scanner_index = 0;
		col_offset = 0;
		target_offset = 0;
	}

	// Phase 1: copy or reference the source columns into the output.
	for (; scanner_index < lstate.scanners.size(); scanner_index++) {
		auto &scanner = lstate.scanners[scanner_index];
		idx_t cols_copied = 0;
		auto copy_result = scanner->CopyData(context, input.interrupt_state, output, count, col_offset, target_offset,
		                                     cols_copied);
		if (copy_result == SourceResultType::BLOCKED) {
			lstate.resuming = true;
			lstate.resume_phase = 1;
			lstate.resume_count = count;
			lstate.resume_scanner_index = scanner_index;
			lstate.resume_col_offset = col_offset;
			lstate.resume_target_offset = target_offset;
			return SourceResultType::BLOCKED;
		}
		col_offset += cols_copied;
		target_offset = 0;
	}

	lstate.resuming = false;
	output.SetCardinality(count);
	return SourceResultType::HAVE_MORE_OUTPUT;
}

ProgressData PhysicalPositionalScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<PositionalScanGlobalSourceState>();

	ProgressData res;

	for (size_t t = 0; t < child_tables.size(); ++t) {
		res.Add(child_tables[t].get().GetProgress(context, *gstate.global_states[t]));
	}

	return res;
}

bool PhysicalPositionalScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}

	auto &other = other_p.Cast<PhysicalPositionalScan>();
	if (child_tables.size() != other.child_tables.size()) {
		return false;
	}
	for (size_t i = 0; i < child_tables.size(); ++i) {
		if (!child_tables[i].get().Equals(other.child_tables[i])) {
			return false;
		}
	}

	return true;
}

vector<const_reference<PhysicalOperator>> PhysicalPositionalScan::GetChildren() const {
	auto result = PhysicalOperator::GetChildren();
	for (auto &entry : child_tables) {
		result.push_back(entry.get());
	}
	return result;
}

} // namespace duckdb
