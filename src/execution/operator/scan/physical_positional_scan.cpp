#include "duckdb/execution/operator/scan/physical_positional_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <utility>

namespace duckdb {

PhysicalPositionalScan::PhysicalPositionalScan(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
                                               unique_ptr<PhysicalOperator> right)
    : PhysicalOperator(PhysicalOperatorType::POSITIONAL_SCAN, std::move(types),
                       MaxValue(left->estimated_cardinality, right->estimated_cardinality)) {

	// Manage the children ourselves
	if (left->type == PhysicalOperatorType::TABLE_SCAN) {
		child_tables.emplace_back(std::move(left));
	} else if (left->type == PhysicalOperatorType::POSITIONAL_SCAN) {
		auto &left_scan = left->Cast<PhysicalPositionalScan>();
		child_tables = std::move(left_scan.child_tables);
	} else {
		throw InternalException("Invalid left input for PhysicalPositionalScan");
	}

	if (right->type == PhysicalOperatorType::TABLE_SCAN) {
		child_tables.emplace_back(std::move(right));
	} else if (right->type == PhysicalOperatorType::POSITIONAL_SCAN) {
		auto &right_scan = right->Cast<PhysicalPositionalScan>();
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
			global_states.emplace_back(table->GetGlobalSourceState(context));
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

	idx_t Refill(ExecutionContext &context) {
		if (source_offset >= source.size()) {
			if (!exhausted) {
				source.Reset();

				InterruptState interrupt_state;
				OperatorSourceInput source_input {global_state, *local_state, interrupt_state};
				auto source_result = table.GetData(context, source, source_input);
				if (source_result == SourceResultType::BLOCKED) {
					throw NotImplementedException(
					    "Unexpected interrupt from table Source in PositionalTableScanner refill");
				}
			}
			source_offset = 0;
		}

		const auto available = source.size() - source_offset;
		if (!available) {
			if (!exhausted) {
				source.Reset();
				for (idx_t i = 0; i < source.ColumnCount(); ++i) {
					auto &vec = source.data[i];
					vec.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(vec, true);
				}
				exhausted = true;
			}
		}

		return available;
	}

	idx_t CopyData(ExecutionContext &context, DataChunk &output, const idx_t count, const idx_t col_offset) {
		if (!source_offset && (source.size() >= count || exhausted)) {
			//	Fast track: aligned and has enough data
			for (idx_t i = 0; i < source.ColumnCount(); ++i) {
				output.data[col_offset + i].Reference(source.data[i]);
			}
			source_offset += count;
		} else {
			// Copy data
			for (idx_t target_offset = 0; target_offset < count;) {
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
				Refill(context);
			}
		}

		return source.ColumnCount();
	}

	double GetProgress(ClientContext &context) {
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
			auto &child = *op.child_tables[i];
			auto &global_state = *gstate.global_states[i];
			scanners.emplace_back(make_uniq<PositionalTableScanner>(context, child, global_state));
		}
	}

	vector<unique_ptr<PositionalTableScanner>> scanners;
};

unique_ptr<LocalSourceState> PhysicalPositionalScan::GetLocalSourceState(ExecutionContext &context,
                                                                         GlobalSourceState &gstate) const {
	return make_uniq<PositionalScanLocalSourceState>(context, gstate.Cast<PositionalScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalPositionalScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PositionalScanGlobalSourceState>(context, *this);
}

SourceResultType PhysicalPositionalScan::GetData(ExecutionContext &context, DataChunk &output,
                                                 OperatorSourceInput &input) const {
	auto &lstate = input.local_state.Cast<PositionalScanLocalSourceState>();

	// Find the longest source block
	idx_t count = 0;
	for (auto &scanner : lstate.scanners) {
		count = MaxValue(count, scanner->Refill(context));
	}

	//	All done?
	if (!count) {
		return SourceResultType::FINISHED;
	}

	// Copy or reference the source columns
	idx_t col_offset = 0;
	for (auto &scanner : lstate.scanners) {
		col_offset += scanner->CopyData(context, output, count, col_offset);
	}

	output.SetCardinality(count);
	return SourceResultType::HAVE_MORE_OUTPUT;
}

double PhysicalPositionalScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<PositionalScanGlobalSourceState>();

	double result = child_tables[0]->GetProgress(context, *gstate.global_states[0]);
	for (size_t t = 1; t < child_tables.size(); ++t) {
		result = MinValue(result, child_tables[t]->GetProgress(context, *gstate.global_states[t]));
	}

	return result;
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
		if (!child_tables[i]->Equals(*other.child_tables[i])) {
			return false;
		}
	}

	return true;
}

vector<const_reference<PhysicalOperator>> PhysicalPositionalScan::GetChildren() const {
	auto result = PhysicalOperator::GetChildren();
	for (auto &entry : child_tables) {
		result.push_back(*entry);
	}
	return result;
}

} // namespace duckdb
