#include "duckdb/execution/operator/projection/physical_unnest.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

namespace duckdb {

class UnnestOperatorState : public OperatorState {
public:
	UnnestOperatorState() : parent_position(0), list_position(0), list_length(-1), first_fetch(true) {
	}

	idx_t parent_position;
	idx_t list_position;
	int64_t list_length;
	bool first_fetch;

	DataChunk list_data;
	vector<VectorData> list_vector_data;
	vector<VectorData> list_child_data;
};

// this implements a sorted window functions variant
PhysicalUnnest::PhysicalUnnest(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), select_list(std::move(select_list)) {
	D_ASSERT(!this->select_list.empty());
}

static void UnnestNull(idx_t start, idx_t end, Vector &result) {
	if (result.GetType().InternalType() == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(result);
		for (auto &child : children) {
			UnnestNull(start, end, *child);
		}
	}
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = start; i < end; i++) {
		validity.SetInvalid(i);
	}
	if (result.GetType().InternalType() == PhysicalType::STRUCT) {
		auto &struct_children = StructVector::GetEntries(result);
		for (auto &child : struct_children) {
			UnnestNull(start, end, *child);
		}
	}
}

template <class T>
static void TemplatedUnnest(VectorData &vdata, idx_t start, idx_t end, Vector &result) {
	auto source_data = (T *)vdata.data;
	auto &source_mask = vdata.validity;
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = start; i < end; i++) {
		auto source_idx = vdata.sel->get_index(i);
		auto target_idx = i - start;
		if (source_mask.RowIsValid(source_idx)) {
			result_data[target_idx] = source_data[source_idx];
			result_mask.SetValid(target_idx);
		} else {
			result_mask.SetInvalid(target_idx);
		}
	}
}

static void UnnestValidity(VectorData &vdata, idx_t start, idx_t end, Vector &result) {
	auto &source_mask = vdata.validity;
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = start; i < end; i++) {
		auto source_idx = vdata.sel->get_index(i);
		auto target_idx = i - start;
		result_mask.Set(target_idx, source_mask.RowIsValid(source_idx));
	}
}

static void UnnestVector(VectorData &vdata, Vector &source, idx_t list_size, idx_t start, idx_t end, Vector &result) {
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedUnnest<int8_t>(vdata, start, end, result);
		break;
	case PhysicalType::INT16:
		TemplatedUnnest<int16_t>(vdata, start, end, result);
		break;
	case PhysicalType::INT32:
		TemplatedUnnest<int32_t>(vdata, start, end, result);
		break;
	case PhysicalType::INT64:
		TemplatedUnnest<int64_t>(vdata, start, end, result);
		break;
	case PhysicalType::INT128:
		TemplatedUnnest<hugeint_t>(vdata, start, end, result);
		break;
	case PhysicalType::UINT8:
		TemplatedUnnest<uint8_t>(vdata, start, end, result);
		break;
	case PhysicalType::UINT16:
		TemplatedUnnest<uint16_t>(vdata, start, end, result);
		break;
	case PhysicalType::UINT32:
		TemplatedUnnest<uint32_t>(vdata, start, end, result);
		break;
	case PhysicalType::UINT64:
		TemplatedUnnest<uint64_t>(vdata, start, end, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedUnnest<float>(vdata, start, end, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedUnnest<double>(vdata, start, end, result);
		break;
	case PhysicalType::INTERVAL:
		TemplatedUnnest<interval_t>(vdata, start, end, result);
		break;
	case PhysicalType::VARCHAR:
		TemplatedUnnest<string_t>(vdata, start, end, result);
		break;
	case PhysicalType::LIST: {
		auto &target = ListVector::GetEntry(result);
		target.Reference(ListVector::GetEntry(source));
		ListVector::SetListSize(result, ListVector::GetListSize(source));
		TemplatedUnnest<list_entry_t>(vdata, start, end, result);
		break;
	}
	case PhysicalType::STRUCT: {
		auto &source_entries = StructVector::GetEntries(source);
		auto &target_entries = StructVector::GetEntries(result);
		UnnestValidity(vdata, start, end, result);
		for (idx_t i = 0; i < source_entries.size(); i++) {
			VectorData sdata;
			source_entries[i]->Orrify(list_size, sdata);
			UnnestVector(sdata, *source_entries[i], list_size, start, end, *target_entries[i]);
		}
		break;
	}
	default:
		throw InternalException("Unimplemented type for UNNEST");
	}
}

unique_ptr<OperatorState> PhysicalUnnest::GetOperatorState(ClientContext &context) const {
	return PhysicalUnnest::GetState(context);
}

unique_ptr<OperatorState> PhysicalUnnest::GetState(ClientContext &context) {
	return make_unique<UnnestOperatorState>();
}

OperatorResultType PhysicalUnnest::ExecuteInternal(ClientContext &context, DataChunk &input, DataChunk &chunk,
                                                   OperatorState &state_p,
                                                   const vector<unique_ptr<Expression>> &select_list,
                                                   bool include_input) {
	auto &state = (UnnestOperatorState &)state_p;
	do {
		if (state.first_fetch) {
			// get the list data to unnest
			ExpressionExecutor executor;
			vector<LogicalType> list_data_types;
			for (auto &exp : select_list) {
				D_ASSERT(exp->type == ExpressionType::BOUND_UNNEST);
				auto bue = (BoundUnnestExpression *)exp.get();
				list_data_types.push_back(bue->child->return_type);
				executor.AddExpression(*bue->child.get());
			}
			state.list_data.Destroy();
			state.list_data.Initialize(list_data_types);
			executor.Execute(input, state.list_data);

			// paranoia aplenty
			state.list_data.Verify();
			D_ASSERT(input.size() == state.list_data.size());
			D_ASSERT(state.list_data.ColumnCount() == select_list.size());

			// initialize VectorData object so the nullmask can accessed
			state.list_vector_data.resize(state.list_data.ColumnCount());
			state.list_child_data.resize(state.list_data.ColumnCount());
			for (idx_t col_idx = 0; col_idx < state.list_data.ColumnCount(); col_idx++) {
				auto &list_vector = state.list_data.data[col_idx];
				list_vector.Orrify(state.list_data.size(), state.list_vector_data[col_idx]);

				if (list_vector.GetType() == LogicalType::SQLNULL) {
					// UNNEST(NULL)
					auto &child_vector = list_vector;
					child_vector.Orrify(0, state.list_child_data[col_idx]);
				} else {
					auto list_size = ListVector::GetListSize(list_vector);
					auto &child_vector = ListVector::GetEntry(list_vector);
					child_vector.Orrify(list_size, state.list_child_data[col_idx]);
				}
			}
			state.first_fetch = false;
		}
		if (state.parent_position >= input.size()) {
			// finished with this input chunk
			state.parent_position = 0;
			state.list_position = 0;
			state.list_length = -1;
			state.first_fetch = true;
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// need to figure out how many times we need to repeat for current row
		if (state.list_length < 0) {
			for (idx_t col_idx = 0; col_idx < state.list_data.ColumnCount(); col_idx++) {
				auto &vdata = state.list_vector_data[col_idx];
				auto current_idx = vdata.sel->get_index(state.parent_position);

				int64_t list_length;
				// deal with NULL values
				if (!vdata.validity.RowIsValid(current_idx)) {
					list_length = 0;
				} else {
					auto list_data = (list_entry_t *)vdata.data;
					auto list_entry = list_data[current_idx];
					list_length = (int64_t)list_entry.length;
				}

				if (list_length > state.list_length) {
					state.list_length = list_length;
				}
			}
		}

		D_ASSERT(state.list_length >= 0);

		auto this_chunk_len = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.list_length - state.list_position);

		// first cols are from child, last n cols from unnest
		chunk.SetCardinality(this_chunk_len);

		idx_t output_offset = 0;
		if (include_input) {
			for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
				ConstantVector::Reference(chunk.data[col_idx], input.data[col_idx], state.parent_position,
				                          input.size());
			}
			output_offset = input.ColumnCount();
		}

		for (idx_t col_idx = 0; col_idx < state.list_data.ColumnCount(); col_idx++) {
			auto &result_vector = chunk.data[col_idx + output_offset];

			if (state.list_data.data[col_idx].GetType() == LogicalType::SQLNULL) {
				// UNNEST(NULL)
				chunk.SetCardinality(0);
			} else {
				auto &vdata = state.list_vector_data[col_idx];
				auto &child_data = state.list_child_data[col_idx];
				auto current_idx = vdata.sel->get_index(state.parent_position);

				auto list_data = (list_entry_t *)vdata.data;
				auto list_entry = list_data[current_idx];

				idx_t list_count;
				if (state.list_position >= list_entry.length) {
					list_count = 0;
				} else {
					list_count = MinValue<idx_t>(this_chunk_len, list_entry.length - state.list_position);
				}

				if (list_entry.length > state.list_position) {
					if (!vdata.validity.RowIsValid(current_idx)) {
						UnnestNull(0, list_count, result_vector);
					} else {
						auto &list_vector = state.list_data.data[col_idx];
						auto &child_vector = ListVector::GetEntry(list_vector);
						auto list_size = ListVector::GetListSize(list_vector);

						auto base_offset = list_entry.offset + state.list_position;
						UnnestVector(child_data, child_vector, list_size, base_offset, base_offset + list_count,
						             result_vector);
					}
				}

				UnnestNull(list_count, this_chunk_len, result_vector);
			}
		}

		state.list_position += this_chunk_len;
		if ((int64_t)state.list_position == state.list_length) {
			state.parent_position++;
			state.list_length = -1;
			state.list_position = 0;
		}

		chunk.Verify();
	} while (chunk.size() == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorResultType PhysicalUnnest::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &gstate, OperatorState &state) const {
	return ExecuteInternal(context.client, input, chunk, state, select_list);
}

} // namespace duckdb
