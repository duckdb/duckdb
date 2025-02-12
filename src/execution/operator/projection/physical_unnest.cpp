#include "duckdb/execution/operator/projection/physical_unnest.hpp"

#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

namespace duckdb {

class UnnestOperatorState : public OperatorState {
public:
	UnnestOperatorState(ClientContext &context, const vector<unique_ptr<Expression>> &select_list)
	    : current_row(0), list_position(0), first_fetch(true),
	      input_sel(STANDARD_VECTOR_SIZE), executor(context) {

		// for each UNNEST in the select_list, we add the child expression to the expression executor
		// and set the return type in the list_data chunk, which will contain the evaluated expression results
		vector<LogicalType> list_data_types;
		for (auto &exp : select_list) {
			D_ASSERT(exp->GetExpressionType() == ExpressionType::BOUND_UNNEST);
			auto &bue = exp->Cast<BoundUnnestExpression>();
			list_data_types.push_back(bue.child->return_type);
			executor.AddExpression(*bue.child.get());

			unnest_sels.emplace_back(STANDARD_VECTOR_SIZE);
		}

		auto &allocator = Allocator::Get(context);
		list_data.Initialize(allocator, list_data_types);

		list_vector_data.resize(list_data.ColumnCount());
		list_child_data.resize(list_data.ColumnCount());
	}

	idx_t current_row;
	idx_t list_position;
	unsafe_vector<idx_t> unnest_lengths;
	bool first_fetch;
	SelectionVector input_sel;
	vector<SelectionVector> unnest_sels;

	ExpressionExecutor executor;
	DataChunk list_data;
	vector<UnifiedVectorFormat> list_vector_data;
	vector<UnifiedVectorFormat> list_child_data;

public:
	//! Reset the fields of the unnest operator state
	void Reset();
	//! Prepare the input for the next unnest
	void PrepareInput(DataChunk &input,
							 const vector<unique_ptr<Expression>> &select_list);
};

void UnnestOperatorState::Reset() {
	current_row = 0;
	list_position = 0;
	first_fetch = true;
}

PhysicalUnnest::PhysicalUnnest(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, std::move(types), estimated_cardinality), select_list(std::move(select_list)) {
	D_ASSERT(!this->select_list.empty());
}

static void UnnestNull(idx_t start, idx_t end, Vector &result) {

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = start; i < end; i++) {
		validity.SetInvalid(i);
	}

	const auto &logical_type = result.GetType();
	if (logical_type.InternalType() == PhysicalType::STRUCT) {
		const auto &struct_children = StructVector::GetEntries(result);
		for (auto &child : struct_children) {
			UnnestNull(start, end, *child);
		}
	} else if (logical_type.InternalType() == PhysicalType::ARRAY) {
		auto &array_child = ArrayVector::GetEntry(result);
		auto array_size = ArrayType::GetSize(logical_type);
		UnnestNull(start * array_size, end * array_size, array_child);
	}
}

template <class T>
static void TemplatedUnnest(UnifiedVectorFormat &vector_data, idx_t start, idx_t end, Vector &result) {

	auto source_data = UnifiedVectorFormat::GetData<T>(vector_data);
	auto &source_mask = vector_data.validity;

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = start; i < end; i++) {
		auto source_idx = vector_data.sel->get_index(i);
		auto target_idx = i - start;
		if (source_mask.RowIsValid(source_idx)) {
			result_data[target_idx] = source_data[source_idx];
			result_mask.SetValid(target_idx);
		} else {
			result_mask.SetInvalid(target_idx);
		}
	}
}

static void UnnestValidity(UnifiedVectorFormat &vector_data, idx_t start, idx_t end, Vector &result) {

	auto &source_mask = vector_data.validity;
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = start; i < end; i++) {
		auto source_idx = vector_data.sel->get_index(i);
		auto target_idx = i - start;
		result_mask.Set(target_idx, source_mask.RowIsValid(source_idx));
	}
}

static void UnnestVector(UnifiedVectorFormat &child_vector_data, Vector &child_vector, idx_t list_size, idx_t start,
                         idx_t end, Vector &result) {

	D_ASSERT(child_vector.GetType() == result.GetType());
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedUnnest<int8_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::INT16:
		TemplatedUnnest<int16_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::INT32:
		TemplatedUnnest<int32_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::INT64:
		TemplatedUnnest<int64_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::INT128:
		TemplatedUnnest<hugeint_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::UINT8:
		TemplatedUnnest<uint8_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::UINT16:
		TemplatedUnnest<uint16_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::UINT32:
		TemplatedUnnest<uint32_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::UINT64:
		TemplatedUnnest<uint64_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::UINT128:
		TemplatedUnnest<uhugeint_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedUnnest<float>(child_vector_data, start, end, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedUnnest<double>(child_vector_data, start, end, result);
		break;
	case PhysicalType::INTERVAL:
		TemplatedUnnest<interval_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::VARCHAR:
		TemplatedUnnest<string_t>(child_vector_data, start, end, result);
		break;
	case PhysicalType::LIST: {
		// the child vector of result now references the child vector source
		// FIXME: only reference relevant children (start - end) instead of all
		auto &target = ListVector::GetEntry(result);
		target.Reference(ListVector::GetEntry(child_vector));
		ListVector::SetListSize(result, ListVector::GetListSize(child_vector));
		// unnest
		TemplatedUnnest<list_entry_t>(child_vector_data, start, end, result);
		break;
	}
	case PhysicalType::STRUCT: {
		auto &child_vector_entries = StructVector::GetEntries(child_vector);
		auto &result_entries = StructVector::GetEntries(result);

		// set the validity mask for the 'outer' struct vector before unnesting its children
		UnnestValidity(child_vector_data, start, end, result);

		for (idx_t i = 0; i < child_vector_entries.size(); i++) {
			UnifiedVectorFormat child_vector_entries_data;
			child_vector_entries[i]->ToUnifiedFormat(list_size, child_vector_entries_data);
			UnnestVector(child_vector_entries_data, *child_vector_entries[i], list_size, start, end,
			             *result_entries[i]);
		}
		break;
	}
	case PhysicalType::ARRAY: {
		auto array_size = ArrayType::GetSize(child_vector.GetType());
		auto &source_array = ArrayVector::GetEntry(child_vector);
		auto &target_array = ArrayVector::GetEntry(result);

		UnnestValidity(child_vector_data, start, end, result);

		UnifiedVectorFormat child_array_data;
		source_array.ToUnifiedFormat(list_size * array_size, child_array_data);
		UnnestVector(child_array_data, source_array, list_size * array_size, start * array_size, end * array_size,
		             target_array);
		break;
	}
	default:
		throw InternalException("Unimplemented type for UNNEST.");
	}
}

void UnnestOperatorState::PrepareInput(DataChunk &input,
                         const vector<unique_ptr<Expression>> &select_list) {
	list_data.Reset();
	// execute the expressions inside each UNNEST in the select_list to get the list data
	// execution results (lists) are kept in list_data chunk
	executor.Execute(input, list_data);

	// verify incoming lists
	list_data.Verify();
	D_ASSERT(input.size() == list_data.size());
	D_ASSERT(list_data.ColumnCount() == select_list.size());
	D_ASSERT(list_vector_data.size() == list_data.ColumnCount());
	D_ASSERT(list_child_data.size() == list_data.ColumnCount());

	// get the UnifiedVectorFormat of each list_data vector (LIST vectors for the different UNNESTs)
	// both for the vector itself and its child vector
	for (idx_t col_idx = 0; col_idx < list_data.ColumnCount(); col_idx++) {
		auto &list_vector = list_data.data[col_idx];
		list_vector.ToUnifiedFormat(list_data.size(), list_vector_data[col_idx]);

		if (list_vector.GetType() == LogicalType::SQLNULL) {
			// UNNEST(NULL): SQLNULL vectors don't have child vectors, but we need to point to the child vector of
			// each vector, so we just get the UnifiedVectorFormat of the vector itself
			auto &child_vector = list_vector;
			child_vector.ToUnifiedFormat(0, list_child_data[col_idx]);
		} else {
			auto list_size = ListVector::GetListSize(list_vector);
			auto &child_vector = ListVector::GetEntry(list_vector);
			child_vector.ToUnifiedFormat(list_size, list_child_data[col_idx]);
		}
	}
	// get the unnest lengths
	if (list_data.size() > unnest_lengths.size()) {
		unnest_lengths.resize(list_data.size());
	}
	for(idx_t r = 0; r < list_data.size(); r++) {
		unnest_lengths[r] = 0;
	}
	for (idx_t col_idx = 0; col_idx < list_data.ColumnCount(); col_idx++) {
		auto &vector_data = list_vector_data[col_idx];
		for(idx_t r = 0; r < list_data.size(); r++) {
			auto current_idx = vector_data.sel->get_index(r);
			if (!vector_data.validity.RowIsValid(current_idx)) {
				continue;
			}
			// check if this list is longer than the current unnest length
			auto list_data_entries = UnifiedVectorFormat::GetData<list_entry_t>(vector_data);
			auto list_entry = list_data_entries[current_idx];
			if (list_entry.length > unnest_lengths[r]) {
				unnest_lengths[r] = list_entry.length;
			}
		}
	}

	first_fetch = false;
}

unique_ptr<OperatorState> PhysicalUnnest::GetOperatorState(ExecutionContext &context) const {
	return PhysicalUnnest::GetState(context, select_list);
}

unique_ptr<OperatorState> PhysicalUnnest::GetState(ExecutionContext &context,
                                                   const vector<unique_ptr<Expression>> &select_list) {
	return make_uniq<UnnestOperatorState>(context.client, select_list);
}

OperatorResultType PhysicalUnnest::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   OperatorState &state_p,
                                                   const vector<unique_ptr<Expression>> &select_list,
                                                   bool include_input) {

	auto &state = state_p.Cast<UnnestOperatorState>();

	do {
		// prepare the input data by executing any expressions and getting the
		// UnifiedVectorFormat of each LIST vector (list_vector_data) and its child vector (list_child_data)
		if (state.first_fetch) {
			state.PrepareInput(input, select_list);
		}

		// finished with all rows of this input chunk, reset
		if (state.current_row >= input.size()) {
			state.Reset();
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// we essentially create two different SelectionVectors to slice
		// one is for the input (if include_input is set)
		// the other is for the list we are unnesting
		// construct these
		idx_t result_length = 0;
		idx_t unnest_list_count = 0;
		auto initial_row = state.current_row;
		while(result_length < STANDARD_VECTOR_SIZE && state.current_row < input.size()) {
			auto current_row_length = MinValue<idx_t>(STANDARD_VECTOR_SIZE - result_length, state.unnest_lengths[state.current_row] - state.list_position);

			if (current_row_length > 0) {
				// set up the selection vectors
				if (include_input) {
					for(idx_t r = 0; r < current_row_length; r++) {
						state.input_sel.set_index(result_length + r, state.current_row);
					}
				}
				for (idx_t col_idx = 0; col_idx < state.list_data.ColumnCount(); col_idx++) {
					auto &vector_data = state.list_vector_data[col_idx];
					auto current_idx = vector_data.sel->get_index(state.current_row);
					auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(vector_data);
					if (!vector_data.validity.RowIsValid(current_idx)) {
						throw InternalException("This should never be reached");
					}
					auto list_entry = list_data[current_idx];
					if (list_entry.length != current_row_length) {
						throw InternalException("Unnest lists with different lengths should take a different code path");
					}
					auto &unnest_sel = state.unnest_sels[col_idx];
					for(idx_t r = 0; r < current_row_length; r++) {
						unnest_sel.set_index(result_length + r, list_entry.offset + state.list_position + r);
					}
				}

				// move to the next row
				result_length += current_row_length;
				state.list_position += current_row_length;
			}
			unnest_list_count++;
			if (state.list_position == state.unnest_lengths[state.current_row]) {
				state.current_row++;
				state.list_position = 0;
			}
		}
		idx_t col_offset = 0;
		chunk.SetCardinality(result_length);
		if (include_input) {
			for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
				if (unnest_list_count == 1) {
					// everything belongs to the same row - we can do a constant reference
					ConstantVector::Reference(chunk.data[col_idx], input.data[col_idx], initial_row, input.size());
				} else {
					// input values come from different rows - we need to slice
					chunk.data[col_idx].Slice(input.data[col_idx], state.input_sel, result_length);
				}
			}
			col_offset = input.ColumnCount();
		}
		for (idx_t col_idx = 0; col_idx < state.list_data.ColumnCount(); col_idx++) {
			if (state.list_data.data[col_idx].GetType() == LogicalType::SQLNULL) {
				// UNNEST(NULL)
				chunk.SetCardinality(0);
				break;
			}
			auto &child_vector = ListVector::GetEntry(state.list_data.data[col_idx]);
			chunk.data[col_offset + col_idx].Slice(child_vector, state.unnest_sels[col_idx], result_length);
		}
		chunk.Verify();
		//
		//
		//
		// // each UNNEST in the select_list contains a list (or NULL) for this row, find the longest list
		// // we emit chunks of either STANDARD_VECTOR_SIZE or smaller
		// auto this_chunk_len = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.unnest_lengths[state.current_row] - state.list_position);
		// chunk.SetCardinality(this_chunk_len);
		//
		// // if we include other projection input columns, e.g. SELECT 1, UNNEST([1, 2]);, then
		// // we need to add them to the resulting chunk
		// idx_t col_offset = 0;
		// if (include_input) {
		// 	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		// 		ConstantVector::Reference(chunk.data[col_idx], input.data[col_idx], state.current_row, input.size());
		// 	}
		// 	col_offset = input.ColumnCount();
		// }
		//
		// // unnest the lists
		// for (idx_t col_idx = 0; col_idx < state.list_data.ColumnCount(); col_idx++) {
		//
		// 	auto &result_vector = chunk.data[col_idx + col_offset];
		//
		// 	if (state.list_data.data[col_idx].GetType() == LogicalType::SQLNULL) {
		// 		// UNNEST(NULL)
		// 		chunk.SetCardinality(0);
		// 		break;
		// 	}
		//
		// 	auto &vector_data = state.list_vector_data[col_idx];
		// 	auto current_idx = vector_data.sel->get_index(state.current_row);
		//
		// 	if (!vector_data.validity.RowIsValid(current_idx)) {
		// 		UnnestNull(0, this_chunk_len, result_vector);
		// 		continue;
		// 	}
		//
		// 	auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(vector_data);
		// 	auto list_entry = list_data[current_idx];
		//
		// 	idx_t list_count = 0;
		// 	if (state.list_position < list_entry.length) {
		// 		// there are still list_count elements to unnest
		// 		list_count = MinValue<idx_t>(this_chunk_len, list_entry.length - state.list_position);
		//
		// 		auto &list_vector = state.list_data.data[col_idx];
		// 		auto &child_vector = ListVector::GetEntry(list_vector);
		// 		auto list_size = ListVector::GetListSize(list_vector);
		// 		auto &child_vector_data = state.list_child_data[col_idx];
		//
		// 		auto base_offset = list_entry.offset + state.list_position;
		// 		UnnestVector(child_vector_data, child_vector, list_size, base_offset, base_offset + list_count,
		// 		             result_vector);
		// 	}
		//
		// 	// fill the rest with NULLs
		// 	if (list_count != this_chunk_len) {
		// 		UnnestNull(list_count, this_chunk_len, result_vector);
		// 	}
		// }
		//
		// chunk.Verify();
		//
		// state.list_position += this_chunk_len;
		// if (state.list_position == state.unnest_lengths[state.current_row]) {
		// 	state.current_row++;
		// 	state.list_position = 0;
		// }

		// we only emit one unnested row (that contains data) at a time
	} while (chunk.size() == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorResultType PhysicalUnnest::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &, OperatorState &state) const {
	return ExecuteInternal(context, input, chunk, state, select_list);
}

} // namespace duckdb
