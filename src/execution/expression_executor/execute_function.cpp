#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

ExecuteFunctionState::ExecuteFunctionState(const Expression &expr, ExpressionExecutorState &root)
    : ExpressionState(expr, root) {
	// Check if the expression is eligible for dictionary optimization
	if (!expr.IsConsistent() || expr.IsVolatile() || expr.CanThrow()) {
		return; // Needs to be consistent, non-volatile, and non-throwing
	}

	if (expr.return_type.IsNested()) {
		return; // Non-nested types only
	}

	// We can only do this optimization if there is exactly one BOUND_REF child
	idx_t bound_ref_count = 0;
	ExpressionIterator::VisitExpressionClass(expr, ExpressionClass::BOUND_REF,
	                                         [&bound_ref_count](const Expression &) { bound_ref_count++; });
	if (bound_ref_count != 1) {
		return;
	}

	// Set input_col_idx accordingly, marking the expression as eligible for dictionary optimization
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION: {
		auto &bound_function = expr.Cast<BoundFunctionExpression>();
		auto &children = bound_function.children;
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			auto &child = *children[child_idx];
			if (child.GetExpressionClass() == ExpressionClass::BOUND_REF && !child.return_type.IsNested()) {
				input_col_idx = child_idx;
			}
		}
		break;
	}
	default:
		break;
	}
}

ExecuteFunctionState::~ExecuteFunctionState() {
}

bool ExecuteFunctionState::TryExecuteDictionaryExpression(const BoundFunctionExpression &expr, DataChunk &args,
                                                          ExpressionState &state, Vector &result) {
	static constexpr idx_t MAX_DICTIONARY_SIZE_THRESHOLD = 20000;
	static constexpr double CHUNK_FILL_RATIO_THRESHOLD = 0.5;

	if (STANDARD_VECTOR_SIZE < ValidityMask::BITS_PER_VALUE) {
		return false; // This is needed to properly offset the validity mask
	}

	if (!input_col_idx.IsValid()) {
		return false; // This expression is not eligible for dictionary optimization
	}

	// Figure out if we can do the optimization
	const auto &unary_input = args.data[input_col_idx.GetIndex()];
	if (unary_input.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return false; // Not a dictionary
	}

	const auto input_dictionary_size_opt = DictionaryVector::DictionarySize(unary_input);
	if (!input_dictionary_size_opt.IsValid()) {
		return false; // Not a dictionary that comes from the storage
	}
	const auto input_dictionary_size = input_dictionary_size_opt.GetIndex();

	auto input_dictionary_id = DictionaryVector::DictionaryId(unary_input);
	if (input_dictionary_id.empty()) {
		return false; // Dictionary has no id, we can't cache across vectors, bail
	}

	if (input_dictionary_size >= MAX_DICTIONARY_SIZE_THRESHOLD) {
		return false; // Dictionary is too large, bail
	}

	if (input_dictionary_id != dictionary_id) {
		// We haven't seen this dictionary before
		const auto chunk_fill_ratio = static_cast<double>(args.size()) / STANDARD_VECTOR_SIZE;
		if (input_dictionary_size > STANDARD_VECTOR_SIZE && chunk_fill_ratio < CHUNK_FILL_RATIO_THRESHOLD) {
			// If the dictionary size is <= STANDARD_VECTOR_SIZE, we always do the optimization
			// If it's greater, we only do the optimization if the chunk is 50% full or more
			// This protects the optimization against selective filters
			return false;
		}

		// We can do dictionary optimization! Re-initialize
		dictionary_id = std::move(input_dictionary_id);
		dictionary = make_uniq<Vector>(result.GetType(), input_dictionary_size);

		// Set up the input chunk and dictionary
		DataChunk input_chunk;
		input_chunk.InitializeEmpty(args.GetTypes());
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			if (col_idx != input_col_idx.GetIndex()) {
				input_chunk.data[col_idx].Reference(args.data[col_idx]);
			}
		}
		auto &input = DictionaryVector::Child(unary_input);
		auto &input_validity = FlatVector::Validity(input);
		const auto &input_type = input.GetType();
		const auto input_type_width = GetTypeIdSize(input_type.InternalType());

		// Set up the output dictionary
		auto &output = *dictionary;
		auto &output_validity = FlatVector::Validity(output);
		output_validity.SetAllValid(input_dictionary_size);
		const auto &output_type = output.GetType();
		const auto output_type_width = GetTypeIdSize(output_type.InternalType());

		// Loop over the dictionary, executing at most STANDARD_VECTOR_SIZE at a time
		for (idx_t offset = 0; offset < input_dictionary_size; offset += STANDARD_VECTOR_SIZE) {
			const auto count = MinValue<idx_t>(input_dictionary_size - offset, STANDARD_VECTOR_SIZE);
			const auto validity_offset = offset / ValidityMask::BITS_PER_VALUE;

			// Offset the input dictionary
			Vector offset_input(input_type, input.GetData() + offset * input_type_width);
			ValidityMask offset_input_validity(input_validity.GetData() + validity_offset, count);
			FlatVector::SetValidity(offset_input, offset_input_validity);
			input_chunk.data[input_col_idx.GetIndex()].Reference(offset_input);
			input_chunk.SetCardinality(count);

			// Offset the output
			Vector offset_output(output_type, output.GetData() + offset * output_type_width);
			ValidityMask offset_result_validity(output_validity.GetData() + validity_offset, count);
			FlatVector::SetValidity(offset_output, offset_result_validity);

			// Execute, while ensuring the aux gets shared properly
			offset_output.SetAuxiliary(output.GetAuxiliary());
			expr.function.function(input_chunk, state, offset_output);
			output.SetAuxiliary(offset_output.GetAuxiliary());

			if (offset_output.GetVectorType() == VectorType::CONSTANT_VECTOR) {
				return false; // Function always returns CONSTANT, bail
			}

			// Output validity may change during function execution, this copies it back over
			const auto function_validity_data = FlatVector::Validity(offset_output).GetData();
			const auto offset_result_validity_data = offset_result_validity.GetData();
			if (function_validity_data != offset_result_validity_data) {
				const auto entry_count = ValidityData::EntryCount(count);
				for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
					offset_result_validity_data[entry_idx] = function_validity_data[entry_idx];
				}
			}
		}
	}

	// Create a dictionary result vector and give it an ID
	const auto &input_sel_vector = DictionaryVector::SelVector(unary_input);
	result.Dictionary(*dictionary, input_dictionary_size, input_sel_vector, args.size());
	if (result.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// Result can be non-dictionary if args.size() == 1, so we need to check before doing this
		DictionaryVector::SetDictionaryId(result, dictionary_id);
	}

	return true;
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExecuteFunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(*child);
	}

	result->Finalize();
	if (expr.function.init_local_state) {
		result->local_state = expr.function.init_local_state(*result, expr, expr.bind_info.get());
	}
	return std::move(result);
}

static void VerifyNullHandling(const BoundFunctionExpression &expr, DataChunk &args, Vector &result) {
#ifdef DEBUG
	if (args.data.empty() || expr.function.null_handling != FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return;
	}

	// Combine all the argument validity masks into a flat validity mask
	idx_t count = args.size();
	ValidityMask combined_mask(count);
	for (auto &arg : args.data) {
		UnifiedVectorFormat arg_data;
		arg.ToUnifiedFormat(count, arg_data);

		for (idx_t i = 0; i < count; i++) {
			auto idx = arg_data.sel->get_index(i);
			if (!arg_data.validity.RowIsValid(idx)) {
				combined_mask.SetInvalid(i);
			}
		}
	}

	// Default is that if any of the arguments are NULL, the result is also NULL
	UnifiedVectorFormat result_data;
	result.ToUnifiedFormat(count, result_data);
	for (idx_t i = 0; i < count; i++) {
		if (!combined_mask.RowIsValid(i)) {
			auto idx = result_data.sel->get_index(i);
			D_ASSERT(!result_data.validity.RowIsValid(idx));
		}
	}
#endif
}

void ExpressionExecutor::Execute(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	state->intermediate_chunk.Reset();
	auto &arguments = state->intermediate_chunk;
	if (!state->types.empty()) {
		for (idx_t i = 0; i < expr.children.size(); i++) {
			D_ASSERT(state->types[i] == expr.children[i]->return_type);
			Execute(*expr.children[i], state->child_states[i].get(), sel, count, arguments.data[i]);
#ifdef DEBUG
			if (expr.children[i]->return_type.id() == LogicalTypeId::VARCHAR) {
				arguments.data[i].UTFVerify(count);
			}
#endif
		}
	}
	arguments.SetCardinality(count);
	arguments.Verify();

	D_ASSERT(expr.function.function);
	auto &execute_function_state = state->Cast<ExecuteFunctionState>();
	if (!execute_function_state.TryExecuteDictionaryExpression(expr, arguments, *state, result)) {
		expr.function.function(arguments, *state, result);
	}

	VerifyNullHandling(expr, arguments, result);
	D_ASSERT(result.GetType() == expr.return_type);
}

} // namespace duckdb
