#include "duckdb/common/type_visitor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

ExecuteFunctionState::ExecuteFunctionState(const Expression &expr, ExpressionExecutorState &root)
    : ExpressionState(expr, root) {
	// Check if the expression is eligible for dictionary optimization
	if (!expr.IsConsistent() || expr.IsVolatile() || expr.CanThrow()) {
		return; // Needs to be consistent, non-volatile, and non-throwing
	}

	if (expr.GetReturnType().InternalType() == PhysicalType::STRUCT) {
		return; // FIXME: get this working for STRUCT
	}

	// Set input_col_idx accordingly, marking the expression as eligible for dictionary optimization
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION: {
		auto &bound_function = expr.Cast<BoundFunctionExpression>();
		auto &children = bound_function.children;
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			auto &child = *children[child_idx];
			if (child.IsFoldable()) {
				continue; // Constant
			}
			if (input_col_idx.IsValid()) {
				input_col_idx.SetInvalid(); // Found more than 1 non-constant
				break;
			}
			if (child.GetReturnType().InternalType() == PhysicalType::STRUCT) {
				break; // FIXME
			}
			input_col_idx = child_idx;
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

	if (!input_col_idx.IsValid()) {
		return false; // This expression is not eligible for dictionary optimization
	}

	// Figure out if we can do the optimization
	const auto &unary_input = args.data[input_col_idx.GetIndex()];
	if (unary_input.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return false; // Not a dictionary
	}

	const auto input_dictionary_size_opt = DictionaryVector::DictionarySize(unary_input);
	const auto &input_dictionary_id = DictionaryVector::DictionaryId(unary_input);
	if (!input_dictionary_size_opt.IsValid() || input_dictionary_id.empty()) {
		return false; // Not a dictionary that comes from storage
	}

	const auto input_dictionary_size = input_dictionary_size_opt.GetIndex();
	if (input_dictionary_size >= MAX_DICTIONARY_SIZE_THRESHOLD) {
		return false; // Dictionary is too large, bail
	}

	if (!output_dictionary || current_input_dictionary_id != input_dictionary_id) {
		// We haven't seen this dictionary before
		const auto chunk_fill_ratio = static_cast<double>(args.size()) / STANDARD_VECTOR_SIZE;
		if (input_dictionary_size > STANDARD_VECTOR_SIZE && chunk_fill_ratio <= CHUNK_FILL_RATIO_THRESHOLD) {
			// If the dictionary size is <= STANDARD_VECTOR_SIZE, we always do the optimization
			// If it's greater, we only do the optimization if the chunk is more than 50% full
			// This protects the optimization against selective filters
			return false;
		}

		// We can do dictionary optimization! Re-initialize
		output_dictionary = DictionaryVector::CreateReusableDictionary(result.GetType(), input_dictionary_size);
		current_input_dictionary_id = input_dictionary_id;

		// Set up the input chunk
		DataChunk input_chunk;
		input_chunk.InitializeEmpty(args.GetTypes());
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			if (col_idx != input_col_idx.GetIndex()) {
				input_chunk.data[col_idx].Reference(args.data[col_idx]);
			}
		}

		// Loop over the dictionary, executing at most STANDARD_VECTOR_SIZE at a time
		for (idx_t offset = 0; offset < input_dictionary_size; offset += STANDARD_VECTOR_SIZE) {
			const auto count = MinValue<idx_t>(input_dictionary_size - offset, STANDARD_VECTOR_SIZE);

			// Offset the input dictionary
			Vector offset_input(DictionaryVector::Child(unary_input), offset, offset + count);
			input_chunk.data[input_col_idx.GetIndex()].Reference(offset_input);
			input_chunk.SetCardinality(count);

			// Execute, storing the result in an intermediate vector, and copying it to the output dictionary
			Vector output_intermediate(result.GetType());
			expr.function.GetFunctionCallback()(input_chunk, state, output_intermediate);
			VectorOperations::Copy(output_intermediate, output_dictionary->data, count, 0, offset);
		}
	}

	// Result references the dictionary
	result.Dictionary(output_dictionary, DictionaryVector::SelVector(unary_input), args.size());

	return true;
}

void ExecuteFunctionState::ResetDictionaryStates() {
	// Clear the cached dictionary information
	current_input_dictionary_id.clear();
	output_dictionary.reset();

	for (const auto &child_state : child_states) {
		child_state->ResetDictionaryStates();
	}
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExecuteFunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(*child);
	}

	result->Finalize();
	if (expr.function.HasInitStateCallback()) {
		result->local_state = expr.function.GetInitStateCallback()(*result, expr, expr.bind_info.get());
	}
	return std::move(result);
}

static void VerifyNullHandling(const BoundFunctionExpression &expr, DataChunk &args, Vector &result) {
#ifdef DEBUG
	if (args.data.empty() || expr.function.GetNullHandling() != FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return;
	}

	// Combine all the argument validity masks into a flat validity mask
	idx_t count = args.size();
	ValidityMask combined_mask(count);
	for (auto &arg : args.data) {
		auto entries = arg.Validity(count);
		if (!entries.CanHaveNull()) {
			continue;
		}
		for (idx_t i = 0; i < count; i++) {
			if (!entries.IsValid(i)) {
				combined_mask.SetInvalid(i);
			}
		}
	}

	// Default is that if any of the arguments are NULL, the result is also NULL
	auto result_validity = result.Validity(count);
	for (idx_t i = 0; i < count; i++) {
		if (!combined_mask.RowIsValid(i)) {
			D_ASSERT(!result_validity.IsValid(i));
		}
	}
#endif
}

static void ExecuteSelectFunction(const BoundFunctionExpression &expr, DataChunk &args, ExpressionState &state,
                                  Vector &result) {
	if (expr.GetReturnType() != LogicalType::BOOLEAN) {
		throw InvalidInputException("Function %s only has a select callback but returns %s", expr.function.name,
		                            expr.GetReturnType().ToString());
	}
	if (expr.function.GetNullHandling() == FunctionNullHandling::SPECIAL_HANDLING) {
		throw InvalidInputException("Function %s only has a select callback with SPECIAL_HANDLING but projected "
		                            "execution requires a scalar callback to produce NULL results",
		                            expr.function.name);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto count = args.size();
	auto result_data = FlatVector::GetDataMutable<bool>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = false;
	}

	auto &result_validity = FlatVector::ValidityMutable(result);
	result_validity.SetAllValid(count);
	D_ASSERT(expr.function.GetNullHandling() == FunctionNullHandling::DEFAULT_NULL_HANDLING);
	for (auto &arg : args.data) {
		auto entries = arg.Validity(count);
		if (!entries.CanHaveNull()) {
			continue;
		}
		for (idx_t i = 0; i < count; i++) {
			if (!entries.IsValid(i)) {
				result_validity.SetInvalid(i);
			}
		}
	}

	SelectionVector true_sel(count);
	auto true_count = expr.function.GetSelectCallback()(args, state, &true_sel, nullptr);
	for (idx_t i = 0; i < true_count; i++) {
		result_data[true_sel.get_index(i)] = true;
	}
}

void ExpressionExecutor::Execute(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	state->intermediate_chunk.Reset();
	auto &arguments = state->intermediate_chunk;
	// if the input is constant and there function is non-volatile we only need to run it on one value
	bool all_constant = true;
	if (expr.function.GetStability() == FunctionStability::VOLATILE) {
		// we cannot optimize away constant vectors for volatile functions
		all_constant = false;
	}
	auto default_null_handling = expr.function.GetNullHandling() == FunctionNullHandling::DEFAULT_NULL_HANDLING;
	if (!state->types.empty()) {
		for (idx_t i = 0; i < expr.children.size(); i++) {
			D_ASSERT(state->types[i] == expr.children[i]->GetReturnType());
			Execute(*expr.children[i], state->child_states[i].get(), sel, count, arguments.data[i]);
			if (arguments.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
				all_constant = false;
			} else if (default_null_handling && ConstantVector::IsNull(arguments.data[i])) {
				// constant NULL input: result is NULL
				ConstantVector::SetNull(result, count_t(count));
				return;
			}
		}
	}
	arguments.SetCardinality(all_constant ? 1 : count);
	arguments.Verify(context ? context->db : nullptr);

	auto &execute_function_state = state->Cast<ExecuteFunctionState>();
	auto dictionary_executed = expr.function.HasFunctionCallback() && !all_constant &&
	                           execute_function_state.TryExecuteDictionaryExpression(expr, arguments, *state, result);
	if (expr.function.HasFunctionCallback() && !dictionary_executed) {
		expr.function.GetFunctionCallback()(arguments, *state, result);
	} else if (expr.function.HasSelectCallback()) {
		ExecuteSelectFunction(expr, arguments, *state, result);
	} else if (dictionary_executed) {
		D_ASSERT(expr.function.HasFunctionCallback());
	} else {
		throw InternalException("Scalar function %s has neither an execution nor a select callback",
		                        expr.function.name);
	}
	if (all_constant) {
		if (result.GetVectorType() != VectorType::FLAT_VECTOR &&
		    result.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			throw InternalException(
			    "Error while executing function %s - function must return a flat or constant vector for count = 1",
			    expr.function.name);
		}
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	VerifyNullHandling(expr, arguments, result);
	D_ASSERT(result.GetType() == expr.GetReturnType());
}

static void ScatterSelectionResult(const SelectionVector &source, idx_t source_count, const SelectionVector *sel,
                                   SelectionVector *target) {
	if (!target) {
		return;
	}
	for (idx_t i = 0; i < source_count; i++) {
		auto idx = source.get_index(i);
		target->set_index(i, sel ? sel->get_index(idx) : idx);
	}
}

idx_t ExpressionExecutor::Select(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	if (!expr.function.HasSelectCallback()) {
		return DefaultSelect(expr, state, sel, count, true_sel, false_sel);
	}
	// FIXME: push constant handling in here
	state->intermediate_chunk.Reset();
	auto &arguments = state->intermediate_chunk;
	for (idx_t i = 0; i < expr.children.size(); i++) {
		D_ASSERT(state->types[i] == expr.children[i]->GetReturnType());
		Execute(*expr.children[i], state->child_states[i].get(), sel, count, arguments.data[i]);
	}
	arguments.SetCardinality(count);
	arguments.Verify(context ? context->db : nullptr);

	const bool has_sel = sel && sel != FlatVector::IncrementalSelectionVector();
	if (!has_sel) {
		return expr.function.GetSelectCallback()(arguments, *state, true_sel, false_sel);
	}

	SelectionVector temp_true(count);
	SelectionVector temp_false(count);
	auto dense_true_sel = true_sel ? &temp_true : nullptr;
	auto dense_false_sel = false_sel ? &temp_false : nullptr;
	auto true_count = expr.function.GetSelectCallback()(arguments, *state, dense_true_sel, dense_false_sel);
	ScatterSelectionResult(temp_true, true_count, sel, true_sel);
	if (false_sel) {
		auto false_count = count - true_count;
		ScatterSelectionResult(temp_false, false_count, sel, false_sel);
	}
	return true_count;
}

} // namespace duckdb
