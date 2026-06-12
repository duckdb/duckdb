#include "duckdb/common/enums/debug_verification_mode.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

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
		auto &children = bound_function.GetChildren();
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
			input_chunk.SetChildCardinality(count);

			// Execute, storing the result in an intermediate vector, and copying it to the output dictionary
			Vector output_intermediate(result.GetType());
			expr.Function().GetFunctionCallback()(input_chunk, state, output_intermediate);
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
	for (auto &child : expr.GetChildren()) {
		result->AddChild(*child);
	}

	result->Finalize();
	if (expr.Function().HasInitStateCallback()) {
		result->local_state = expr.Function().GetInitStateCallback()(*result, expr, expr.BindInfo().get());
	}
	return std::move(result);
}

static void VerifyNullHandling(const BoundFunctionExpression &expr, DataChunk &args, Vector &result) {
	if (DBConfigOptions::global_verification_mode != DebugVerificationMode::VERIFY_FUNCTIONS) {
		return;
	}
	if (args.data.empty() || expr.Function().GetNullHandling() != FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return;
	}

	// Combine all the argument validity masks into a flat validity mask
	idx_t count = args.size();
	ValidityMask combined_mask(count);
	for (const auto &arg : args.data) {
		auto entries = arg.Validity();
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
	auto result_validity = result.Validity();
	for (idx_t i = 0; i < count; i++) {
		if (!combined_mask.RowIsValid(i) && result_validity.IsValid(i)) {
			throw InternalException(
			    "VerifyNullHandling failed for scalar function \"%s\": row %d has a NULL argument but the result is "
			    "not NULL - functions with default NULL handling should return NULL for any NULL input",
			    expr.Function().GetName(), i);
		}
	}
}

static idx_t SelectBooleanResult(Vector &result, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	return UnaryExecutor::Select<bool>(
	    result, sel, count, [](bool value) { return value; }, true_sel, false_sel);
}

static void ExecuteConstantSelectFunction(const BoundFunctionExpression &expr, DataChunk &args, ExpressionState &state,
                                          Vector &result) {
	D_ASSERT(args.size() == 1);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::Validity(result).SetAllValid(1);

	SelectionVector true_sel(1);
	SelectionVector false_sel(1);
	auto true_count = expr.Function().GetSelectCallback()(args, state, nullptr, &true_sel, &false_sel);
	*ConstantVector::GetData<bool>(result) = true_count == 1;
}

void ExpressionExecutor::Execute(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	state->intermediate_chunk.Reset();
	auto &arguments = state->intermediate_chunk;
	// if the input is constant and there function is non-volatile we only need to run it on one value
	bool all_constant = true;
	if (expr.Function().GetStability() == FunctionStability::VOLATILE) {
		// we cannot optimize away constant vectors for volatile functions
		all_constant = false;
	}
	auto default_null_handling = expr.Function().GetNullHandling() == FunctionNullHandling::DEFAULT_NULL_HANDLING;
	if (!state->types.empty()) {
		for (idx_t i = 0; i < expr.GetChildren().size(); i++) {
			D_ASSERT(state->types[i] == expr.GetChildren()[i]->GetReturnType());
			Execute(*expr.GetChildren()[i], state->child_states[i].get(), sel, count, arguments.data[i]);
			if (arguments.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
				all_constant = false;
			} else if (default_null_handling && ConstantVector::IsNull(arguments.data[i])) {
				// constant NULL input: result is NULL
				ConstantVector::SetNull(result, count_t(count));
				return;
			}
		}
	}
	if (all_constant) {
		// if all arguments are constant temporarily set the child cardinality to 1
		arguments.SetChildCardinality(1ULL);
	} else {
		arguments.SetChildCardinality(count);
	}
	arguments.Verify(context);

	auto &execute_function_state = state->Cast<ExecuteFunctionState>();
	auto dictionary_executed = expr.Function().HasFunctionCallback() && !all_constant &&
	                           execute_function_state.TryExecuteDictionaryExpression(expr, arguments, *state, result);
	if (!dictionary_executed) {
		if (expr.Function().HasFunctionCallback()) {
			expr.Function().GetFunctionCallback()(arguments, *state, result);
		} else if (all_constant && expr.Function().HasSelectCallback()) {
			ExecuteConstantSelectFunction(expr, arguments, *state, result);
		} else {
			throw InternalException("Scalar function %s has no execution callback", expr.Function().GetName());
		}
	}
	if (all_constant) {
		// restore the input cardinality
		for (auto &arg : arguments.data) {
			arg.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		arguments.SetChildCardinality(count);
		// ensure the result type is constant
		result.FlattenAndSetConstant();
	}
	FlatVector::SetSize(result, count_t(count));

	VerifyNullHandling(expr, arguments, result);
	D_ASSERT(result.GetType() == expr.GetReturnType());
}

idx_t ExpressionExecutor::Select(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	if (!expr.Function().HasSelectCallback()) {
		return DefaultSelect(expr, state, sel, count, true_sel, false_sel);
	}
	state->intermediate_chunk.Reset();
	auto &arguments = state->intermediate_chunk;
	bool all_constant = true;
	if (expr.Function().GetStability() == FunctionStability::VOLATILE) {
		all_constant = false;
	}
	auto default_null_handling = expr.Function().GetNullHandling() == FunctionNullHandling::DEFAULT_NULL_HANDLING;
	for (idx_t i = 0; i < expr.GetChildren().size(); i++) {
		D_ASSERT(state->types[i] == expr.GetChildren()[i]->GetReturnType());
		Execute(*expr.GetChildren()[i], state->child_states[i].get(), sel, count, arguments.data[i]);
		if (arguments.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			all_constant = false;
		} else if (default_null_handling && ConstantVector::IsNull(arguments.data[i])) {
			Vector result(LogicalType::BOOLEAN);
			ConstantVector::SetNull(result, count_t(1));
			return SelectBooleanResult(result, sel, count, true_sel, false_sel);
		}
	}
	if (all_constant) {
		// if all arguments are constant we only need to run the function on one value
		arguments.SetChildCardinality(1ULL);
	} else {
		arguments.SetChildCardinality(count);
	}
	arguments.Verify(context);
	if (all_constant) {
		Vector result(LogicalType::BOOLEAN);
		if (expr.Function().HasFunctionCallback()) {
			expr.Function().GetFunctionCallback()(arguments, *state, result);
			result.FlattenAndSetConstant();
		} else {
			ExecuteConstantSelectFunction(expr, arguments, *state, result);
		}
		// restore the input cardinality
		for (auto &arg : arguments.data) {
			arg.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		arguments.SetChildCardinality(count);
		return SelectBooleanResult(result, sel, count, true_sel, false_sel);
	}
	auto &execute_function_state = state->Cast<ExecuteFunctionState>();
	if (expr.Function().HasFunctionCallback()) {
		Vector result(LogicalType::BOOLEAN);
		auto dictionary_executed =
		    execute_function_state.TryExecuteDictionaryExpression(expr, arguments, *state, result);
		if (dictionary_executed) {
			return SelectBooleanResult(result, sel, count, true_sel, false_sel);
		}
	}
	return expr.Function().GetSelectCallback()(arguments, *state, sel, true_sel, false_sel);
}

} // namespace duckdb
