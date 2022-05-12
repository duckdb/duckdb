#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

struct ListTransformBindData : public FunctionData {
	ListTransformBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr);
	~ListTransformBindData() override;

	LogicalType stype;
	unique_ptr<Expression> lambda_expr;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

ListTransformBindData::ListTransformBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr_p)
    : stype(stype_p), lambda_expr(move(lambda_expr_p)) {
}

unique_ptr<FunctionData> ListTransformBindData::Copy() const {
	return make_unique<ListTransformBindData>(stype, lambda_expr->Copy());
}

bool ListTransformBindData::Equals(const FunctionData &other_p) const {
	auto &other = (ListTransformBindData &)other_p;
	return lambda_expr->Equals(other.lambda_expr.get());
}

ListTransformBindData::~ListTransformBindData() {
}

static void ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	// always at least the list argument
	auto col_count = args.ColumnCount();
	D_ASSERT(col_count >= 1);

	auto count = args.size();
	Vector &lists = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
	    result_validity.SetInvalid(0);
	    return;
	}

	// get the lists data
	VectorData lists_data;
	lists.Orrify(count, lists_data);
	auto list_entries = (list_entry_t *)lists_data.data;

	// get the lambda expression
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ListTransformBindData &)*func_expr.bind_info;
	auto &lambda_expr = info.lambda_expr;

	// get the child vector and child data
	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);
	VectorData child_data;
	child_vector.Orrify(lists_size, child_data);

	// to slice the child vector
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	// non-lambda parameter columns
	vector<VectorData> columns;
	vector<idx_t> indexes;
	vector<LogicalType> types;
	vector<LogicalType> result_types;
	vector<SelectionVector> sel_vectors;

	types.push_back(child_vector.GetType());
	result_types.push_back(lambda_expr->return_type);

	for (idx_t i = 1; i < col_count; i++) { // skip the list column
		columns.emplace_back(VectorData());
		args.data[i].Orrify(count, columns[i - 1]);
		indexes.push_back(0);
		types.push_back(args.data[i].GetType());
		sel_vectors.emplace_back(SelectionVector(STANDARD_VECTOR_SIZE));
	}

	// get the expression executor
	ExpressionExecutor expr_executor(*lambda_expr);

	// loop over the child entries and create chunks to be executed by the expression executor
	idx_t element_count = 0;
	idx_t offset = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {

		auto lists_index = lists_data.sel->get_index(row_idx);
		const auto &list_entry = list_entries[lists_index];

		// set the result to NULL for this row
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(row_idx);
			continue;
		}

		result_entries[row_idx].offset = offset;
		result_entries[row_idx].length = list_entry.length;

		// empty list, nothing to execute
		if (list_entry.length == 0) {
			continue;
		}

		// get the data indexes
		for (idx_t col_idx = 0; col_idx < col_count - 1; col_idx++) {
			indexes[col_idx] = columns[col_idx].sel->get_index(row_idx);
		}

		// iterate list elements and create transformed expression columns
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {

			// reached STANDARD_VECTOR_SIZE elements, execute
			if (element_count == STANDARD_VECTOR_SIZE) {

				// TODO: same as at the end of the for-loop
				element_count = 0;
			}

			// to slice the child vector
			auto source_idx = child_data.sel->get_index(list_entry.offset + child_idx);
			sel.set_index(element_count, source_idx);

			// for each column, set the index of the selection vector to slice properly
			for (idx_t col_idx = 0; col_idx < col_count - 1; col_idx++) {
				sel_vectors[col_idx].set_index(element_count, indexes[col_idx]);
			}
			element_count++;
		}

		offset += result_entries[row_idx].length;
	}

	if (element_count != 0) {

		// create the input chunk
		DataChunk input_chunk;
		input_chunk.InitializeEmpty(types);

		// set the list child vector
		Vector slice(child_vector, sel, element_count);
		input_chunk.data[0].Reference(slice);

		// set the other vectors
		vector<Vector> slices;
		for (idx_t col_idx = 0; col_idx < col_count - 1; col_idx++) {
			slices.push_back(Vector(args.data[col_idx + 1], sel_vectors[col_idx], element_count));
			input_chunk.data[col_idx + 1].Reference(slices[col_idx]);
		}
		input_chunk.SetCardinality(element_count);

		// create the result chunk
		DataChunk lambda_chunk;
		lambda_chunk.Initialize(result_types);
		lambda_chunk.SetCardinality(element_count);

		// execute the lambda expression and append the result to the result list
		expr_executor.Execute(input_chunk, lambda_chunk);

		auto &lambda_vector = lambda_chunk.data[0];
		VectorData lambda_child_data;
		lambda_vector.Orrify(element_count, lambda_child_data);
		ListVector::Append(result, lambda_vector, *lambda_child_data.sel, element_count, 0);

	}
}

static void TransformExpression(unique_ptr<Expression> &original, unique_ptr<Expression> &replacement,
                                ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
                                LogicalType &list_child_type) {

	// check if the original expression is a lambda parameter
	bool is_lambda_parameter = false;
	if (original->expression_class == ExpressionClass::BOUND_COLUMN_REF) {

		// TODO: is there a better way to determine if this is the lambda parameter, maybe using __lambda_internal_...?
		auto &bound_col_ref = (BoundColumnRefExpression &)*original;
		if (bound_col_ref.binding.table_index == DConstants::INVALID_INDEX) {
			is_lambda_parameter = true;
		}
	}

	if (is_lambda_parameter) {
		// this is a lambda parameter, so the replacement refers to the first argument, which is the list
		replacement = make_unique<BoundReferenceExpression>(arguments[0]->alias, list_child_type, 0);

	} else {
		// this is not a lambda parameter, so we need to create a new argument for the arguments vector
		replacement = make_unique<BoundReferenceExpression>(original->alias, original->return_type, arguments.size());
		bound_function.arguments.push_back(original->return_type);
		arguments.push_back(move(original));
	}
}

static unique_ptr<FunctionData> ListTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// the list column and the lambda function
	D_ASSERT(bound_function.arguments.size() == 2);
	D_ASSERT(arguments.size() == 2);

	bound_function.return_type = LogicalType::LIST(arguments[1]->return_type);

	// remove the lambda function and add the column references as arguments
	auto lambda_expr = move(arguments.back());
	arguments.pop_back();
	bound_function.arguments.pop_back();

	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}

	D_ASSERT(arguments[0]->return_type.id() == LogicalTypeId::LIST);
	auto list_child_type = ListType::GetChildType(arguments[0]->return_type);

	// TODO: other expression classes that do not have children
	// TODO: see of there are tests where lambda_expr or child (as lambda parameter) have this class
	/*
		case ExpressionClass::BOUND_CONSTANT:
		case ExpressionClass::BOUND_DEFAULT:
		case ExpressionClass::BOUND_PARAMETER:
		case ExpressionClass::BOUND_REF:
	*/

	// if the lambda expression does not have any children, transform directly
	if (lambda_expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {

		// move the lambda expr because we are going to replace it
		auto original = move(lambda_expr);
		unique_ptr<Expression> replacement;

		TransformExpression(original, replacement, bound_function, arguments, list_child_type);

		// now we replace the child
		lambda_expr = move(replacement);

	} else {
		// enumerate the children of the lambda expression
		ExpressionIterator::EnumerateChildren(*lambda_expr,
		                                      [&](unique_ptr<Expression> &child) {

                // TODO: see (when writing tests) if these this expression class is enough
		        // TODO: in which case would the lambda expression be a BOUND_CONSTANT?
		        // skip all other expression classes
                if (child->expression_class == ExpressionClass::BOUND_CONSTANT ||
			        child->expression_class == ExpressionClass::BOUND_COLUMN_REF) {

			        // move the child expr because we are going to replace it
                    auto original = move(child);
                    unique_ptr<Expression> replacement;

				    TransformExpression(original, replacement, bound_function, arguments, list_child_type);

                    // now we replace the child
                    child = move(replacement);
                }
           ;}
		);
	}

	// now the lambda expression has been modified, put it in the function data to use it during execution
	return make_unique<ListTransformBindData>(bound_function.return_type, move(lambda_expr));
}

ScalarFunction ListTransformFun::GetFunction() {
	// TODO: what logical type is the lambda function?
	// after the bind this is any, because it is the return value of the rhs?
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, LogicalType::LIST(LogicalType::ANY),
	                      ListTransformFunction, false, false, ListTransformBind, nullptr);
}

void ListTransformFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_transform", "array_transform"}, GetFunction());
}

} // namespace duckdb