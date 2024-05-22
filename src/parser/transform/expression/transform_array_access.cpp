#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformArrayAccess(duckdb_libpgquery::PGAIndirection &indirection_node) {

	// Transform the source expression.
	unique_ptr<ParsedExpression> result;
	result = TransformExpression(indirection_node.arg);

	// Iterate the indices.
	// For more complex expressions like (foo).field_name[42] a single indirection
	// node can contain multiple indices.
	idx_t list_size = 0;
	for (auto node = indirection_node.indirection->head; node != nullptr; node = node->next) {
		optional_ptr<duckdb_libpgquery::PGNode> t2 = PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);

		switch (t2->type) {
		case duckdb_libpgquery::T_PGAIndices: {
			// Index access.
			auto i2 = PGCast<duckdb_libpgquery::PGAIndices>(*t2.get());
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(std::move(result));

			if (i2.is_slice) {
				// If either the lower or upper bound is not specified, we use an empty constant LIST,
				// which we handle in the execution.
				auto constant_list = make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));

				auto lower = i2.lidx ? TransformExpression(i2.lidx) : constant_list->Copy();
				children.push_back(std::move(lower));
				auto upper = i2.uidx ? TransformExpression(i2.uidx) : constant_list->Copy();
				children.push_back(std::move(upper));

				if (i2.step) {
					children.push_back(TransformExpression(i2.step));
				}
				result = make_uniq<OperatorExpression>(ExpressionType::ARRAY_SLICE, std::move(children));
				break;
			}

			// Array access.
			D_ASSERT(!i2.lidx && i2.uidx);
			children.push_back(TransformExpression(i2.uidx));
			result = make_uniq<OperatorExpression>(ExpressionType::ARRAY_EXTRACT, std::move(children));
			break;
		}
		case duckdb_libpgquery::T_PGString: {
			auto value = PGCast<duckdb_libpgquery::PGValue>(*t2.get());
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(std::move(result));
			children.push_back(TransformValue(value));
			result = make_uniq<OperatorExpression>(ExpressionType::STRUCT_EXTRACT, std::move(children));
			break;
		}
		case duckdb_libpgquery::T_PGFuncCall: {
			auto func = PGCast<duckdb_libpgquery::PGFuncCall>(*t2.get());
			auto function = TransformFuncCall(func);
			if (function->type != ExpressionType::FUNCTION) {
				throw ParserException("%s.%s() call must be a function", result->ToString(), function->ToString());
			}
			auto &function_expr = function->Cast<FunctionExpression>();
			function_expr.children.insert(function_expr.children.begin(), std::move(result));
			result = std::move(function);
			break;
		}
		default:
			throw NotImplementedException("Unimplemented subscript type");
		}

		list_size++;
		StackCheck(list_size);
	}
	return result;
}

} // namespace duckdb
