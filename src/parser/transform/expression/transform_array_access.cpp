#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

void Transformer::DetermineSliceBounds(duckdb_libpgquery::PGNode *idx, unique_ptr<ParsedExpression> &bound,
                                       int64_t limit) {
	if (idx) {
		bound = TransformExpression(idx);
		if (bound->type == ExpressionType::VALUE_CONSTANT) {
			auto &const_expr_bound = bound->Cast<ConstantExpression>();
			if (!const_expr_bound.value.IsNull() && const_expr_bound.value.type() == LogicalType::BIGINT &&
			    (const_expr_bound.value.template GetValue<int64_t>() == NumericLimits<int64_t>::Minimum() ||
			     const_expr_bound.value.template GetValue<int64_t>() == NumericLimits<int64_t>::Maximum())) {
				throw ParserException("The lower and/or upper bound of a slice cannot be a numeric limit. Consider "
				                      "leaving it empty or using a different value.");
			}
		}
	} else {
		bound = make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
	}
}

unique_ptr<ParsedExpression> Transformer::TransformArrayAccess(duckdb_libpgquery::PGAIndirection &indirection_node) {
	// transform the source expression
	unique_ptr<ParsedExpression> result;
	result = TransformExpression(indirection_node.arg);

	// now go over the indices
	// note that a single indirection node can contain multiple indices
	// this happens for e.g. more complex accesses (e.g. (foo).field1[42])
	idx_t list_size = 0;
	for (auto node = indirection_node.indirection->head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		D_ASSERT(target);

		switch (target->type) {
		case duckdb_libpgquery::T_PGAIndices: {
			// index access (either slice or extract)
			auto index = PGPointerCast<duckdb_libpgquery::PGAIndices>(target);
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(std::move(result));
			if (index->is_slice) {
				// slice
				// if either the lower or upper bound is not specified, we use the minimum/maximum value so that we can
				// handle it in the execution
				unique_ptr<ParsedExpression> lower;
				DetermineSliceBounds(index->lidx, lower, NumericLimits<int64_t>::Minimum());
				children.push_back(std::move(lower));
				unique_ptr<ParsedExpression> upper;
				DetermineSliceBounds(index->uidx, upper, NumericLimits<int64_t>::Maximum());
				children.push_back(std::move(upper));
				if (index->step) {
					children.push_back(TransformExpression(index->step));
				}
				result = make_uniq<OperatorExpression>(ExpressionType::ARRAY_SLICE, std::move(children));
			} else {
				// array access
				D_ASSERT(!index->lidx);
				D_ASSERT(index->uidx);
				children.push_back(TransformExpression(index->uidx));
				result = make_uniq<OperatorExpression>(ExpressionType::ARRAY_EXTRACT, std::move(children));
			}
			break;
		}
		case duckdb_libpgquery::T_PGString: {
			auto val = PGPointerCast<duckdb_libpgquery::PGValue>(target);
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(std::move(result));
			children.push_back(TransformValue(*val));
			result = make_uniq<OperatorExpression>(ExpressionType::STRUCT_EXTRACT, std::move(children));
			break;
		}
		case duckdb_libpgquery::T_PGFuncCall: {
			auto func = PGPointerCast<duckdb_libpgquery::PGFuncCall>(target);
			auto function = TransformFuncCall(*func);
			if (function->type != ExpressionType::FUNCTION) {
				throw ParserException("%s.%s() call must be a function", result->ToString(), function->ToString());
			}
			auto &f = function->Cast<FunctionExpression>();
			f.children.insert(f.children.begin(), std::move(result));
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
