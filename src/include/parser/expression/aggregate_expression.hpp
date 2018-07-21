
#pragma once

#include "common/exception.hpp"

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
class AggregateExpression : public AbstractExpression {
  public:
	AggregateExpression(ExpressionType type, bool distinct,
	                    std::unique_ptr<AbstractExpression> child)
	    : AbstractExpression(type) {
		this->distinct = distinct;
		switch (type) {
		case ExpressionType::AGGREGATE_COUNT:
			if (child && child->GetExpressionType() == ExpressionType::STAR) {
				child = nullptr;
				type = ExpressionType::AGGREGATE_COUNT_STAR;
				expr_name = "count(*)";
			} else {
				expr_name = "count";
			}
			break;
		case ExpressionType::AGGREGATE_SUM:
			expr_name = "sum";
			break;
		case ExpressionType::AGGREGATE_MIN:
			expr_name = "min";
			break;
		case ExpressionType::AGGREGATE_MAX:
			expr_name = "max";
			break;
		case ExpressionType::AGGREGATE_AVG:
			expr_name = "avg";
			break;
		default:
			throw Exception("Aggregate type not supported");
		}
		if (child) {
			children.push_back(std::move(child));
		}
	}

	virtual void ResolveType() override {
		AbstractExpression::ResolveType();
	    switch (type) {
	      // if count return an integer
	      case ExpressionType::AGGREGATE_COUNT:
	      case ExpressionType::AGGREGATE_COUNT_STAR:
	        return_type = TypeId::INTEGER;
	        break;
	      // return the type of the base
	      case ExpressionType::AGGREGATE_MAX:
	      case ExpressionType::AGGREGATE_MIN:
	      case ExpressionType::AGGREGATE_SUM:
	        return_type = children[0]->return_type;
	        break;
	      case ExpressionType::AGGREGATE_AVG:
	        return_type = TypeId::DECIMAL;
	        break;
	      default:
	        break;
	    }
	}

	virtual bool IsAggregate() override { return true; }

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

  private:
	bool distinct;
	std::string expr_name;
};
}
