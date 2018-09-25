//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/aggregate_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
//! The AggregateExpression represents an aggregate in the query
class AggregateExpression : public AbstractExpression {
  public:
	AggregateExpression(ExpressionType type, bool distinct,
	                    std::unique_ptr<AbstractExpression> child)
	    : AbstractExpression(type), index(0) {
		this->distinct = distinct;

		// translate COUNT(*) into AGGREGATE_COUNT_STAR
		if (type == ExpressionType::AGGREGATE_COUNT) {
			if (!child) {
				this->type = ExpressionType::AGGREGATE_COUNT_STAR;
			} else if (child->GetExpressionType() == ExpressionType::STAR) {
				child = nullptr;
				this->type = ExpressionType::AGGREGATE_COUNT_STAR;
			}
		}
		switch (type) {
		case ExpressionType::AGGREGATE_COUNT:
		case ExpressionType::AGGREGATE_COUNT_STAR:
		case ExpressionType::AGGREGATE_SUM:
		case ExpressionType::AGGREGATE_MIN:
		case ExpressionType::AGGREGATE_MAX:
		case ExpressionType::AGGREGATE_FIRST:

			break;
		default:
			throw Exception("Aggregate type not supported");
		}
		if (child) {
			AddChild(std::move(child));
		}
	}

	//! Resolve the type of the aggregate
	virtual void ResolveType() override {
		AbstractExpression::ResolveType();
		switch (type) {
		case ExpressionType::AGGREGATE_COUNT_STAR:
			return_type = TypeId::BIGINT;
			break;
		case ExpressionType::AGGREGATE_COUNT:
			if (children[0]->IsScalar()) {
				stats.has_stats = false;
			} else {
				Statistics::Count(children[0]->stats, stats);
			}
			return_type = TypeId::BIGINT;
			break;
		case ExpressionType::AGGREGATE_MAX:
			Statistics::Max(children[0]->stats, stats);
			return_type =
			    std::max(children[0]->return_type, stats.MinimalType());
			break;
		case ExpressionType::AGGREGATE_MIN:
			Statistics::Min(children[0]->stats, stats);
			return_type =
			    std::max(children[0]->return_type, stats.MinimalType());
			break;
		case ExpressionType::AGGREGATE_SUM:
			if (children[0]->IsScalar()) {
				stats.has_stats = false;
				switch (children[0]->return_type) {
				case TypeId::BOOLEAN:
				case TypeId::TINYINT:
				case TypeId::SMALLINT:
				case TypeId::INTEGER:
				case TypeId::BIGINT:
					return_type = TypeId::BIGINT;
					break;
				default:
					return_type = children[0]->return_type;
				}
			} else {
				Statistics::Count(children[0]->stats, stats);
				Statistics::Sum(children[0]->stats, stats);
				return_type =
				    std::max(children[0]->return_type, stats.MinimalType());
			}

			break;
		case ExpressionType::AGGREGATE_FIRST:
			return_type = children[0]->return_type;
			break;
		default:
			throw NotImplementedException("Unsupported aggregate type!");
		}
	}

	virtual void
	GetAggregates(std::vector<AggregateExpression *> &expressions) override;
	virtual bool IsAggregate() override { return true; }

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	size_t index;

  private:
	//! Whether or not the aggregate returns only distinct values (what?)
	bool distinct;
};
} // namespace duckdb
