#include "from_substrait.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/cross_product_relation.hpp"

#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parser.hpp"

#include "substrait/plan.pb.h"

using namespace std;

SubstraitToDuckDB::SubstraitToDuckDB(duckdb::Connection &con_p, substrait::Plan &plan_p) : con(con_p), plan(plan_p) {
	for (auto &sext : plan.extensions()) {
		if (!sext.has_extension_function()) {
			continue;
		}
		functions_map[sext.extension_function().function_anchor()] = sext.extension_function().name();
	}
}

unique_ptr<duckdb::ParsedExpression> SubstraitToDuckDB::TransformExpr(const substrait::Expression &sexpr) {
	switch (sexpr.rex_type_case()) {
	case substrait::Expression::RexTypeCase::kLiteral: {
		const auto &slit = sexpr.literal();
		duckdb::Value dval;
		switch (slit.literal_type_case()) {
		case substrait::Expression_Literal::LiteralTypeCase::kFp64:
			dval = duckdb::Value::DOUBLE(slit.fp64());
			break;

		case substrait::Expression_Literal::LiteralTypeCase::kString:
			dval = duckdb::Value(slit.string());
			break;
		case substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
			const auto &substrait_decimal = slit.decimal();
			// TODO: Support hugeint?
			int64_t substrait_value = std::stoll(substrait_decimal.value());
			dval = duckdb::Value::DECIMAL(substrait_value, substrait_decimal.precision(), substrait_decimal.scale());
			break;
		}

		case substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
			dval = duckdb::Value(slit.boolean());
			break;
		}
		case substrait::Expression_Literal::LiteralTypeCase::kI32:
			dval = duckdb::Value::INTEGER(slit.i32());
			break;
		case substrait::Expression_Literal::LiteralTypeCase::kI64:
			dval = duckdb::Value::BIGINT(slit.i64());
			break;
		default:
			throw runtime_error(to_string(slit.literal_type_case()));
		}
		return duckdb::make_unique<duckdb::ConstantExpression>(dval);
	}
	case substrait::Expression::RexTypeCase::kSelection: {
		if (!sexpr.selection().has_direct_reference() || !sexpr.selection().direct_reference().has_struct_field()) {
			throw runtime_error("Can only have direct struct references in selections");
		}
		return duckdb::make_unique<duckdb::PositionalReferenceExpression>(
		    sexpr.selection().direct_reference().struct_field().field() + 1);
	}

	case substrait::Expression::RexTypeCase::kScalarFunction: {
		vector<unique_ptr<duckdb::ParsedExpression>> children;
		for (auto &sarg : sexpr.scalar_function().args()) {
			children.push_back(TransformExpr(sarg));
		}
		auto function_name = FindFunction(sexpr.scalar_function().function_reference());
		// string compare galore
		// TODO simplify this
		if (function_name == "and") {
			return duckdb::make_unique<duckdb::ConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND,
			                                                          move(children));
		} else if (function_name == "cast") {
			// TODO actually unpack the constant expression here and not rely on ToString
			auto expr =
			    duckdb::Parser::ParseExpressionList(duckdb::StringUtil::Format("asdf::%s", children[1]->ToString()));
			auto &cast = (duckdb::CastExpression &)*expr[0];
			return duckdb::make_unique<duckdb::CastExpression>(cast.cast_type, move(children[0]));
		} else if (function_name == "or") {
			return duckdb::make_unique<duckdb::ConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_OR,
			                                                          move(children));
		} else if (function_name == "lessthan") {
			return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_LESSTHAN,
			                                                         move(children[0]), move(children[1]));
		} else if (function_name == "equal") {
			return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_EQUAL,
			                                                         move(children[0]), move(children[1]));
		} else if (function_name == "notequal") {
			return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_NOTEQUAL,
			                                                         move(children[0]), move(children[1]));
		} else if (function_name == "lessthanequal") {
			return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO,
			                                                         move(children[0]), move(children[1]));
		} else if (function_name == "greaterthanequal") {
			return duckdb::make_unique<duckdb::ComparisonExpression>(
			    duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO, move(children[0]), move(children[1]));
		} else if (function_name == "greaterthan") {
			return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_GREATERTHAN,
			                                                         move(children[0]), move(children[1]));
		} else if (function_name == "is_not_null") {
			return duckdb::make_unique<duckdb::OperatorExpression>(duckdb::ExpressionType::OPERATOR_IS_NOT_NULL,
			                                                       move(children[0]));
		}

		return duckdb::make_unique<duckdb::FunctionExpression>(function_name, move(children));
	}
	case substrait::Expression::RexTypeCase::kIfThen: {
		const auto &scase = sexpr.if_then();

		auto dcase = duckdb::make_unique<duckdb::CaseExpression>();
		for (const auto &sif : scase.ifs()) {
			duckdb::CaseCheck dif;
			dif.when_expr = TransformExpr(sif.if_());
			dif.then_expr = TransformExpr(sif.then());
			dcase->case_checks.push_back(move(dif));
		}
		dcase->else_expr = TransformExpr(scase.else_());
		return dcase;
	}
	default:
		throw runtime_error("Unsupported expression type " + to_string(sexpr.rex_type_case()));
	}
}

string SubstraitToDuckDB::FindFunction(uint64_t id) {
	if (functions_map.find(id) == functions_map.end()) {
		throw runtime_error("Could not find aggregate function " + to_string(id));
	}
	return functions_map[id];
}

duckdb::OrderByNode SubstraitToDuckDB::TransformOrder(const substrait::SortField &sordf) {

	duckdb::OrderType dordertype;
	duckdb::OrderByNullType dnullorder;

	switch (sordf.direction()) {
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
		dordertype = duckdb::OrderType::ASCENDING;
		dnullorder = duckdb::OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
		dordertype = duckdb::OrderType::ASCENDING;
		dnullorder = duckdb::OrderByNullType::NULLS_LAST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
		dordertype = duckdb::OrderType::DESCENDING;
		dnullorder = duckdb::OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
		dordertype = duckdb::OrderType::DESCENDING;
		dnullorder = duckdb::OrderByNullType::NULLS_LAST;
		break;
	default:
		throw runtime_error("Unsupported ordering " + to_string(sordf.direction()));
	}

	return {dordertype, dnullorder, TransformExpr(sordf.expr())};
}

shared_ptr<duckdb::Relation> SubstraitToDuckDB::TransformOp(const substrait::Rel &sop) {
	switch (sop.rel_type_case()) {
	case substrait::Rel::RelTypeCase::kJoin: {
		auto &sjoin = sop.join();

		duckdb::JoinType djointype;
		switch (sjoin.type()) {
		case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
			djointype = duckdb::JoinType::INNER;
			break;
		case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
			djointype = duckdb::JoinType::LEFT;
			break;
		case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
			djointype = duckdb::JoinType::RIGHT;
			break;
			//		case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_MARK:
			//			djointype = duckdb::JoinType::MARK;
			//			break;
			//		case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SINGLE:
			//			djointype = duckdb::JoinType::SINGLE;
			//			break;
		case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SEMI:
			djointype = duckdb::JoinType::SEMI;
			break;
		default:
			throw runtime_error("Unsupported join type");
		}
		vector<unique_ptr<duckdb::ParsedExpression>> expressions;
		//		for (auto &sexpr : sjoin.duplicate_eliminated_columns()) {
		//			expressions.push_back(TransformExpr(sexpr));
		//		}
		return duckdb::make_shared<duckdb::JoinRelation>(TransformOp(sjoin.left())->Alias("left"),
		                                                 TransformOp(sjoin.right())->Alias("right"),
		                                                 TransformExpr(sjoin.expression()), djointype);
	}
	case substrait::Rel::RelTypeCase::kCross: {
		auto &sub_cross = sop.cross();

		return duckdb::make_shared<duckdb::CrossProductRelation>(TransformOp(sub_cross.left())->Alias("left"),
		                                                         TransformOp(sub_cross.right())->Alias("right"));
	}
	case substrait::Rel::RelTypeCase::kFetch: {
		auto &slimit = sop.fetch();
		return duckdb::make_shared<duckdb::LimitRelation>(TransformOp(slimit.input()), slimit.count(), slimit.offset());
	}
	case substrait::Rel::RelTypeCase::kFilter: {
		auto &sfilter = sop.filter();
		return duckdb::make_shared<duckdb::FilterRelation>(TransformOp(sfilter.input()),
		                                                   TransformExpr(sfilter.condition()));
	}
	case substrait::Rel::RelTypeCase::kProject: {
		vector<unique_ptr<duckdb::ParsedExpression>> expressions;
		vector<string> aliases;
		duckdb::idx_t expr_idx = 1;
		for (auto &sexpr : sop.project().expressions()) {
			expressions.push_back(TransformExpr(sexpr));
			aliases.push_back("expr_" + to_string(expr_idx++));
		}
		return duckdb::make_shared<duckdb::ProjectionRelation>(TransformOp(sop.project().input()), move(expressions),
		                                                       move(aliases));
	}
	case substrait::Rel::RelTypeCase::kAggregate: {
		vector<unique_ptr<duckdb::ParsedExpression>> groups, expressions;

		if (sop.aggregate().groupings_size() > 1) {
			throw runtime_error("Only single grouping sets are supported for now");
		}
		if (sop.aggregate().groupings_size() > 0) {
			for (auto &sgrpexpr : sop.aggregate().groupings(0).grouping_expressions()) {
				groups.push_back(TransformExpr(sgrpexpr));
				expressions.push_back(TransformExpr(sgrpexpr));
			}
		}

		for (auto &smeas : sop.aggregate().measures()) {
			vector<unique_ptr<duckdb::ParsedExpression>> children;
			for (auto &sarg : smeas.measure().args()) {
				children.push_back(TransformExpr(sarg));
			}
			expressions.push_back(duckdb::make_unique<duckdb::FunctionExpression>(
			    FindFunction(smeas.measure().function_reference()), move(children)));
		}

		return duckdb::make_shared<duckdb::AggregateRelation>(TransformOp(sop.aggregate().input()), move(expressions),
		                                                      move(groups));
	}
	case substrait::Rel::RelTypeCase::kRead: {
		auto &sget = sop.read();
		if (!sget.has_named_table()) {
			throw runtime_error("Can only scan named tables for now");
		}

		auto scan = con.Table(sop.read().named_table().names(0));

		if (sget.has_filter()) {
			scan = duckdb::make_shared<duckdb::FilterRelation>(move(scan), TransformExpr(sget.filter()));
		}

		if (sget.has_projection()) {
			vector<unique_ptr<duckdb::ParsedExpression>> expressions;
			vector<string> aliases;
			duckdb::idx_t expr_idx = 0;
			for (auto &sproj : sget.projection().select().struct_items()) {
				// FIXME how to get actually alias?
				aliases.push_back("expr_" + to_string(expr_idx++));
				// TODO make sure nothing else is in there
				expressions.push_back(duckdb::make_unique<duckdb::PositionalReferenceExpression>(sproj.field() + 1));
			}

			scan = duckdb::make_shared<duckdb::ProjectionRelation>(move(scan), move(expressions), move(aliases));
		}

		return scan;
	}
	case substrait::Rel::RelTypeCase::kSort: {
		vector<duckdb::OrderByNode> order_nodes;
		for (auto &sordf : sop.sort().sorts()) {
			order_nodes.push_back(TransformOrder(sordf));
		}
		return duckdb::make_shared<duckdb::OrderRelation>(TransformOp(sop.sort().input()), move(order_nodes));
	}
	default:
		throw runtime_error("Unsupported relation type " + to_string(sop.rel_type_case()));
	}
}

std::shared_ptr<duckdb::Relation> SubstraitToDuckDB::TransformPlan(const substrait::Plan &splan) {
	return TransformOp(plan.relations(0).rel());
}
