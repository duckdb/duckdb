#include "duckdb/optimizer/measure_agg_rewriter.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "core_functions/scalar/random_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include <sstream>
#include <iostream>
#include <stack>
#include <optional>
#include <unordered_set>
#include <queue>
#include <algorithm>

namespace duckdb {

namespace {

template <typename FunctionType>
const LogicalType *IsMeasureAggCall(const FunctionType &function) {
	if (function.name == "measure_agg" && function.arguments.size() == 1) {
		if (function.arguments[0].id() == LogicalTypeId::MEASURE_TYPE) {
			return &function.arguments[0];
		}
	}
	return nullptr;
}

struct MeasureTypeState {
	LogicalType measure_type;
	uint64_t measure_id;
	// The operator in which this measure is defined.
	const LogicalOperator *measure_defining_op = nullptr;
	// All BOUND_AGGREGATE expressions that call the MEASURE_AGG function.
	std::unordered_set<const Expression *> bound_aggregate_invocations;
	// The expression that defines this measure
	const Expression *measure_definition = nullptr;
	// Measure IDs that this measure depends on in its own defining expression.
	std::vector<uint64_t> measure_ids_referenced;

	Expression &MeasureBoundExpression() const {
		return *measure_type.GetAuxInfoShrPtr()->Cast<MeasureTypeInfo>().bound_measure_expression;
	}

	MeasureTypeState(LogicalType measure_type, uint64_t measure_id)
	    : measure_type(measure_type), measure_id(measure_id) {
	}

	friend std::ostream &operator<<(std::ostream &os, const MeasureTypeState &state) {
		const MeasureTypeInfo &measure_type_info = state.measure_type.GetAuxInfoShrPtr()->Cast<MeasureTypeInfo>();
		os << "Measure ID " << state.measure_id << " called " << measure_type_info.alias << " with bound expression "
		   << state.MeasureBoundExpression().ToString();
		return os;
	}
};

struct MeasureRewriteState : public LogicalOperatorVisitor {
	std::vector<MeasureTypeState> measure_type_states;
	std::stack<LogicalOperator *> op_stack;

	~MeasureRewriteState() override {
	}

	void VisitPlan(LogicalOperator &op) {
		op_stack.push(&op);
		VisitOperator(op);
		D_ASSERT(op_stack.top() == &op);
		op_stack.pop();
	}

	struct MeasureBoundExpressionWalker : public LogicalOperatorVisitor {
		MeasureRewriteState &measure_rewrite_state;
		MeasureTypeState &measure_type_state;

		MeasureBoundExpressionWalker(MeasureRewriteState &measure_rewrite_state, MeasureTypeState &measure_type_state)
		    : measure_rewrite_state(measure_rewrite_state), measure_type_state(measure_type_state) {
			VisitExpression(measure_type_state.MeasureBoundExpression());
		}

		void VisitExpression(Expression &expr) {
			if (expr.return_type.id() == LogicalTypeId::MEASURE_TYPE) {
				uint64_t other_measure_id = measure_rewrite_state.GetOrCreateMeasureTypeState(expr.return_type);
				measure_type_state.measure_ids_referenced.push_back(other_measure_id);
			}
			VisitExpressionChildren(expr);
		}
		void VisitExpression(unique_ptr<Expression> *expression) override {
			auto &expr = **expression;
			VisitExpression(expr);
		}
	};

	void WalkMeasureDependencies(MeasureTypeState &state) {
		MeasureBoundExpressionWalker walker(*this, state);
	}

	// Find or create a measure type state for the given type
	uint64_t GetOrCreateMeasureTypeState(const LogicalType &type) {
		for (uint64_t i = 0; i < measure_type_states.size(); i++) {
			if (measure_type_states[i].measure_type == type) {
				return i;
			}
		}
		uint64_t new_id = measure_type_states.size();
		measure_type_states.push_back(MeasureTypeState(type, new_id));
		WalkMeasureDependencies(measure_type_states[new_id]);
		return new_id;
	}

	void VisitExpression(unique_ptr<Expression> *expression) override {
		auto &expr = **expression;

		// Check if this expression has a MEASURE_TYPE return type (measure definition)
		if (expr.return_type.id() == LogicalTypeId::MEASURE_TYPE) {
			uint64_t measure_id = GetOrCreateMeasureTypeState(expr.return_type);
			measure_type_states[measure_id].measure_defining_op = op_stack.top();
			measure_type_states[measure_id].measure_definition = &expr;

			std::cerr << "MEASURE USAGE found: ID=" << measure_id << ", Type=" << expr.return_type.ToString()
			          << ", Expression=" << expr.ToString() << ", Operator=" << op_stack.top()->GetName() << std::endl;
		}

		// Check if this is a measure_agg function call
		if (expr.GetExpressionType() == ExpressionType::BOUND_AGGREGATE) {
			auto &agg_expr = (const BoundAggregateExpression &)expr;

			const LogicalType *measure_type = IsMeasureAggCall(agg_expr.function);
			if (measure_type) {
				uint64_t measure_id = GetOrCreateMeasureTypeState(*measure_type);
				measure_type_states[measure_id].bound_aggregate_invocations.insert(&expr);
				std::cerr << "MEASURE_AGG AGGREGATE INVOCATION found: calls measure ID=" << measure_id
				          << ", Expression=" << expr.ToString() << ", Operator=" << op_stack.top()->GetName()
				          << std::endl;
			}
		} else {
			std::cerr << "Unhandled expression type: " << ExpressionTypeToString(expr.GetExpressionType()) << " "
			          << expr.ToString() << std::endl;
		}

		VisitExpressionChildren(expr);
	}

	MeasureTypeState *MeasureNotCallingOtherMeasures() {
		for (auto &measure_state : measure_type_states) {
			if (measure_state.measure_ids_referenced.empty() && !measure_state.bound_aggregate_invocations.empty()) {
				return &measure_state;
			}
		}
		for (auto &measure_state : measure_type_states) {
			if (!measure_state.measure_ids_referenced.empty()) {
				throw InvalidInputException("Measures call each other in a cycle");
			}
		}
		return nullptr;
	}

	friend std::ostream &operator<<(std::ostream &os, const MeasureRewriteState &state) {
		if (state.measure_type_states.empty()) {
			os << "No measures found in the query plan.\n";
			return os;
		}

		for (const auto &measure_state : state.measure_type_states) {
			os << "\n--- Measure ID " << measure_state.measure_id << " ---\n";

			if (measure_state.measure_definition) {
				os << "Definition: " << measure_state.measure_definition->ToString() << "\n";
				os << "Defined in operator: " << measure_state.measure_defining_op->GetName() << "\n";
			} else {
				os << "Definition: NOT FOUND\n";
			}

			if (!measure_state.bound_aggregate_invocations.empty()) {
				os << "Bound aggregate expressions: " << measure_state.bound_aggregate_invocations.size()
				   << " found:\n";
				size_t i = 1;
				for (const auto &invocation : measure_state.bound_aggregate_invocations) {
					os << "  " << i << ". " << invocation->ToString() << "\n";
					i++;
				}
			} else {
				os << "No bound aggregate expressions found.\n";
			}

			if (!measure_state.measure_ids_referenced.empty()) {
				os << "Dependencies: ";
				for (size_t i = 0; i < measure_state.measure_ids_referenced.size(); i++) {
					if (i > 0)
						os << ", ";
					os << "Measure ID " << measure_state.measure_ids_referenced[i];
				}
				os << "\n";
			} else {
				os << "No dependencies.\n";
			}
		}

		return os;
	}
};

struct MeasureSimpleReplacer : public LogicalOperatorVisitor {
	MeasureTypeState &measure_state;
	Optimizer &optimizer;

	MeasureSimpleReplacer(MeasureTypeState &measure_state, Optimizer &optimizer)
	    : measure_state(measure_state), optimizer(optimizer) {
	}

	void VisitPlan(LogicalOperator &op) {
		VisitOperator(op);
	}

	void VisitExpression(unique_ptr<Expression> *original_expression) override {
		const Expression *expr = &**original_expression;
		if (measure_state.bound_aggregate_invocations.find(expr) != measure_state.bound_aggregate_invocations.end()) {
			const auto &bound_expression = measure_state.MeasureBoundExpression();
			D_ASSERT(expr->return_type == bound_expression.return_type);

			vector<unique_ptr<Expression>> children;
			children.push_back(bound_expression.Copy());

			FunctionBinder function_binder(optimizer.GetContext());
			auto first_agg =
			    function_binder.BindAggregateFunction(FirstFunctionGetter::GetFunction(bound_expression.return_type),
			                                          std::move(children), nullptr, AggregateType::NON_DISTINCT);

			*original_expression = std::move(first_agg);
		}
		VisitExpressionChildren(**original_expression);
	}
};
} // namespace

void MeasureAggRewriter::RewriteMeasures(unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}

	for (;;) {
		MeasureRewriteState measure_rewrite_state;
		measure_rewrite_state.VisitPlan(*plan);

		if (measure_rewrite_state.measure_type_states.empty()) {
			return;
		}

		std::cerr << plan->ToString() << std::endl << measure_rewrite_state << std::endl;

		auto measure_state = measure_rewrite_state.MeasureNotCallingOtherMeasures();
		if (!measure_state) {
			return;
		}

		std::cerr << "Measure being rewritten: " << *measure_state << std::endl;

		MeasureSimpleReplacer measure_simple_replacer(*measure_state, optimizer);
		measure_simple_replacer.VisitPlan(*plan);

		std::cerr << "Plan after rewriting: " << plan->ToString() << std::endl;
	}
}

} // namespace duckdb
