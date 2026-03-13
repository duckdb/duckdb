#include "duckdb/optimizer/projection_pullup.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

void ProjectionPullup::PopParents(const LogicalOperator &op) {
	// pop back elements until the last operator in the stack is THIS operator
	while (!parents.empty() && &parents.back().get() != &op) {
		parents.pop_back();
	}
	// then pop THIS operator back, and stop
	if (!parents.empty()) {
		parents.pop_back();
	}
}

optional_ptr<LogicalOperator> ProjectionPullup::FindParent(LogicalOperator &target, LogicalOperator &current) {
	if (&current == &target) {
		return nullptr;
	}
	for (auto &child : current.children) {
		if (child.get() == &target) {
			return &current;
		}
		if (child) {
			auto result = FindParent(target, *child);
			if (result) {
				return result;
			}
		}
	}
	return nullptr;
}

void ProjectionPullup::InsertProjectionBelowOp(unique_ptr<LogicalOperator> &op, unique_ptr<LogicalOperator> &child,
                                               bool stop_at_op) {
	if (child->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		child->ResolveOperatorTypes();
		auto proj_index = optimizer.binder.GenerateTableIndex();
		auto child_bindings = child->GetColumnBindings();
		const auto child_types = child->types;
		const auto column_count = child_bindings.size();

		vector<unique_ptr<Expression>> expressions;
		expressions.reserve(column_count);
		for (idx_t i = 0; i < column_count; i++) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]));
		}

		ColumnBindingReplacer replacer;
		for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			const auto &old_binding = child_bindings[col_idx];
			replacer.replacement_bindings.emplace_back(old_binding,
			                                           ColumnBinding(proj_index, ProjectionIndex(col_idx)));
		}

		auto new_projection = make_uniq<LogicalProjection>(proj_index, std::move(expressions));
		if (child->has_estimated_cardinality) {
			new_projection->SetEstimatedCardinality(child->estimated_cardinality);
		}

		new_projection->children.emplace_back(std::move(child));
		child = std::move(new_projection);

		if (stop_at_op) {
			replacer.stop_operator = op.get();
		} else {
			replacer.stop_operator = child.get();
		}
		replacer.VisitOperator(*root);
	}
	ProjectionPullup next(optimizer, root);
	next.Optimize(child->children[0]);
}

void ProjectionPullup::Optimize(unique_ptr<LogicalOperator> &op) {
	switch (op->type) {
	// These operators depend on column order.
	// If their immediate child is a projection, keep it and recurse into the projection’s child.
	// If no projection is present, insert one, then recurse into the newly added projection’s child.
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_UNION: {
		for (auto &child : op->children) {
			InsertProjectionBelowOp(op, child, true);
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_CTE_REF:
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
	case LogicalOperatorType::LOGICAL_PIVOT: {
		for (auto &child : op->children) {
			InsertProjectionBelowOp(op, child, false);
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &comp_join = op->Cast<LogicalComparisonJoin>();
		if (comp_join.join_type == JoinType::MARK || comp_join.join_type == JoinType::SINGLE) {
			break; // bail
		}

		// We can pull through this operator, add it to the stack
		parents.push_back(*op);
		if (comp_join.join_type == JoinType::SEMI) {
			// LHS: can pull through
			Optimize(comp_join.children[0]);

			// RHS: Cannot pull through. Add a projection "barrier"
			InsertProjectionBelowOp(op, comp_join.children[1], false);
		} else {
			// All other joins: recurse normally on both sides
			for (auto &child : op->children) {
				Optimize(child);
			}
		}

		PopParents(*op);
		return;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		// We can pull through this operator, add it to the stack
		parents.push_back(*op);

		// Recurse
		Optimize(op->children[0]);

		PopParents(*op);
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &proj = op->Cast<LogicalProjection>();
		auto proj_bindings = proj.GetColumnBindings();

		// Check if all expressions are simple column refs
		// Cannot pull this projection up safely if any expression is not a column ref
		bool all_column_refs = true;
		column_binding_map_t<unique_ptr<Expression>> projection_map;
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			projection_map[proj_bindings[i]] = proj.expressions[i]->Copy();
			if (proj.expressions[i]->type != ExpressionType::BOUND_COLUMN_REF) {
				all_column_refs = false;
			}
			if (proj.expressions[i]->IsVolatile()) {
				ProjectionPullup next(optimizer, root);
				next.Optimize(proj.children[0]);
				return; // bail
			}
		}

		bool can_pull_through = true;

		// if expressions in the projections are colrefs, we can always pull it up
		// if it's not a colref, we can pull it up only if it does not appear in the operator enumerate expressions
		idx_t pull_up_to_here = parents.size();
		for (idx_t i = parents.size(); i > 0; i--) {
			idx_t parent_idx = i - 1;
			LogicalOperator &parent_op = parents[parent_idx].get();
			can_pull_through = true;

			LogicalOperatorVisitor::EnumerateExpressions(parent_op, [&](unique_ptr<Expression> *expr) {
				ExpressionIterator::EnumerateExpression(*expr, [&](unique_ptr<Expression> &child_expr) {
					if (child_expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
						return;
					}

					auto &colref = child_expr->Cast<BoundColumnRefExpression>();
					auto entry = projection_map.find(colref.binding);

					if (entry == projection_map.end()) {
						return;
					}

					// This parent references a projection output
					// If that output is NOT a simple column ref, we cannot pull through
					if (entry->second->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
						can_pull_through = false;
					}
				});
			});

			if (!can_pull_through) {
				break;
			}
		}

		if (!can_pull_through) {
			// Recurse into child;
			ProjectionPullup next(optimizer, root);
			next.Optimize(proj.children[0]);
			return;
		}

		// after the loop we figured out how far we can pull it up
		// If we can pull up, replace bindings along parents and remove this projection
		if (pull_up_to_here > 0) {
			// Do not remove projections above UNNEST. The projection above the unnest extracts just the required
			// fields. Removing it forces all other operators to carry the full struct, eventually causing the
			// memory blowup.
			if (op->children[0]->type == LogicalOperatorType::LOGICAL_UNNEST) {
				parents.push_back(*op);
				Optimize(op->children[0]);
				PopParents(*op);
				return;
			}
			if (all_column_refs) {
				ColumnBindingReplacer replacer;
				for (idx_t i = 0; i < proj.expressions.size(); i++) {
					auto &colref = proj.expressions[i]->Cast<BoundColumnRefExpression>();
					replacer.replacement_bindings.emplace_back(proj_bindings[i], colref.binding);
				}

				replacer.stop_operator = proj.children[0];
				replacer.VisitOperator(*root);

				// Re-run optimization after removing this projection.
				// Binding rewrites can make parent projections redundant, and without
				// another pass they would not be eliminated.
				Optimize(op->children[0]);
				op = std::move(op->children[0]);

				return;
			}

			// Not all expressions are colrefs. We can pull up instead of removing
			for (idx_t i = 0; i < proj.expressions.size(); i++) {
				// FIXME: Constants should be safe to pass through if they are in projections on the non-nullable side
				// of a join. Skip for now
				if (proj.expressions[i]->type == ExpressionType::VALUE_CONSTANT) {
					return;
				}
			}

			LogicalOperator &insert_at_node = parents[parents.size() - pull_up_to_here].get();

			// FIXME: would like to make that faster/better
			auto parent_of_insert = FindParent(insert_at_node, *root);

			// Prepare the column binding replacer once
			ColumnBindingReplacer replacer;
			for (idx_t i = 0; i < proj.expressions.size(); i++) {
				if (proj.expressions[i]->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &colref = proj.expressions[i]->Cast<BoundColumnRefExpression>();
					replacer.replacement_bindings.emplace_back(proj_bindings[i], colref.binding);
				}
			}
			for (idx_t i = 0; i < pull_up_to_here; i++) {
				replacer.VisitOperator(parents[i].get());
			}
			replacer.replacement_bindings.clear();

			// actually pull up the projection
			insert_at_node.ResolveOperatorTypes();
			auto insert_bindings = insert_at_node.GetColumnBindings();
			const auto insert_types = insert_at_node.types;

			// Build the set of bindings that actually exist BELOW insert_at_node
			// by checking its child's bindings, not insert_at_node itself
			// insert_at_node's own bindings may include stale refs that
			// were already rewritten by a previous pass
			column_binding_set_t child_available_bindings;
			if (!insert_at_node.children.empty()) {
				insert_at_node.children[0]->ResolveOperatorTypes();
				for (auto &b : insert_at_node.children[0]->GetColumnBindings()) {
					child_available_bindings.insert(b);
				}
			}
			column_binding_set_t existing_bindings(proj_bindings.begin(), proj_bindings.end());
			auto projection_to_move = std::move(op);
			op = std::move(projection_to_move->children[0]);

			idx_t next_col = proj.expressions.size();
			for (idx_t i = 0; i < insert_bindings.size(); i++) {
				// Skip bindings that no longer exist in the plan
				if (child_available_bindings.find(insert_bindings[i]) == child_available_bindings.end()) {
					continue;
				}
				if (existing_bindings.find(insert_bindings[i]) == existing_bindings.end()) {
					proj.expressions.push_back(
					    make_uniq<BoundColumnRefExpression>(insert_types[i], insert_bindings[i]));
					replacer.replacement_bindings.emplace_back(
					    insert_bindings[i], ColumnBinding(proj.table_index, ProjectionIndex(next_col)));
					next_col++;
				}
			}
			replacer.stop_operator = &insert_at_node;
			replacer.VisitOperator(*root);

			// Find where to rewire the plan
			if (!parent_of_insert) {
				projection_to_move->children[0] = std::move(root);
				root = std::move(projection_to_move);
			} else {
				for (auto &child_ptr : parent_of_insert->children) {
					if (child_ptr.get() == &insert_at_node) {
						projection_to_move->children[0] = std::move(child_ptr);
						child_ptr = std::move(projection_to_move);
						break;
					}
				}
			}
			return;
		}

		// Recurse on child
		Optimize(op->children[0]);
		PopParents(*op);
		return;
	}
	default: {
		break;
	}
	}

	// Create new optimizer for child (start fresh without any state)
	for (auto &child : op->children) {
		ProjectionPullup next(optimizer, root);
		next.Optimize(child);
	}
}
} // namespace duckdb
