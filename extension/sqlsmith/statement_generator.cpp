#include "statement_generator.hpp"

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

struct GeneratorContext {
	vector<TestType> test_types;
	vector<reference<CatalogEntry>> scalar_functions;
	vector<reference<CatalogEntry>> table_functions;
	vector<reference<CatalogEntry>> pragma_functions;
	vector<reference<CatalogEntry>> tables_and_views;
};

StatementGenerator::StatementGenerator(ClientContext &context) : context(context), parent(nullptr), depth(0) {
	generator_context = GetDatabaseState(context);
}

StatementGenerator::StatementGenerator(StatementGenerator &parent_p)
    : context(parent_p.context), parent(&parent_p), generator_context(parent_p.generator_context),
      depth(parent_p.depth + 1) {
	if (depth > MAX_DEPTH) {
		throw InternalException("depth too high");
	}
}

StatementGenerator::~StatementGenerator() {
}

shared_ptr<GeneratorContext> StatementGenerator::GetDatabaseState(ClientContext &context) {
	auto result = make_shared<GeneratorContext>();
	result->test_types = TestAllTypesFun::GetTestTypes();

	auto schemas = Catalog::GetAllSchemas(context);
	// extract the functions
	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();
		schema.Scan(context, CatalogType::SCALAR_FUNCTION_ENTRY,
		            [&](CatalogEntry &entry) { result->scalar_functions.push_back(entry); });
		schema.Scan(context, CatalogType::TABLE_FUNCTION_ENTRY,
		            [&](CatalogEntry &entry) { result->table_functions.push_back(entry); });
		schema.Scan(context, CatalogType::PRAGMA_FUNCTION_ENTRY,
		            [&](CatalogEntry &entry) { result->pragma_functions.push_back(entry); });
		schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			result->tables_and_views.push_back(entry);
		});
	}
	return result;
}

unique_ptr<SQLStatement> StatementGenerator::GenerateStatement() {
	return GenerateStatement(StatementType::SELECT_STATEMENT);
}

unique_ptr<SQLStatement> StatementGenerator::GenerateStatement(StatementType type) {
	switch (type) {
	case StatementType::SELECT_STATEMENT:
		return GenerateSelect();
	default:
		throw InternalException("Unsupported type");
	}
}

//===--------------------------------------------------------------------===//
// Statements
//===--------------------------------------------------------------------===//
unique_ptr<SQLStatement> StatementGenerator::GenerateSelect() {
	auto select = make_uniq<SelectStatement>();
	select->node = GenerateQueryNode();
	return std::move(select);
}

//===--------------------------------------------------------------------===//
// Query Node
//===--------------------------------------------------------------------===//
void StatementGenerator::GenerateCTEs(QueryNode &node) {
	if (depth > 0) {
		return;
	}
	while (RandomPercentage(20)) {
		auto cte = make_uniq<CommonTableExpressionInfo>();
		cte->query = unique_ptr_cast<SQLStatement, SelectStatement>(GenerateSelect());
		for (idx_t i = 0; i < 1 + RandomValue(10); i++) {
			cte->aliases.push_back(GenerateIdentifier());
		}
		node.cte_map.map.insert(make_pair(GenerateTableIdentifier(), std::move(cte)));
	}
}
unique_ptr<QueryNode> StatementGenerator::GenerateQueryNode() {
	unique_ptr<QueryNode> result;
	bool is_distinct = false;
	if (RandomPercentage(70)) {
		// select node
		auto select_node = make_uniq<SelectNode>();
		// generate CTEs
		GenerateCTEs(*select_node);

		is_distinct = RandomPercentage(30);
		if (RandomPercentage(95)) {
			select_node->from_table = GenerateTableRef();
		}
		select_node->select_list = GenerateChildren(1, 10);
		select_node->where_clause = RandomExpression(60);
		select_node->having = RandomExpression(25);
		if (RandomPercentage(30)) {
			select_node->groups.group_expressions = GenerateChildren(1, 5);
			auto group_count = select_node->groups.group_expressions.size();
			if (RandomPercentage(70)) {
				// single GROUP BY
				GroupingSet set;
				for (idx_t i = 0; i < group_count; i++) {
					set.insert(i);
				}
				select_node->groups.grouping_sets.push_back(std::move(set));
			} else {
				// multiple grouping sets
				while (true) {
					GroupingSet set;
					while (true) {
						set.insert(RandomValue(group_count));
						if (RandomPercentage(50)) {
							break;
						}
					}
					select_node->groups.grouping_sets.push_back(std::move(set));
					if (RandomPercentage(70)) {
						break;
					}
				}
			}
		}
		select_node->qualify = RandomExpression(10);
		select_node->aggregate_handling =
		    RandomPercentage(10) ? AggregateHandling::FORCE_AGGREGATES : AggregateHandling::STANDARD_HANDLING;
		if (RandomPercentage(10)) {
			auto sample = make_uniq<SampleOptions>();
			sample->is_percentage = RandomPercentage(50);
			if (sample->is_percentage) {
				sample->sample_size = Value::BIGINT(RandomValue(100));
			} else {
				sample->sample_size = Value::BIGINT(RandomValue(99999));
			}
			sample->method = Choose<SampleMethod>(
			    {SampleMethod::BERNOULLI_SAMPLE, SampleMethod::RESERVOIR_SAMPLE, SampleMethod::SYSTEM_SAMPLE});
			select_node->sample = std::move(sample);
		}
		result = std::move(select_node);
	} else {
		auto setop = make_uniq<SetOperationNode>();
		GenerateCTEs(*setop);
		setop->setop_type = Choose<SetOperationType>({SetOperationType::EXCEPT, SetOperationType::INTERSECT,
		                                              SetOperationType::UNION, SetOperationType::UNION_BY_NAME});
		setop->left = GenerateQueryNode();
		setop->right = GenerateQueryNode();
		switch (setop->setop_type) {
		case SetOperationType::EXCEPT:
		case SetOperationType::INTERSECT:
			is_distinct = true;
			break;
		case SetOperationType::UNION:
		case SetOperationType::UNION_BY_NAME:
			is_distinct = RandomBoolean();
			break;
		default:
			throw InternalException("Unsupported set operation type");
		}
		result = std::move(setop);
	}

	if (is_distinct) {
		result->modifiers.push_back(make_uniq<DistinctModifier>());
	}
	if (RandomPercentage(20)) {
		result->modifiers.push_back(GenerateOrderBy());
	}
	if (RandomPercentage(20)) {
		if (RandomPercentage(50)) {
			auto limit_percent_modifier = make_uniq<LimitPercentModifier>();
			if (RandomPercentage(30)) {
				limit_percent_modifier->limit = GenerateExpression();
			} else if (RandomPercentage(30)) {
				limit_percent_modifier->offset = GenerateExpression();
			} else {
				limit_percent_modifier->limit = GenerateExpression();
				limit_percent_modifier->offset = GenerateExpression();
			}
			result->modifiers.push_back(std::move(limit_percent_modifier));
		} else {
			auto limit_modifier = make_uniq<LimitModifier>();
			if (RandomPercentage(30)) {
				limit_modifier->limit = GenerateExpression();
			} else if (RandomPercentage(30)) {
				limit_modifier->offset = GenerateExpression();
			} else {
				limit_modifier->limit = GenerateExpression();
				limit_modifier->offset = GenerateExpression();
			}
			result->modifiers.push_back(std::move(limit_modifier));
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Table Ref
//===--------------------------------------------------------------------===//
unique_ptr<TableRef> StatementGenerator::GenerateTableRef() {
	if (RandomPercentage(60)) {
		return GenerateBaseTableRef();
	}
	if (RandomPercentage(20)) {
		return GenerateExpressionListRef();
	}
	if (RandomPercentage(40)) {
		return GenerateJoinRef();
	}
	switch (RandomValue(3)) {
	case 0:
		return GenerateSubqueryRef();
	case 1:
		return GenerateTableFunctionRef();
	case 2:
		return GeneratePivotRef();
	default:
		throw InternalException("StatementGenerator::GenerateTableRef");
	}
}

unique_ptr<TableRef> StatementGenerator::GenerateBaseTableRef() {
	if (generator_context->tables_and_views.empty()) {
		return GenerateExpressionListRef();
	}
	auto &entry_ref = Choose(generator_context->tables_and_views);
	auto &entry = entry_ref.get();
	auto result = make_uniq<BaseTableRef>();
	idx_t column_count;
	switch (entry.type) {
	case CatalogType::TABLE_ENTRY: {
		auto &table = entry.Cast<TableCatalogEntry>();
		column_count = table.GetColumns().LogicalColumnCount();
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		auto &view = entry.Cast<ViewCatalogEntry>();
		column_count = view.types.size();
		break;
	}
	default:
		throw InternalException("StatementGenerator::GenerateBaseTableRef");
	}
	for (idx_t i = 0; i < column_count; i++) {
		result->column_name_alias.push_back(GenerateIdentifier());
	}
	result->alias = GenerateTableIdentifier();
	result->table_name = entry.name;
	return std::move(result);
}

unique_ptr<TableRef> StatementGenerator::GenerateExpressionListRef() {
	auto result = make_uniq<ExpressionListRef>();
	auto column_count = 1 + RandomValue(10);
	for (idx_t r = 0; r < 1 + RandomValue(10); r++) {
		vector<unique_ptr<ParsedExpression>> values;
		for (idx_t c = 0; c < column_count; c++) {
			values.push_back(GenerateConstant());
		}
		result->values.push_back(std::move(values));
	}
	return std::move(result);
}

unique_ptr<TableRef> StatementGenerator::GenerateJoinRef() {
	JoinRefType join_ref;
	if (RandomPercentage(10)) {
		join_ref = JoinRefType::CROSS;
	} else if (RandomPercentage(10)) {
		join_ref = JoinRefType::ASOF;
	} else if (RandomPercentage(10)) {
		join_ref = JoinRefType::NATURAL;
	} else if (RandomPercentage(10)) {
		join_ref = JoinRefType::POSITIONAL;
	} else {
		join_ref = JoinRefType::REGULAR;
	}
	auto join = make_uniq<JoinRef>(join_ref);
	join->left = GenerateTableRef();
	join->right = GenerateTableRef();
	if (join_ref != JoinRefType::CROSS && join_ref != JoinRefType::NATURAL) {
		if (RandomPercentage(70)) {
			join->condition = GenerateExpression();
		} else {
			while (true) {
				join->using_columns.push_back(GenerateColumnName());
				if (RandomPercentage(50)) {
					break;
				}
			}
		}
	}
	join->type = Choose<JoinType>(
	    {JoinType::LEFT, JoinType::RIGHT, JoinType::INNER, JoinType::OUTER, JoinType::SEMI, JoinType::ANTI});
	return std::move(join);
}

unique_ptr<TableRef> StatementGenerator::GenerateSubqueryRef() {
	if (depth >= MAX_DEPTH) {
		return GenerateBaseTableRef();
	}
	unique_ptr<SelectStatement> subquery;
	{
		StatementGenerator child_generator(*this);
		subquery = unique_ptr_cast<SQLStatement, SelectStatement>(child_generator.GenerateSelect());
		for (auto &col : child_generator.current_column_names) {
			current_column_names.push_back(std::move(col));
		}
	}
	auto result = make_uniq<SubqueryRef>(std::move(subquery), GenerateTableIdentifier());
	return std::move(result);
}

unique_ptr<TableRef> StatementGenerator::GenerateTableFunctionRef() {
	auto function = make_uniq<TableFunctionRef>();
	auto &table_function_ref = Choose(generator_context->table_functions);
	auto &entry = table_function_ref.get().Cast<TableFunctionCatalogEntry>();
	auto table_function = entry.functions.GetFunctionByOffset(RandomValue(entry.functions.Size()));

	auto result = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	for (idx_t i = 0; i < table_function.arguments.size(); i++) {
		children.push_back(GenerateConstant());
	}
	vector<string> names;
	for (auto &e : table_function.named_parameters) {
		names.push_back(e.first);
	}
	while (!names.empty() && RandomPercentage(50)) {
		auto name = Choose(names);
		names.erase(std::find(names.begin(), names.end(), name));
		auto expr = GenerateConstant();
		expr->alias = name;
		children.push_back(std::move(expr));
	}
	result->function = make_uniq<FunctionExpression>(entry.name, std::move(children));
	for (idx_t i = 0; i < 1 + RandomValue(9); i++) {
		result->column_name_alias.push_back(GenerateIdentifier());
	}
	result->alias = GenerateTableIdentifier();
	return std::move(result);
}

unique_ptr<TableRef> StatementGenerator::GeneratePivotRef() {
	auto pivot = make_uniq<PivotRef>();
	pivot->source = GenerateTableRef();
	bool is_pivot = RandomPercentage(50);
	if (is_pivot) {
		// pivot
		// aggregates
		while (true) {
			pivot->aggregates.push_back(GenerateFunction());
			if (RandomPercentage(50)) {
				break;
			}
		}
		while (RandomPercentage(50)) {
			pivot->groups.push_back(GenerateColumnName());
		}
	} else {
		// unpivot
		while (true) {
			pivot->unpivot_names.push_back(GenerateColumnName());
			if (RandomPercentage(50)) {
				break;
			}
		}
		pivot->include_nulls = RandomBoolean();
	}
	while (true) {
		PivotColumn col;
		idx_t number_of_columns = 1 + RandomValue(2);
		for (idx_t i = 0; i < number_of_columns; i++) {
			if (is_pivot) {
				col.pivot_expressions.push_back(GenerateExpression());
			} else {
				col.unpivot_names.push_back(GenerateColumnName());
			}
		}
		while (true) {
			PivotColumnEntry entry;
			for (idx_t i = 0; i < number_of_columns; i++) {
				entry.values.push_back(GenerateConstantValue());
			}
			col.entries.push_back(std::move(entry));
			if (RandomPercentage(50)) {
				break;
			}
		}
		pivot->pivots.push_back(std::move(col));
		if (RandomPercentage(70)) {
			break;
		}
	}
	return std::move(pivot);
}

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//
class ExpressionDepthChecker {
public:
	explicit ExpressionDepthChecker(StatementGenerator &generator) : generator(generator) {
		generator.expression_depth++;
	}
	~ExpressionDepthChecker() {
		generator.expression_depth--;
	}

	StatementGenerator &generator;
};

unique_ptr<ParsedExpression> StatementGenerator::GenerateExpression() {
	ExpressionDepthChecker checker(*this);
	if (RandomPercentage(50) || RandomPercentage(expression_depth + depth * 5)) {
		return GenerateColumnRef();
	}
	if (RandomPercentage(30)) {
		return GenerateConstant();
	}
	if (RandomPercentage(3)) {
		return GenerateSubquery();
	}
	switch (RandomValue(9)) {
	case 0:
		return GenerateOperator();
	case 1:
		return GenerateFunction();
	case 2:
		return GenerateWindowFunction();
	case 3:
		return GenerateConjunction();
	case 4:
		return GenerateStar();
	case 5:
		return GenerateCast();
	case 6:
		return GenerateBetween();
	case 7:
		return GenerateComparison();
	case 8:
		return GeneratePositionalReference();
	default:
		throw InternalException("StatementGenerator::GenerateExpression");
	}
}

Value StatementGenerator::GenerateConstantValue() {
	if (RandomPercentage(50)) {
		return Value::BIGINT(RandomValue(9999));
	}
	if (RandomPercentage(30)) {
		return Value(UUID::ToString(UUID::GenerateRandomUUID(RandomEngine::Get(context))));
	}
	auto &val = Choose(generator_context->test_types);
	Value constant_val;
	switch (RandomValue(3)) {
	case 0:
		constant_val = val.min_value;
		break;
	case 1:
		constant_val = val.max_value;
		break;
	case 2:
		constant_val = Value(val.type);
		break;
	default:
		throw InternalException("StatementGenerator::GenerateConstant");
	}
	return constant_val;
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateConstant() {
	return make_uniq<ConstantExpression>(GenerateConstantValue());
}

LogicalType StatementGenerator::GenerateLogicalType() {
	return Choose(generator_context->test_types).type;
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateColumnRef() {
	auto column_name = GenerateColumnName();
	if (column_name.empty()) {
		return GenerateConstant();
	}
	return make_uniq<ColumnRefExpression>(std::move(column_name));
}

class AggregateChecker {
public:
	explicit AggregateChecker(StatementGenerator &generator) : generator(generator) {
		generator.in_aggregate = true;
	}
	~AggregateChecker() {
		generator.in_aggregate = false;
	}

	StatementGenerator &generator;
};

unique_ptr<ParsedExpression> StatementGenerator::GenerateFunction() {
	// get a random function
	auto &function_ref = Choose(generator_context->scalar_functions);
	auto &function = function_ref.get();
	string name;
	idx_t min_parameters;
	idx_t max_parameters;
	unique_ptr<ParsedExpression> filter;
	unique_ptr<OrderModifier> order_bys;
	bool distinct = false;
	unique_ptr<AggregateChecker> checker;
	vector<LogicalType> arguments;
	switch (function.type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		auto &scalar_entry = function.Cast<ScalarFunctionCatalogEntry>();
		auto actual_function = scalar_entry.functions.GetFunctionByOffset(RandomValue(scalar_entry.functions.Size()));

		name = scalar_entry.name;
		arguments = actual_function.arguments;
		min_parameters = actual_function.arguments.size();
		max_parameters = min_parameters;
		if (actual_function.varargs.id() != LogicalTypeId::INVALID) {
			max_parameters += 5;
		}
		break;
	}
	case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
		auto &aggregate_entry = function.Cast<AggregateFunctionCatalogEntry>();
		auto actual_function =
		    aggregate_entry.functions.GetFunctionByOffset(RandomValue(aggregate_entry.functions.Size()));

		name = aggregate_entry.name;
		min_parameters = actual_function.arguments.size();
		max_parameters = min_parameters;
		if (actual_function.varargs.id() != LogicalTypeId::INVALID) {
			max_parameters += 5;
		}
		if (RandomPercentage(10) && !in_window) {
			return GenerateWindowFunction(&actual_function);
		}
		if (in_aggregate) {
			// we cannot nest aggregates
			return GenerateColumnRef();
		}
		checker = make_uniq<AggregateChecker>(*this);
		filter = RandomExpression(10);
		if (RandomPercentage(10)) {
			// generate order by
			order_bys = GenerateOrderBy();
		}
		if (RandomPercentage(10)) {
			distinct = true;
		}
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		auto &macro_entry = function.Cast<MacroCatalogEntry>();
		name = macro_entry.name;
		min_parameters = macro_entry.function->parameters.size();
		max_parameters = min_parameters;
		break;
	}
	default:
		throw InternalException("StatementGenerator::GenerateFunction");
	}
	auto children = GenerateChildren(min_parameters, max_parameters);
	// push lambda expressions
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i].id() == LogicalTypeId::LAMBDA) {
			children[i] = GenerateLambda();
		}
	}
	// FIXME: add export_state
	return make_uniq<FunctionExpression>(std::move(name), std::move(children), std::move(filter), std::move(order_bys),
	                                     distinct);
}

unique_ptr<OrderModifier> StatementGenerator::GenerateOrderBy() {
	auto result = make_uniq<OrderModifier>();
	while (true) {
		auto order_type = Choose<OrderType>({OrderType::ASCENDING, OrderType::DESCENDING, OrderType::ORDER_DEFAULT});
		auto null_type = Choose<OrderByNullType>(
		    {OrderByNullType::NULLS_FIRST, OrderByNullType::NULLS_LAST, OrderByNullType::ORDER_DEFAULT});
		result->orders.emplace_back(order_type, null_type, GenerateExpression());
		// continue with a random chance
		if (RandomPercentage(50)) {
			break;
		}
	}
	return result;
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateOperator() {
	ExpressionType type;
	idx_t min_parameters;
	idx_t max_parameters;
	switch (RandomValue(9)) {
	case 0:
		type = ExpressionType::COMPARE_IN;
		min_parameters = 2;
		max_parameters = 10;
		break;
	case 1:
		type = ExpressionType::COMPARE_NOT_IN;
		min_parameters = 2;
		max_parameters = 10;
		break;
	case 2:
		type = ExpressionType::OPERATOR_NOT;
		min_parameters = 1;
		max_parameters = 1;
		break;
	case 3:
		type = ExpressionType::OPERATOR_COALESCE;
		min_parameters = 2;
		max_parameters = 10;
		break;
	case 4:
		type = ExpressionType::OPERATOR_IS_NULL;
		min_parameters = 1;
		max_parameters = 1;
		break;
	case 5:
		type = ExpressionType::OPERATOR_IS_NOT_NULL;
		min_parameters = 1;
		max_parameters = 1;
		break;
	case 6:
		type = ExpressionType::ARRAY_EXTRACT;
		min_parameters = 2;
		max_parameters = 2;
		break;
	case 7:
		type = ExpressionType::ARRAY_SLICE;
		min_parameters = 3;
		max_parameters = 3;
		break;
	case 8:
		type = ExpressionType::ARRAY_CONSTRUCTOR;
		min_parameters = 0;
		max_parameters = 10;
		break;
	default:
		throw InternalException("StatementGenerator::GenerateOperator");
	}
	auto children = GenerateChildren(min_parameters, max_parameters);
	return make_uniq<OperatorExpression>(type, std::move(children));
}

vector<unique_ptr<ParsedExpression>> StatementGenerator::GenerateChildren(idx_t min, idx_t max) {
	if (min > max) {
		throw InternalException("StatementGenerator::GenerateChildren min > max");
	}
	vector<unique_ptr<ParsedExpression>> children;
	if (min == 0 && max == 0) {
		return children;
	}
	idx_t upper_bound = min == max ? min : min + RandomValue(max - min);
	for (idx_t i = 0; i < upper_bound; i++) {
		children.push_back(GenerateExpression());
	}
	return children;
}

class WindowChecker {
public:
	explicit WindowChecker(StatementGenerator &generator) : generator(generator) {
		generator.in_window = true;
	}
	~WindowChecker() {
		generator.in_window = false;
	}

	StatementGenerator &generator;
};

unique_ptr<ParsedExpression> StatementGenerator::GenerateWindowFunction(optional_ptr<AggregateFunction> function) {
	if (in_window) {
		// we cannot nest window functions
		return GenerateColumnRef();
	}
	ExpressionType type;
	string name;
	idx_t min_parameters;
	idx_t max_parameters;
	if (function) {
		type = ExpressionType::WINDOW_AGGREGATE;
		name = function->name;
		min_parameters = function->arguments.size();
		max_parameters = min_parameters;
	} else {
		name = Choose<string>({"rank", "rank_dense", "percent_rank", "row_number", "first_value", "last_value",
		                       "nth_value", "cume_dist", "lead", "lag", "ntile"});
		type = WindowExpression::WindowToExpressionType(name);
		switch (type) {
		case ExpressionType::WINDOW_RANK:
		case ExpressionType::WINDOW_RANK_DENSE:
		case ExpressionType::WINDOW_ROW_NUMBER:
		case ExpressionType::WINDOW_PERCENT_RANK:
		case ExpressionType::WINDOW_CUME_DIST:
			min_parameters = 0;
			break;
		case ExpressionType::WINDOW_NTILE:
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_LAST_VALUE:
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG:
			min_parameters = 1;
			break;
		case ExpressionType::WINDOW_NTH_VALUE:
			min_parameters = 2;
			break;
		default:
			throw InternalException("StatementGenerator::GenerateWindow");
		}
		max_parameters = min_parameters;
	}
	WindowChecker checker(*this);
	auto result = make_uniq<WindowExpression>(type, INVALID_CATALOG, INVALID_SCHEMA, std::move(name));
	result->children = GenerateChildren(min_parameters, max_parameters);
	while (RandomPercentage(50)) {
		result->partitions.push_back(GenerateExpression());
	}
	if (RandomPercentage(30)) {
		result->orders = std::move(GenerateOrderBy()->orders);
	}
	if (function) {
		result->filter_expr = RandomExpression(30);
		if (RandomPercentage(30)) {
			result->ignore_nulls = true;
		}
	}
	vector<WindowBoundary> window_boundaries {
	    WindowBoundary::UNBOUNDED_PRECEDING,  WindowBoundary::UNBOUNDED_FOLLOWING, WindowBoundary::CURRENT_ROW_RANGE,
	    WindowBoundary::CURRENT_ROW_ROWS,     WindowBoundary::EXPR_PRECEDING_ROWS, WindowBoundary::EXPR_FOLLOWING_ROWS,
	    WindowBoundary::EXPR_PRECEDING_RANGE, WindowBoundary::EXPR_FOLLOWING_RANGE};
	do {
		result->start = Choose(window_boundaries);
	} while (result->start == WindowBoundary::UNBOUNDED_FOLLOWING);
	do {
		result->end = Choose(window_boundaries);
	} while (result->end == WindowBoundary::UNBOUNDED_PRECEDING);
	switch (result->start) {
	case WindowBoundary::EXPR_PRECEDING_ROWS:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		result->start_expr = GenerateExpression();
		break;
	default:
		break;
	}
	switch (result->end) {
	case WindowBoundary::EXPR_PRECEDING_ROWS:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		result->end_expr = GenerateExpression();
		break;
	default:
		break;
	}
	switch (type) {
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		result->offset_expr = RandomExpression(30);
		result->default_expr = RandomExpression(30);
		break;
	default:
		break;
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> StatementGenerator::RandomExpression(idx_t percentage) {
	if (RandomPercentage(percentage)) {
		return GenerateExpression();
	}
	return nullptr;
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateConjunction() {
	auto left = GenerateExpression();
	auto right = GenerateExpression();
	ExpressionType conjunction_type;
	switch (RandomValue(2)) {
	case 0:
		conjunction_type = ExpressionType::CONJUNCTION_AND;
		break;
	case 1:
		conjunction_type = ExpressionType::CONJUNCTION_OR;
		break;
	default:
		throw InternalException("StatementGenerator::GenerateConjunction");
	}
	return make_uniq<ConjunctionExpression>(conjunction_type, std::move(left), std::move(right));
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateStar() {
	auto result = make_uniq<StarExpression>();
	if (!current_relation_names.empty()) {
		if (RandomPercentage(10)) {
			result->relation_name = GenerateRelationName();
		}
	}

	while (RandomPercentage(20)) {
		auto column_name = GenerateColumnName();
		if (column_name.empty()) {
			break;
		}
		result->exclude_list.insert(column_name);
	}
	while (RandomPercentage(20)) {
		auto column_name = GenerateColumnName();
		if (column_name.empty()) {
			break;
		}
		result->replace_list.insert(make_pair(column_name, GenerateExpression()));
	}
	if (RandomPercentage(50) || expression_depth > 0) {
		result->columns = true;
		if (RandomPercentage(50)) {
			result->expr = GenerateLambda();
		}
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateLambda() {
	// generate the lambda name and add the lambda column names to the set of possibly-generated column names
	auto lambda_parameter = GenerateIdentifier();
	// generate the lhs
	auto lhs = make_uniq<ColumnRefExpression>(lambda_parameter);
	auto rhs = GenerateExpression();
	current_column_names.erase(std::find(current_column_names.begin(), current_column_names.end(), lambda_parameter));

	return make_uniq<LambdaExpression>(std::move(lhs), std::move(rhs));
}

string StatementGenerator::GenerateTableIdentifier() {
	auto identifier = "t" + to_string(GetIndex());
	current_relation_names.push_back(identifier);
	return identifier;
}

string StatementGenerator::GenerateIdentifier() {
	auto identifier = "c" + to_string(GetIndex());
	current_column_names.push_back(identifier);
	return identifier;
}

idx_t StatementGenerator::GetIndex() {
	if (parent) {
		return parent->GetIndex();
	}
	return ++index;
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateSubquery() {
	if (depth >= MAX_DEPTH || in_window) {
		return GenerateConstant();
	}
	auto subquery = make_uniq<SubqueryExpression>();

	{
		StatementGenerator child_generator(*this);
		subquery->subquery = unique_ptr_cast<SQLStatement, SelectStatement>(child_generator.GenerateSelect());
	}
	subquery->subquery_type =
	    Choose<SubqueryType>({SubqueryType::ANY, SubqueryType::EXISTS, SubqueryType::SCALAR, SubqueryType::NOT_EXISTS});
	if (subquery->subquery_type == SubqueryType::ANY || subquery->subquery_type == SubqueryType::SCALAR) {
		subquery->child = GenerateExpression();
	}
	if (subquery->subquery_type == SubqueryType::ANY) {
		subquery->comparison_type = GenerateComparisonType();
	}
	return std::move(subquery);
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateCast() {
	auto child = GenerateExpression();
	auto type = GenerateLogicalType();
	return make_uniq<CastExpression>(std::move(type), std::move(child), RandomBoolean());
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateBetween() {
	auto input = GenerateExpression();
	auto lower = GenerateExpression();
	auto upper = GenerateExpression();
	return make_uniq<BetweenExpression>(std::move(input), std::move(lower), std::move(upper));
}

ExpressionType StatementGenerator::GenerateComparisonType() {
	static vector<ExpressionType> comparisons {ExpressionType::COMPARE_EQUAL,
	                                           ExpressionType::COMPARE_NOTEQUAL,
	                                           ExpressionType::COMPARE_LESSTHAN,
	                                           ExpressionType::COMPARE_GREATERTHAN,
	                                           ExpressionType::COMPARE_LESSTHANOREQUALTO,
	                                           ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                           ExpressionType::COMPARE_DISTINCT_FROM,
	                                           ExpressionType::COMPARE_NOT_DISTINCT_FROM};
	return Choose(comparisons);
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateComparison() {
	auto lhs = GenerateExpression();
	auto rhs = GenerateExpression();
	return make_uniq<ComparisonExpression>(GenerateComparisonType(), std::move(lhs), std::move(rhs));
}

unique_ptr<ParsedExpression> StatementGenerator::GeneratePositionalReference() {
	return make_uniq<PositionalReferenceExpression>(1 + RandomValue(10));
}

unique_ptr<ParsedExpression> StatementGenerator::GenerateCase() {
	auto case_stmt = make_uniq<CaseExpression>();
	case_stmt->else_expr = GenerateExpression();
	while (true) {
		CaseCheck check;
		check.then_expr = GenerateExpression();
		check.when_expr = GenerateExpression();
		case_stmt->case_checks.push_back(std::move(check));
		if (RandomPercentage(50)) {
			break;
		}
	}
	return std::move(case_stmt);
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string StatementGenerator::GenerateRelationName() {
	if (parent) {
		auto parent_relation = parent->GenerateRelationName();
		if (current_relation_names.empty()) {
			return parent_relation;
		}
		if (parent_relation.empty() || RandomPercentage(80)) {
			return Choose(current_relation_names);
		}
		return parent_relation;
	} else {
		if (current_relation_names.empty()) {
			return string();
		}
		return Choose(current_relation_names);
	}
}

string StatementGenerator::GenerateColumnName() {
	if (parent) {
		auto parent_column = parent->GenerateColumnName();
		if (current_column_names.empty()) {
			return parent_column;
		}
		if (parent_column.empty() || RandomPercentage(80)) {
			return Choose(current_column_names);
		}
		return parent_column;
	} else {
		if (current_column_names.empty()) {
			return string();
		}
		return Choose(current_column_names);
	}
}

idx_t StatementGenerator::RandomValue(idx_t max) {
	if (max == 0) {
		return 0;
	}
	return RandomEngine::Get(context).NextRandomInteger() % max;
}

bool StatementGenerator::RandomBoolean() {
	return RandomValue(2) == 0;
}

bool StatementGenerator::RandomPercentage(idx_t percentage) {
	if (percentage > 100) {
		return true;
	}
	return RandomValue(100) <= percentage;
}

//===--------------------------------------------------------------------===//
// Exhaustive Function Generation
//===--------------------------------------------------------------------===//
bool StatementGenerator::FunctionArgumentsAlwaysNull(const string &name) {
	// some functions run for a very long time with extreme parameters because they e.g. generate giant strings
	// for that reason we skip testing those functions with extreme parameters
	static case_insensitive_set_t always_null_functions {"rpad", "pad", "lpad", "repeat"};

	return always_null_functions.find(name) != always_null_functions.end();
}
string StatementGenerator::GenerateTestAllTypes(BaseScalarFunction &base_function) {
	auto select = make_uniq<SelectStatement>();
	auto node = make_uniq<SelectNode>();

	bool always_null = FunctionArgumentsAlwaysNull(base_function.name);

	vector<unique_ptr<ParsedExpression>> children;
	for (auto &arg : base_function.arguments) {
		// look up the type
		unique_ptr<ParsedExpression> argument;
		if (!always_null) {
			for (auto &test_type : generator_context->test_types) {
				if (test_type.type.id() == arg.id()) {
					argument = make_uniq<ColumnRefExpression>(test_type.name);
				}
			}
		}
		if (!argument) {
			argument = make_uniq<ConstantExpression>(Value(arg));
		}
		children.push_back(std::move(argument));
	}
	auto from_clause = make_uniq<BaseTableRef>();
	from_clause->table_name = "all_types";
	node->from_table = std::move(from_clause);

	auto function_expr = make_uniq<FunctionExpression>(base_function.name, std::move(children));
	node->select_list.push_back(std::move(function_expr));

	select->node = std::move(node);
	return select->ToString();
}

string StatementGenerator::GenerateTestVectorTypes(BaseScalarFunction &base_function) {
	auto select = make_uniq<SelectStatement>();
	auto node = make_uniq<SelectNode>();

	bool always_null = FunctionArgumentsAlwaysNull(base_function.name);

	vector<unique_ptr<ParsedExpression>> children;
	vector<unique_ptr<ParsedExpression>> test_vector_types;
	vector<string> column_aliases;
	for (auto &arg : base_function.arguments) {
		unique_ptr<ParsedExpression> argument;
		if (!always_null) {
			string argument_name = "c" + to_string(column_aliases.size() + 1);
			column_aliases.push_back(argument_name);
			argument = make_uniq<ColumnRefExpression>(std::move(argument_name));
			auto constant_expr = make_uniq<ConstantExpression>(Value());
			auto cast = make_uniq<CastExpression>(arg, std::move(constant_expr));
			test_vector_types.push_back(std::move(cast));
		} else {
			argument = make_uniq<ConstantExpression>(Value(arg));
		}
		children.push_back(std::move(argument));
	}
	auto from_clause = make_uniq<TableFunctionRef>();
	auto vector_types_fun = make_uniq<FunctionExpression>("test_vector_types", std::move(test_vector_types));
	from_clause->function = std::move(vector_types_fun);
	from_clause->alias = "test_vector_types";
	from_clause->column_name_alias = std::move(column_aliases);
	node->from_table = std::move(from_clause);

	auto function_expr = make_uniq<FunctionExpression>(base_function.name, std::move(children));
	node->select_list.push_back(std::move(function_expr));

	select->node = std::move(node);
	return select->ToString();
}

string StatementGenerator::GenerateCast(const LogicalType &target, const string &source_name, bool add_varchar) {
	auto select = make_uniq<SelectStatement>();
	auto node = make_uniq<SelectNode>();

	auto from_clause = make_uniq<BaseTableRef>();
	from_clause->table_name = "all_types";
	node->from_table = std::move(from_clause);

	unique_ptr<ParsedExpression> source;
	source = make_uniq<ColumnRefExpression>(source_name);
	if (add_varchar) {
		source = make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(source));
	}
	auto cast = make_uniq<CastExpression>(target, std::move(source));
	node->select_list.push_back(std::move(cast));

	select->node = std::move(node);
	return select->ToString();
}

void StatementGenerator::GenerateAllScalar(ScalarFunctionCatalogEntry &scalar_function, vector<string> &result) {
	for (idx_t offset = 0; offset < scalar_function.functions.Size(); offset++) {
		auto function = scalar_function.functions.GetFunctionByOffset(offset);

		result.push_back(GenerateTestAllTypes(function));
		result.push_back(GenerateTestVectorTypes(function));
	}
}

void StatementGenerator::GenerateAllAggregate(AggregateFunctionCatalogEntry &aggregate_function,
                                              vector<string> &result) {
	for (idx_t offset = 0; offset < aggregate_function.functions.Size(); offset++) {
		auto function = aggregate_function.functions.GetFunctionByOffset(offset);

		result.push_back(GenerateTestAllTypes(function));
		result.push_back(GenerateTestVectorTypes(function));
	}
}

vector<string> StatementGenerator::GenerateAllFunctionCalls() {
	// all scalar functions
	vector<string> result;
	for (auto &function_ref : generator_context->scalar_functions) {
		auto &function = function_ref.get();
		switch (function.type) {
		case CatalogType::SCALAR_FUNCTION_ENTRY: {
			auto &scalar_entry = function.Cast<ScalarFunctionCatalogEntry>();
			GenerateAllScalar(scalar_entry, result);
			break;
		}
		case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
			auto &aggregate_entry = function.Cast<AggregateFunctionCatalogEntry>();
			GenerateAllAggregate(aggregate_entry, result);
			break;
		}
		case CatalogType::MACRO_ENTRY:
		default:
			break;
		}
	}
	// generate all casts
	for (auto &source_type : generator_context->test_types) {
		for (auto &target_type : generator_context->test_types) {
			result.push_back(GenerateCast(target_type.type, source_type.name, false));
			result.push_back(GenerateCast(target_type.type, source_type.name, true));
		}
	}
	return result;
}

} // namespace duckdb
