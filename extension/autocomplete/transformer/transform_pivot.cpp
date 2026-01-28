#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {
void PEGTransformerFactory::AddPivotEntry(PEGTransformer &transformer, string enum_name, unique_ptr<SelectNode> base,
                                          unique_ptr<ParsedExpression> column, unique_ptr<QueryNode> subquery,
                                          bool has_parameters) {
	auto result = make_uniq<CreatePivotEntry>();
	result->enum_name = std::move(enum_name);
	result->base = std::move(base);
	result->column = std::move(column);
	result->subquery = std::move(subquery);
	result->has_parameters = has_parameters;
	transformer.pivot_entries.push_back(std::move(result));
}

vector<PivotColumn> PEGTransformerFactory::TransformPivotOn(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr_list = ExtractParseResultsFromList(list_pr.GetChild(1));
	vector<PivotColumn> result;
	for (auto &expr_pr : expr_list) {
		PivotColumn col;
		auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(expr_pr);
		if (expr->IsScalar()) {
			throw ParserException(expr->GetQueryLocation(), "Cannot pivot on constant value \"%s\"", expr->ToString());
		}
		if (expr->HasSubquery()) {
			throw ParserException(expr->GetQueryLocation(), "Cannot pivot on subquery \"%s\"", expr->ToString());
		}
		col.pivot_expressions.push_back(std::move(expr));
		result.push_back(std::move(col));
	}
	return result;
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformPivotStatement(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto current_param_count = transformer.ParamCount();
	auto source = transformer.Transform<unique_ptr<TableRef>>(list_pr.GetChild(1));
	auto next_param_count = transformer.ParamCount();
	bool has_parameters = next_param_count > current_param_count;
	auto pivot_columns = list_pr.Child<OptionalParseResult>(2);
	auto select_node = make_uniq<SelectNode>();
	auto pivot_group = list_pr.Child<OptionalParseResult>(4);
	if (!pivot_columns.HasResult()) {
		select_node->from_table = transformer.Transform<unique_ptr<TableRef>>(list_pr.GetChild(1));
		if (pivot_group.HasResult()) {
			auto pivot_group_list = transformer.Transform<vector<string>>(pivot_group.optional_result);
			GroupingSet set;
			for (idx_t gr = 0; gr < pivot_group_list.size(); gr++) {
				auto &group = pivot_group_list[gr];
				auto colref = make_uniq<ColumnRefExpression>(group);
				select_node->select_list.push_back(colref->Copy());
				select_node->groups.group_expressions.push_back(std::move(colref));
				set.insert(gr);
			}
			select_node->groups.grouping_sets.push_back(std::move(set));
		}

		vector<unique_ptr<ParsedExpression>> pivot_using;
		transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 3, pivot_using);
		for (auto &col : pivot_using) {
			select_node->select_list.push_back(std::move(col));
		}
		auto result = make_uniq<SelectStatement>();
		result->node = std::move(select_node);
		return result;
	}
	auto columns = transformer.Transform<vector<PivotColumn>>(pivot_columns.optional_result);
	for (idx_t c = 0; c < columns.size(); c++) {
		auto &col = columns[c];
		if (!col.pivot_enum.empty() || !col.entries.empty()) {
			continue;
		}
		if (col.pivot_expressions.size() != 1) {
			throw InternalException("PIVOT statement with multiple names in pivot entry!?");
		}
		auto enum_name = "__pivot_enum_" + UUID::ToString(UUID::GenerateRandomUUID());

		auto new_select = make_uniq<SelectNode>();
		new_select->from_table = source->Copy();
		AddPivotEntry(transformer, enum_name, std::move(new_select), col.pivot_expressions[0]->Copy(),
		              std::move(col.subquery), has_parameters);
		col.pivot_enum = enum_name;
	}

	// Generate the actual query, including the pivot
	select_node->select_list.push_back(make_uniq<StarExpression>());

	auto pivot_ref = make_uniq<PivotRef>();
	pivot_ref->source = std::move(source);
	auto pivot_aggregates = list_pr.Child<OptionalParseResult>(3);
	if (pivot_aggregates.HasResult()) {
		pivot_ref->aggregates =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(pivot_aggregates.optional_result);
	} else {
		// pivot but no aggregates specified - push a count star
		vector<unique_ptr<ParsedExpression>> children;
		auto function = make_uniq<FunctionExpression>("count_star", std::move(children));
		pivot_ref->aggregates.push_back(std::move(function));
	}
	if (pivot_group.HasResult()) {
		pivot_ref->groups = transformer.Transform<vector<string>>(pivot_group.optional_result);
	}
	pivot_ref->pivots = std::move(columns);
	select_node->from_table = std::move(pivot_ref);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return select_statement;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformPivotUsing(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.GetChild(1));
}

vector<string> PEGTransformerFactory::TransformUnpivotHeader(PEGTransformer &transformer,
                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<string>>(list_pr.Child<ChoiceParseResult>(0).result);
}

vector<string> PEGTransformerFactory::TransformUnpivotHeaderSingle(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> result;
	result.push_back(transformer.Transform<string>(list_pr.GetChild(0)));
	return result;
}

vector<string> PEGTransformerFactory::TransformUnpivotHeaderList(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.GetChild(0));
	auto col_list = ExtractParseResultsFromList(extract_parens);
	vector<string> result;
	for (auto col : col_list) {
		result.push_back(transformer.Transform<string>(col));
	}
	return result;
}

bool PEGTransformerFactory::TransformIncludeOrExcludeNulls(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<bool>(list_pr.Child<ChoiceParseResult>(0).result);
}

} // namespace duckdb
