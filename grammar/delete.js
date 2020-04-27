
function GenerateValues(options) {
	return [
		Keyword("VALUES"),
		OneOrMore(
			Sequence([
				Keyword("("),
				OneOrMore(Expression(), ","),
				Keyword(")")
			]), Keyword(","))
	]
}

function GenerateDistinctClause(options) {
	return [
		Choice(0, [
			new Skip(),
			Sequence([
				Keyword("DISTINCT"),
				Optional(Sequence([
					Keyword("("),
					OneOrMore(Expression("distinct-term"), ","),
					Keyword(")"),
				]) , "skip")
			]),
			Keyword("ALL")
		])
	]
}

function GenerateSelectNode(options) {
	return [Stack([
		Sequence([
			Keyword("SELECT"),
			Expandable("distinct-clause", options, "distinct-clause", GenerateDistinctClause),
			OneOrMore(Expression(), ",")
		]),
		Sequence([
			Optional(Sequence([
				Keyword("FROM"),
				Choice(0, [
					OneOrMore(Expression("table-or-subquery"), ","),
					Sequence([Expression("join-clause")])
				])
			])),
			Optional(
				Sequence([
					Keyword("WHERE"),
					Expression()
				])
			)
		]),
		Sequence([
			Optional(Sequence([
				Keyword("GROUP"),
				Keyword("BY"),
				OneOrMore(Expression(), ","),
			])),
			Optional(Sequence([
				Keyword("HAVING"),
				Expression()
			]))
		]),
		Optional(
			Sequence([Sequence([
					Keyword("WINDOW"),
					Expression("window-name"),
					Keyword("AS"),
					Expression("window-definition")
				])
			]), "skip"),
		Sequence([
			Optional(Sequence([
				Keyword("ORDER"),
				Keyword("BY"),
				OneOrMore(Expression("ordering-term"), ",")
			]))
		]),
		Optional(Sequence([
			Keyword("LIMIT"),
			Expression(),
			Optional(Sequence([
				Keyword("OFFSET"),
				Expression()
			]), "skip")
		]))
	])]
}

function GenerateDelete(options = {}) {
	return Diagram([
		Stack([
			Optional(
				Sequence([
					Keyword("WITH"),
					Optional(Keyword("RECURSIVE"), "skip"),
					OneOrMore(Expression("common-table-expr"), ",")
				]), "skip"),
			Choice(0, [
				Expandable("select-node", options, "select-node", GenerateSelectNode),
				Expandable("values-list", options, "values", GenerateValues)
			])
		])
	])
}

function Initialize(options = {}) {
	document.getElementById("rrdiagram").innerHTML = GenerateDelete(options).toString();
}

function Refresh(node_name, set_node) {
	options[node_name] = set_node;
	Initialize(options);
}

options = {}
Initialize()


