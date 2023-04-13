#include "duckdb/parser/statement/select_statement.hpp"

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

SelectStatement::SelectStatement(const SelectStatement &other) : SQLStatement(other), node(other.node->Copy()) {
}

unique_ptr<SQLStatement> SelectStatement::Copy() const {
	return unique_ptr<SelectStatement>(new SelectStatement(*this));
}

void SelectStatement::Serialize(Serializer &serializer) const {
	node->Serialize(serializer);
}

void SelectStatement::FormatSerialize(FormatSerializer &serializer) const {
	serializer.WriteProperty("node", node);
}

unique_ptr<SelectStatement> SelectStatement::Deserialize(Deserializer &source) {
	auto result = make_uniq<SelectStatement>();
	result->node = QueryNode::Deserialize(source);
	return result;
}

unique_ptr<SelectStatement> SelectStatement::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = make_uniq<SelectStatement>();
	deserializer.ReadProperty("node", result->node);
	return result;
}

bool SelectStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto &other = other_p->Cast<SelectStatement>();
	return node->Equals(other.node.get());
}

string SelectStatement::ToString() const {
	return node->ToString();
}

} // namespace duckdb

/*
 json_serialize_sql('SELECT BLOB ''\x01\x10'';', format := CAST('t' AS BOOLEAN)){
"error": false,
"statements": [
    {
        "node": {
            "type": "SELECT_NODE",
            "modifiers": [],
            "cte_map": {
                "map": []
            },
            "select_list": [
                {
                    "class": "CONSTANT",
                    "type": "CONSTANT",
                    "alias": "",
                    "value": {
                        "type": {
                            "id": "BLOB",
                            "type_info": null
                        },
                        "is_null": false,
                        "value": "\u0001\u0010"
                    }
                }
            ],
            "from_table": {
                "type": "EMPTY",
                "alias": "",
                "sample": null
            },
            "where_clause": null,
            "group_expressions": [],
            "group_sets": [],
            "aggregate_handling": "STANDARD_HANDLING",
            "having": null,
            "sample": null,
            "qualify": null
        }
    }
]
}
 */
