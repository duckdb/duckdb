#include "duckdb/parser/tableref/expressionlistref.hpp"

#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

bool ExpressionListRef::Equals(const TableRef *other_) const {
	if (!TableRef::Equals(other_)) {
		return false;
	}
	auto other = (ExpressionListRef *)other_;
	if (values.size() != other->values.size()) {
		return false;
	}
	for (idx_t i = 0; i < values.size(); i++) {
		if (values[i].size() != other->values[i].size()) {
			return false;
		}
		for (idx_t j = 0; j < values[i].size(); j++) {
			if (!values[i][j]->Equals(other->values[i][j].get())) {
				return false;
			}
		}
	}
	return true;
}

unique_ptr<TableRef> ExpressionListRef::Copy() {
	// value list
	auto result = make_unique<ExpressionListRef>();
	for (auto &val_list : values) {
		vector<unique_ptr<ParsedExpression>> new_val_list;
		for (auto &val : val_list) {
			new_val_list.push_back(val->Copy());
		}
		result->values.push_back(move(new_val_list));
	}
	result->alias = alias;
	return move(result);
}

void ExpressionListRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
	serializer.Write<idx_t>(values.size());
	for (idx_t i = 0; i < values.size(); i++) {
		serializer.WriteList(values[i]);
	}
}

unique_ptr<TableRef> ExpressionListRef::Deserialize(Deserializer &source) {
	auto result = make_unique<ExpressionListRef>();
	// value list
	idx_t value_list_size = source.Read<idx_t>();
	for (idx_t i = 0; i < value_list_size; i++) {
		vector<unique_ptr<ParsedExpression>> value_list;
		source.ReadList<ParsedExpression>(value_list);
		result->values.push_back(move(value_list));
	}
	return move(result);
}
