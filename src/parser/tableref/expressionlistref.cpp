#include "duckdb/parser/tableref/expressionlistref.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string ExpressionListRef::ToString() const {
	D_ASSERT(!values.empty());
	string result = "(VALUES ";
	for (idx_t row_idx = 0; row_idx < values.size(); row_idx++) {
		if (row_idx > 0) {
			result += ", ";
		}
		auto &row = values[row_idx];
		result += "(";
		for (idx_t col_idx = 0; col_idx < row.size(); col_idx++) {
			if (col_idx > 0) {
				result += ", ";
			}
			result += row[col_idx]->ToString();
		}
		result += ")";
	}
	result += ")";
	return BaseToString(result, expected_names);
}

bool ExpressionListRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (ExpressionListRef *)other_p;
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
		new_val_list.reserve(val_list.size());
		for (auto &val : val_list) {
			new_val_list.push_back(val->Copy());
		}
		result->values.push_back(move(new_val_list));
	}
	result->expected_names = expected_names;
	result->expected_types = expected_types;
	CopyProperties(*result);
	return move(result);
}

void ExpressionListRef::Serialize(FieldWriter &writer) const {
	writer.WriteList<string>(expected_names);
	writer.WriteRegularSerializableList<LogicalType>(expected_types);
	auto &serializer = writer.GetSerializer();
	writer.WriteField<uint32_t>(values.size());
	for (idx_t i = 0; i < values.size(); i++) {
		serializer.WriteList(values[i]);
	}
}

unique_ptr<TableRef> ExpressionListRef::Deserialize(FieldReader &reader) {
	auto result = make_unique<ExpressionListRef>();
	// value list
	result->expected_names = reader.ReadRequiredList<string>();
	result->expected_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	idx_t value_list_size = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < value_list_size; i++) {
		vector<unique_ptr<ParsedExpression>> value_list;
		source.ReadList<ParsedExpression>(value_list);
		result->values.push_back(move(value_list));
	}
	return move(result);
}

} // namespace duckdb
