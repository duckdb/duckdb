#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/parser/property_graph_table.hpp"

namespace duckdb {

PropertyGraphTable::PropertyGraphTable() = default;

PropertyGraphTable::PropertyGraphTable(string table_name_p, vector<string> column_names_p, vector<string> labels_p)
    : table_name(std::move(table_name_p)), column_names(std::move(column_names_p)), sub_labels(std::move(labels_p)) {

#ifdef DEBUG
	for (auto &col_name : column_names) {
		D_ASSERT(!col_name.empty());
	}

	for (auto &label : sub_labels) {
		D_ASSERT(!label.empty());
	}
#endif
}

PropertyGraphTable::PropertyGraphTable(string table_name_p, string table_name_alias_p, vector<string> column_names_p,
                                       vector<string> labels_p)
    : table_name(std::move(table_name_p)), table_name_alias(std::move(table_name_alias_p)),
      column_names(std::move(column_names_p)), sub_labels(std::move(labels_p)) {
#ifdef DEBUG
	for (auto &col_name : column_names) {
		D_ASSERT(!col_name.empty());
	}
	for (auto &except_column : except_columns) {
		D_ASSERT(!except_column.empty());
	}

	for (auto &label : sub_labels) {
		D_ASSERT(!label.empty());
	}
#endif
}

string PropertyGraphTable::ToString() const {
	string result = table_name + " " + (table_name_alias.empty() ? "" : "AS " + table_name_alias);
	if (!is_vertex_table) {
		result += "SOURCE KEY (";
		for (idx_t i = 0; i < source_fk.size(); i++) {
			if (i != source_fk.size() - 1) {
				result += source_fk[i] + ", ";
			} else {
				// Last element should be without a trailing , instead )
				result = source_fk[i] + ") ";
			}
		}
		result += "REFERENCES " + source_reference + " (";
		for (idx_t i = 0; i < source_pk.size(); i++) {
			if (i != source_pk.size() - 1) {
				result += source_pk[i] + ", ";
			} else {
				result = source_pk[i] + ") ";
			}
		}
		result += "\n";
		result += "DESTINATION KEY (";
		for (idx_t i = 0; i < destination_fk.size(); i++) {
			if (i != destination_fk.size() - 1) {
				result += destination_fk[i] + ", ";
			} else {
				// Last element should be without a trailing , instead )
				result = destination_fk[i] + ") ";
			}
		}
		result += "REFERENCES " + destination_reference + " (";
		for (idx_t i = 0; i < destination_pk.size(); i++) {
			if (i != destination_pk.size() - 1) {
				result += destination_pk[i] + ", ";
			} else {
				result = destination_pk[i] + ") ";
			}
		}
	}
	result += "\n";
	result += "PROPERTIES (";
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (i != column_names.size() - 1) {
			result += column_names[i] + (column_aliases[i].empty() ? "" : "AS " + column_aliases[i]) + ", ";
		} else {
			result = column_names[i] + (column_aliases[i].empty() ? "" : "AS " + column_aliases[i]) + ") ";
		}
	}

	result += "LABEL " + main_label;
	if (!sub_labels.empty()) {
		result += " IN " + discriminator + "( ";
		for (idx_t i = 0; i < sub_labels.size(); i++) {
			if (i != sub_labels.size() - 1) {
				result += sub_labels[i] + ", ";
			} else {
				result = sub_labels[i] + ") ";
			}
		}
	}

	return result;
}

bool PropertyGraphTable::Equals(const PropertyGraphTable *other_p) const {

	auto other = (PropertyGraphTable *)other_p;
	if (table_name != other->table_name) {
		return false;
	}

	if (table_name_alias != other->table_name_alias) {
		return false;
	}

	if (column_names.size() != other->column_names.size()) {
		return false;
	}
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (column_names[i] != other->column_names[i]) {
			return false;
		}
	}
	if (column_aliases.size() != other->column_aliases.size()) {
		return false;
	}
	for (idx_t i = 0; i < column_aliases.size(); i++) {
		if (column_aliases[i] != other->column_aliases[i]) {
			return false;
		}
	}
	if (except_columns.size() != other->except_columns.size()) {
		return false;
	}
	for (idx_t i = 0; i < except_columns.size(); i++) {
		if (except_columns[i] != other->except_columns[i]) {
			return false;
		}
	}
	if (sub_labels.size() != other->sub_labels.size()) {
		return false;
	}
	for (idx_t i = 0; i < sub_labels.size(); i++) {
		if (sub_labels[i] != other->sub_labels[i]) {
			return false;
		}
	}

	if (main_label != other->main_label) {
		return false;
	}
	if (all_columns != other->all_columns) {
		return false;
	}
	if (no_columns != other->no_columns) {
		return false;
	}
	if (is_vertex_table != other->is_vertex_table) {
		return false;
	}
	if (discriminator != other->discriminator) {
		return false;
	}
	if (source_fk.size() != other->source_fk.size()) {
		return false;
	}
	for (idx_t i = 0; i < source_fk.size(); i++) {
		if (source_fk[i] != other->source_fk[i]) {
			return false;
		}
	}
	if (source_pk.size() != other->source_pk.size()) {
		return false;
	}
	for (idx_t i = 0; i < source_pk.size(); i++) {
		if (source_pk[i] != other->source_pk[i]) {
			return false;
		}
	}
	if (source_reference != other->source_reference) {
		return false;
	}

	if (destination_fk.size() != other->destination_fk.size()) {
		return false;
	}
	for (idx_t i = 0; i < destination_fk.size(); i++) {
		if (destination_fk[i] != other->destination_fk[i]) {
			return false;
		}
	}

	if (destination_pk.size() != other->destination_pk.size()) {
		return false;
	}
	for (idx_t i = 0; i < destination_pk.size(); i++) {
		if (destination_pk[i] != other->destination_pk[i]) {
			return false;
		}
	}
	if (destination_reference != other->destination_reference) {
		return false;
	}

	return true;
}

void PropertyGraphTable::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(table_name);

	writer.WriteList<string>(column_names);
	writer.WriteList<string>(column_aliases);
	writer.WriteList<string>(except_columns);
	writer.WriteList<string>(sub_labels);
	writer.WriteString(main_label);

	writer.WriteField<bool>(is_vertex_table);
	writer.WriteField<bool>(all_columns);
	writer.WriteField<bool>(no_columns);
	if (!is_vertex_table) {
		writer.WriteList<string>(source_pk);
		writer.WriteList<string>(source_fk);
		writer.WriteString(source_reference);

		writer.WriteList<string>(destination_pk);
		writer.WriteList<string>(destination_fk);
		writer.WriteString(destination_reference);
	}
	writer.Finalize();
}

shared_ptr<PropertyGraphTable> PropertyGraphTable::Deserialize(Deserializer &deserializer) {
	auto pg_table = make_uniq<PropertyGraphTable>();
	FieldReader reader(deserializer);
	pg_table->table_name = reader.ReadRequired<string>();
	reader.ReadList<string>(pg_table->column_names);
	reader.ReadList<string>(pg_table->column_aliases);
	reader.ReadList<string>(pg_table->except_columns);
	reader.ReadList<string>(pg_table->sub_labels);
	pg_table->main_label = reader.ReadRequired<string>();

	pg_table->is_vertex_table = reader.ReadRequired<bool>();
	pg_table->all_columns = reader.ReadRequired<bool>();
	pg_table->no_columns = reader.ReadRequired<bool>();
	if (!pg_table->is_vertex_table) {
		reader.ReadList<string>(pg_table->source_pk);
		reader.ReadList<string>(pg_table->source_fk);
		pg_table->source_reference = reader.ReadRequired<string>();

		reader.ReadList<string>(pg_table->destination_pk);
		reader.ReadList<string>(pg_table->destination_fk);
		pg_table->destination_reference = reader.ReadRequired<string>();
	}
	reader.Finalize();
	return std::move(pg_table);
}

shared_ptr<PropertyGraphTable> PropertyGraphTable::Copy() const {
	auto result = make_uniq<PropertyGraphTable>();

	result->table_name = table_name;
	for (auto &column_name : column_names) {
		result->column_names.push_back(column_name);
	}
	for (auto &except_column : except_columns) {
		result->except_columns.push_back(except_column);
	}
	for (auto &label : sub_labels) {
		result->sub_labels.push_back(label);
	}

	result->main_label = main_label;
	result->is_vertex_table = is_vertex_table;
	result->all_columns = all_columns;
	result->no_columns = no_columns;
	result->discriminator = discriminator;

	result->source_reference = source_reference;

	for (auto &key : source_fk) {
		result->source_fk.push_back(key);
	}

	for (auto &key : source_pk) {
		result->source_pk.push_back(key);
	}

	result->destination_reference = destination_reference;

	for (auto &key : destination_fk) {
		result->destination_fk.push_back(key);
	}

	for (auto &key : destination_pk) {
		result->destination_pk.push_back(key);
	}
	return std::move(result);
}

} // namespace duckdb
