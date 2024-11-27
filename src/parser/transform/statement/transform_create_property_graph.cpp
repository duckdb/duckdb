#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/property_graph_table.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

shared_ptr<PropertyGraphTable>
Transformer::TransformPropertyGraphTable(duckdb_libpgquery::PGPropertyGraphTable *graph_table,
                                         case_insensitive_set_t &global_label_set,
                                         case_insensitive_map_t<string> &table_alias_map) {
	vector<string> column_names;
	vector<string> except_list;
	vector<string> label_names;

	auto table_name = reinterpret_cast<duckdb_libpgquery::PGRangeVar *>(graph_table->table->head->data.ptr_value);
	auto transformed_name = TransformQualifiedName(*table_name);
	string table_name_alias =
	    reinterpret_cast<duckdb_libpgquery::PGValue *>(graph_table->table->head->next->data.ptr_value)->val.str;
	if (!table_name_alias.empty()) {
		table_alias_map[table_name_alias] = transformed_name.name;
	}

	bool all_columns = false;
	bool no_columns = graph_table->properties == nullptr;

	if (!no_columns) {
		for (auto property_element = graph_table->properties->head; property_element != nullptr;
		     property_element = property_element->next) {
			auto column_optional_as = reinterpret_cast<duckdb_libpgquery::PGList *>(property_element->data.ptr_value);
			auto column_name =
			    reinterpret_cast<duckdb_libpgquery::PGColumnDef *>(column_optional_as->head->data.ptr_value);
			if (strcmp(column_name->colname, "*") == 0) {
				all_columns = true;
				continue;
			}
			// auto column_alias = reinterpret_cast<duckdb_libpgquery::PGColumnDef*>(column_optional_as->head->next->data.ptr_value);
			// TODO
			//  	- 	Change this to support the optional as
			// 		  	Looking at the next element of column_optional_as, which is a linked list
			// 			If the string is equal to the first string then there is no alias

			//! Every column listed after * is seen as part of the except columns
			all_columns ? except_list.emplace_back(column_name->colname)
			            : column_names.emplace_back(column_name->colname);
		}
	}

	for (auto label_element = graph_table->labels->head; label_element != nullptr;
	     label_element = label_element->next) {
		auto label = reinterpret_cast<duckdb_libpgquery::PGValue *>(label_element->data.ptr_value);
		D_ASSERT(label->type == duckdb_libpgquery::T_PGString);
		std::string label_str;
		if (label->val.str == nullptr) {
			label_str = transformed_name.name;
		} else {
			label_str = label->val.str;
		}
		label_str = StringUtil::Lower(label_str);
		if (global_label_set.find(label_str) != global_label_set.end()) {
			throw ConstraintException("Label %s is not unique, make sure all labels are unique", label_str);
		}
		global_label_set.insert(label_str);
		label_names.emplace_back(label_str);
	}

	auto pg_table = make_shared_ptr<PropertyGraphTable>(transformed_name.name,
		table_name_alias, column_names, label_names);

	if (!transformed_name.schema.empty()) {
		pg_table->catalog_name = transformed_name.catalog.empty() ? transformed_name.schema : transformed_name.catalog;
		if (!transformed_name.catalog.empty()) {
			pg_table->schema_name = transformed_name.schema;
		}
	}
	pg_table->is_vertex_table = graph_table->is_vertex_table;
	pg_table->except_columns = std::move(except_list);
	pg_table->all_columns = all_columns;
	pg_table->no_columns = no_columns;

	if (graph_table->discriminator) {
		//! In this case there is a list with length > 1 of labels
		//! of which the last element in the list is the main label
		auto discriminator = TransformQualifiedName(*graph_table->discriminator);
		pg_table->discriminator = discriminator.name;
	}
	pg_table->main_label = pg_table->sub_labels[pg_table->sub_labels.size() - 1];
	pg_table->sub_labels.pop_back();

	//! Everything from this point is only related to edge tables
	if (!graph_table->is_vertex_table) {
		D_ASSERT(graph_table->src_name);
		auto src_name = TransformQualifiedName(*graph_table->src_name);
		auto possible_src_alias = table_alias_map.find(src_name.name);
		if (possible_src_alias == table_alias_map.end()) {
			pg_table->source_reference = src_name.name;
		} else {
			pg_table->source_reference = possible_src_alias->second;
		}
		D_ASSERT(graph_table->dst_name);
		auto dst_name = TransformQualifiedName(*graph_table->dst_name);
		auto possible_dst_alias = table_alias_map.find(dst_name.name);
		if (possible_dst_alias == table_alias_map.end()) {
			pg_table->destination_reference = dst_name.name;
		} else {
			pg_table->destination_reference = possible_dst_alias->second;
		}

		if (graph_table->src_pk) {
			for (auto &src_key = graph_table->src_pk->head; src_key != nullptr; src_key = lnext(src_key)) {
				auto key = reinterpret_cast<duckdb_libpgquery::PGValue *>(src_key->data.ptr_value);
				pg_table->source_pk.emplace_back(key->val.str);
			}
		}

		if (graph_table->dst_pk) {
			for (auto &dst_key = graph_table->dst_pk->head; dst_key != nullptr; dst_key = lnext(dst_key)) {
				auto key = reinterpret_cast<duckdb_libpgquery::PGValue *>(dst_key->data.ptr_value);
				pg_table->destination_pk.emplace_back(key->val.str);
			}
		}

		if (graph_table->src_fk) {
			for (auto &src_key = graph_table->src_fk->head; src_key != nullptr; src_key = lnext(src_key)) {
				auto key = reinterpret_cast<duckdb_libpgquery::PGValue *>(src_key->data.ptr_value);
				pg_table->source_fk.emplace_back(key->val.str);
			}
		}

		if (graph_table->dst_fk) {
			for (auto &dst_key = graph_table->dst_fk->head; dst_key != nullptr; dst_key = lnext(dst_key)) {
				auto key = reinterpret_cast<duckdb_libpgquery::PGValue *>(dst_key->data.ptr_value);
				pg_table->destination_fk.emplace_back(key->val.str);
			}
		}
	}
	return pg_table;
}

unique_ptr<CreateStatement>
Transformer::TransformCreatePropertyGraph(duckdb_libpgquery::PGCreatePropertyGraphStmt &stmt) {
	auto info = make_uniq<CreatePropertyGraphInfo>();

	case_insensitive_set_t global_label_set;
	case_insensitive_map_t<string> table_alias_map;
	auto property_graph_name = TransformQualifiedName(*stmt.name);
	info->property_graph_name = property_graph_name.name;
	info->on_conflict = TransformOnConflict(stmt.onconflict);
	D_ASSERT(stmt.vertex_tables);
	for (auto &vertex_table = stmt.vertex_tables->head; vertex_table != nullptr; vertex_table = lnext(vertex_table)) {
		auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(vertex_table->data.ptr_value);

		if (node->type != duckdb_libpgquery::T_PGPropertyGraphTable) {
			throw NotImplementedException("CreatePropertyGraphTable not implemented.");
		}
		auto graph_table = reinterpret_cast<duckdb_libpgquery::PGPropertyGraphTable *>(vertex_table->data.ptr_value);
		auto pg_table = TransformPropertyGraphTable(graph_table, global_label_set, table_alias_map);
		for (auto &label : pg_table->sub_labels) {
			info->label_map[label] = pg_table;
		}
		info->label_map[pg_table->main_label] = pg_table;

		info->vertex_tables.push_back(std::move(pg_table));
	}

	if (stmt.edge_tables) {
		for (auto &edge_table = stmt.edge_tables->head; edge_table != nullptr; edge_table = lnext(edge_table)) {
			auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(edge_table->data.ptr_value);

			if (node->type != duckdb_libpgquery::T_PGPropertyGraphTable) {
				throw NotImplementedException("CreatePropertyGraphTable not implemented.");
			}
			auto graph_table = reinterpret_cast<duckdb_libpgquery::PGPropertyGraphTable *>(edge_table->data.ptr_value);
			auto pg_table = TransformPropertyGraphTable(graph_table, global_label_set, table_alias_map);
			for (auto &label : pg_table->sub_labels) {
				info->label_map[label] = pg_table;
			}
			info->label_map[pg_table->main_label] = pg_table;
			pg_table->source_pg_table = info->GetTableByName(pg_table->source_reference);
			D_ASSERT(pg_table->source_pg_table);
			pg_table->destination_pg_table = info->GetTableByName(pg_table->destination_reference);
			D_ASSERT(pg_table->destination_pg_table);
			info->edge_tables.push_back(std::move(pg_table));
		}
	}

	auto result = make_uniq<CreateStatement>();
	result->info = std::move(info);

	return result;
}

} // namespace duckdb
