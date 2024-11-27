//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_property_graph_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <vector>
#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/property_graph_table.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/catalog/catalog_entry.hpp"



namespace duckdb {

struct CreatePropertyGraphInfo : public CreateInfo {
	CreatePropertyGraphInfo();
	explicit CreatePropertyGraphInfo(string property_graph_name);

	//! Property graph name
	string property_graph_name;
	//! List of vector tables
	vector<shared_ptr<PropertyGraphTable>> vertex_tables;

	vector<shared_ptr<PropertyGraphTable>> edge_tables;

	//! Dictionary to point label to vector or edge table
	case_insensitive_map_t<shared_ptr<PropertyGraphTable>> label_map;

public:
	unique_ptr<CreateInfo> Copy() const override;

	string ToString() const override {
		string result = "-CREATE PROPERTY GRAPH " + property_graph_name + " ";
		result += "VERTEX TABLES ( ";
		for (idx_t i = 0; i < vertex_tables.size(); i++) {
			result += vertex_tables[i]->ToString();
			if (i != vertex_tables.size() - 1) {
				result += ", ";
			}
		}
		result += " ) ";
		result += "EDGE TABLES ( ";
		for (idx_t i = 0; i < edge_tables.size(); i++) {
			result += edge_tables[i]->ToString();
			if (i != edge_tables.size() - 1) {
				result += ", ";
			}
		}
		result += " ) ";
		return result;
	}

	static size_t LevenshteinDistance(const string &s1, const string &s2) {
		const size_t len1 = s1.size(), len2 = s2.size();
		std::vector<std::vector<size_t>> d(len1 + 1, std::vector<size_t>(len2 + 1));

		// Initialize the first row and column
		d[0][0] = 0;
		for (size_t i = 1; i <= len1; ++i) d[i][0] = i;
		for (size_t i = 1; i <= len2; ++i) d[0][i] = i;

		// Compute the Levenshtein distance
		for (size_t i = 1; i <= len1; ++i) {
			for (size_t j = 1; j <= len2; ++j) {
				// Calculate the cost for substitution
				size_t substitution_cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;

				// Compute deletion, insertion, and substitution costs
				size_t deletion = d[i - 1][j] + 1;
				size_t insertion = d[i][j - 1] + 1;
				size_t substitution = d[i - 1][j - 1] + substitution_cost;

				// Find the minimum of the three
				size_t min_cost = deletion; // Start with deletion
				if (insertion < min_cost) {
					min_cost = insertion;
				}
				if (substitution < min_cost) {
					min_cost = substitution;
				}

				// Set the minimum cost in the matrix
				d[i][j] = min_cost;
			}
		}

		return d[len1][len2];
	}

	shared_ptr<PropertyGraphTable> GetTableByName(const string &table_name, bool error_not_found = true, bool is_vertex_table = true) {
		if (is_vertex_table) {
			// search vertex tables
			for (const auto &vertex_table : vertex_tables) {
				if (vertex_table->table_name == table_name) {
					return vertex_table;
				}
			}
		} else {
			// Search edge tables
			for (const auto &edge_table : edge_tables) {
				if (edge_table->table_name == table_name) {
					return edge_table;
				}
			}
		}
		if (error_not_found) {
			throw Exception(ExceptionType::INVALID, "Table '" + table_name + "' not found in the property graph " + property_graph_name + ".");
		}
		return nullptr; // Return nullptr if no match is found and error_not_found is false
	}

	shared_ptr<PropertyGraphTable> GetTableByLabel(const string &label, bool error_not_found = true, bool is_vertex_table = true) {
	    // First, check if there is an exact match for the table name in label_map
	    auto table_entry = label_map.find(label);
	    if (table_entry != label_map.end()) {
	        // Exact table match found, but verify if it matches the vertex/edge type
	        if (table_entry->second->is_vertex_table == is_vertex_table) {
	            return table_entry->second;
	        }
	    	if (error_not_found) {
	            throw Exception(ExceptionType::INVALID,
	                            "Exact label '" + label + "' found, but it is not a " +
	                            (is_vertex_table ? "vertex" : "edge") + " table.");
	        }
	        return nullptr;
	    }

	    // If no exact table match is found, search for the closest label match using Levenshtein distance
	    string closest_label;
	    auto min_distance = std::numeric_limits<size_t>::max();

	    for (const auto &pair : label_map) {
	        const auto &pg_table = pair.second;

	        // Only consider tables of the correct type (vertex or edge)
	        if (pg_table->is_vertex_table != is_vertex_table) {
	            continue;
	        }
	    	if  (pg_table->table_name == label) {
	    		throw Exception(ExceptionType::INVALID, "Table '" + label + "' found in the property graph, but does not have the correct label. Did you mean the label '" + pg_table->main_label + "' instead?");
	    	}

	        // Use int64_t for the distance calculations
	        auto distance_main_label = LevenshteinDistance(label, pg_table->main_label);
	        if (distance_main_label < min_distance) {
	            min_distance = distance_main_label;
	            closest_label = pg_table->main_label;
	        }

	        for (const auto &sub_label : pg_table->sub_labels) {
	            auto distance_sub_label = LevenshteinDistance(label, sub_label);
	            if (distance_sub_label < min_distance) {
	                min_distance = distance_sub_label;
	                closest_label = sub_label;

	            }
	        }
	    }

	    // If a close label match is found, suggest it in the error message
	    if (min_distance < std::numeric_limits<size_t>::max() && error_not_found) {
	        throw Exception(ExceptionType::INVALID,
	                        "Label '" + label + "' not found. Did you mean the " +
	                        (is_vertex_table ? "vertex" : "edge") + " label '" + closest_label + "'?");
	    }

	    // If no match is found and error_not_found is true, throw an error
	    if (error_not_found) {
	        throw Exception(ExceptionType::INVALID,
	                        "Label '" + label + "' not found in the property graph for a " +
	                        (is_vertex_table ? "vertex" : "edge") + " table.");
	    }

	    // Return nullptr if no match is found and error_not_found is false
	    return nullptr;
	}


	//! Serializes a blob into a CreatePropertyGraphInfo
	void Serialize(Serializer &serializer) const override;
	//! Deserializes a blob back into a CreatePropertyGraphInfo
	static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
