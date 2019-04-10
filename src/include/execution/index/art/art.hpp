//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/art.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "common/types/vector.hpp"
#include "parser/parsed_expression.hpp"
#include "storage/data_table.hpp"
#include "storage/index.hpp"
#include "node.hpp"
#include "node4.hpp"
#include "node16.hpp"
#include "node48.hpp"
#include "node256.hpp"

namespace duckdb {
    class ART : public Index {
    public:
        ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
                   vector<unique_ptr<Expression>> expressions, size_t initial_capacity,
                   vector<unique_ptr<Expression>> unbinded_expressions);

        //! Insert data into the index, one element at a time
        void Insert(DataChunk &data, Vector &row_ids);
         //! Print the index to the console
        void Print();

        void BulkLoad(DataChunk &data, Vector &row_ids);

        //! Root of the tree
        Node* tree;
        //! The table
        DataTable &table;
        //! Column identifiers to extract from the base table
        vector<column_t> column_ids;
        //! Types of the column identifiers
        vector<TypeId> types;
        //! The expressions to evaluate
        vector<unique_ptr<Expression>> expressions;
        //! Unbinded expressions to be used in the optimizer
        vector<unique_ptr<Expression>> unbinded_expressions;
        //! The actual index
        unique_ptr<uint8_t[]> data;
    private:
        DataChunk expression_result;


    };

} // namespace duckdb
