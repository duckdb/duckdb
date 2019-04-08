//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/order_index.hpp
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

namespace duckdb {

    class ART : public Index {
    public:
        //! Header of inner nodes
        struct Node {
            //! Length of Compressed Path
            uint32_t prefixLength;
            //! Number of non-null children
            uint16_t count;
            //! Node type (i.e., 4,16,48,256)
            int8_t type;
            //! compressed path (prefix)
            uint8_t prefix[maxPrefixLength];

            Node(int8_t type) : prefixLength(0),count(0),type(type) {}
        };

        struct Node4 : Node {
            uint8_t key[4];
            Node* child[4];

            Node4() : Node(NodeType4) {
                memset(key,0,sizeof(key));
                memset(child,0,sizeof(child));
            }
        };

        struct Node16 : Node {
            uint8_t key[16];
            Node* child[16];

            Node16() : Node(NodeType16) {
                memset(key,0,sizeof(key));
                memset(child,0,sizeof(child));
            }
        };

        struct Node48 : Node {
            uint8_t childIndex[256];
            Node* child[48];

            Node48() : Node(NodeType48) {
                memset(childIndex,48,sizeof(childIndex));
                memset(child,0,sizeof(child));
            }
        };

        struct Node256 : Node {
            Node* child[256];

            Node256() : Node(NodeType256) {
                memset(child,0,sizeof(child));
            }
        };

        ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
                   vector<unique_ptr<Expression>> expressions, size_t initial_capacity,
                   vector<unique_ptr<Expression>> unbinded_expressions);

        //! Appends data into the index, but does not perform the sort yet! This can
        //! be done separately by calling the OrderIndex::Sort() method
        void Insert(DataChunk &data, Vector &row_ids);
        //! Print the index to the console
        void Print();

        void BulkLoad(DataChunk &data, Vector &row_ids);

        //! Initialize a scan on the index with the given expression and column ids
        //! to fetch from the base table for a single predicate
        unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction, vector<column_t> column_ids,
                                                                 Value value, ExpressionType expressionType) override;

        //! Initialize a scan on the index with the given expression and column ids
        //! to fetch from the base table for two predicates
        unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, vector<column_t> column_ids,
                                                               Value low_value, ExpressionType low_expression_type,
                                                               Value high_value,
                                                               ExpressionType high_expression_type) override;
        //! Perform a lookup on the index
        void Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) override;

        // Append entries to the index
        void Append(ClientContext &context, DataChunk &entries, size_t row_identifier_start) override;
        // Update entries in the index
        void Update(ClientContext &context, vector<column_t> &column_ids, DataChunk &update_data,
                    Vector &row_identifiers) override;

        //! Lock used for updating the index
        std::mutex lock;
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
        //! The size of one tuple
        size_t tuple_size;
        //! The actual index
        unique_ptr<uint8_t[]> data;
        //! The amount of entries in the index
        size_t count;
        //! The capacity of the index
        size_t capacity;

    private:
        DataChunk expression_result;

        //! Get the start/end position in the index for a Less Than Equal Operator
        size_t SearchLTE(Value value);
        //! Get the start/end position in the index for a Greater Than Equal Operator
        size_t SearchGTE(Value value);
        //! Get the start/end position in the index for a Less Than Operator
        size_t SearchLT(Value value);
        //! Get the start/end position in the index for a Greater Than Operator
        size_t SearchGT(Value value);
        //! Scan the index starting from the position, updating the position.
        //! Returns the amount of tuples scanned.
        void Scan(size_t &position_from, size_t &position_to, Value value, Vector &result_identifiers);
    };

} // namespace duckdb
