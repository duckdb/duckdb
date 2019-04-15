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
    struct ARTIndexScanState : public IndexScanState {
        Value value_left;
        Value value_right;

        ExpressionType expression_type_left;
        ExpressionType expression_type_right;

        ARTIndexScanState(vector<column_t> column_ids) : IndexScanState(column_ids) {
        }
    };

    class ART : public Index {
    public:
        ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
            vector<unique_ptr<Expression>> expressions,vector<unique_ptr<Expression>> unbound_expressions);
        //! Insert data into the index, one element at a time
        void Insert(DataChunk &data, Vector &row_ids);
         //! Print the index to the console
        void Print(){

        };

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
                                                               ExpressionType high_expression_type) override{};
        //! Perform a lookup on the index
        void Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) override;

        //! Append entries to the index
        void Append(ClientContext &context, DataChunk &entries, size_t row_identifier_start) override{};
        //! Update entries in the index
        void Update(ClientContext &context, vector<column_t> &column_ids, DataChunk &update_data,
                    Vector &row_identifiers) override{};

        //! Root of the tree
        Node* tree;
        //! The table
        DataTable &table;
        //! Column identifiers to extract from the base table
        vector<column_t> column_ids;
        //! Types of the column identifiers
        vector<TypeId> types;
        //! True if machine is little endian
        bool is_little_endian;
    private:
        DataChunk expression_result;

        template <class P> void convert_to_big_endian(uintptr_t tid,uint8_t key[]) {
            switch(sizeof(P)){
                case 1:
                        reinterpret_cast<P*>(key)[0]=tid;
                        return;
                case 2:
                        reinterpret_cast<P*>(key)[0]=__builtin_bswap16(tid);
                        return;
                case 4:
                        reinterpret_cast<P*>(key)[0]=__builtin_bswap32(tid);
                        return;
                case 8:
                        reinterpret_cast<P*>(key)[0]=__builtin_bswap64(tid);
                        return;
            }
        }
        //! This sets 1 on the MSB of the corresponding type
        uint8_t flipSign(uint8_t val ) {
                return val^128;
        }

        template <class T,class P> void templated_insert(DataChunk &input, Vector &row_ids) {
                auto input_data = (P *)input.data[0].data;
                auto row_identifiers = (uint64_t *)row_ids.data;
                for (size_t i = 0; i < row_ids.count; i++) {
                        uint8_t minKey[maxPrefixLength];
                        if (is_little_endian){
                                convert_to_big_endian<P>(input_data[i],minKey);
                        }
                        else{
                                reinterpret_cast<P*>(minKey)[0]=input_data[i];
                        }
//                    minKey[0] = flipSign(minKey[0]);
                    Node::insert(tree,&tree,minKey,0,input_data[i],8);
                    Node* leaf= ART::tree->lookup(tree,minKey,8,0,8);
//                    assert (leaf != nullptr);
                }
        }

        //TODO: For now only lookup
        template <class T,class P> size_t templated_lookup(T key, uint64_t *result_ids) {
                uint8_t minKey[maxPrefixLength];
            if (is_little_endian){
                convert_to_big_endian<P>(key,minKey);
            }
            else{
                reinterpret_cast<P*>(minKey)[0]=key;
            }
                minKey[0] = flipSign(minKey[0]);
                Node* leaf= ART::tree->lookup(tree,minKey,8,0,8);
                if(leaf){
                    auto possible_match = Node::getLeafValue(leaf);
//                    assert(possible_match = 5);
                }
                return 1;

        }
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
        void Scan(size_t &position_from, size_t &position_to, Value value, Vector &result_identifiers){};

        };

} // namespace duckdb
