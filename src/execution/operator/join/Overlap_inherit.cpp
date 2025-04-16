#include "duckdb/execution/operator/join/physical_range_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/sort/sorted_block.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

// dummy

namespace duckdb {

    //! PhysicalTimeSeriesOverlapJoin represents a time-series overlap join between two tables
    class PhysicalTimeSeriesOverlapJoin : public PhysicalRangeJoin {
    public:
        static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TIME_SERIES_OVERLAP_JOIN;
        
    public:
        PhysicalTimeSeriesOverlapJoin(LogicalComparisonJoin &op, unique_ptr<PhysicalOperator> left, 
                                     unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, 
                                     JoinType join_type, idx_t estimated_cardinality);
        
        // Time-series specific attributes
        vector<LogicalType> join_key_types;
        vector<BoundOrderByNode> lhs_orders;
        vector<BoundOrderByNode> rhs_orders;
        
    public:
        // Source interface (from IE_Join)
        unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
                                                        GlobalSourceState &gstate) const override;
        unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
        SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, 
                               OperatorSourceInput &input) const override;
        bool IsSource() const override {
            return true;
        }
        bool ParallelSource() const override {
            return true;
        }
        
    public:
        // Sink interface (from IE_Join)
        unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
        unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
        SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, 
                           OperatorSinkInput &input) const override;
        SinkCombineResultType Combine(ExecutionContext &context, 
                                     OperatorSinkCombineInput &input) const override;
        SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                OperatorSinkFinalizeInput &input) const override;
        bool IsSink() const override {
            return true;
        }
        bool ParallelSink() const override {
            return true;
        }
        
    public:
        // Pipeline building (from IE_Join)
        void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
        
    private:
        // Your sweep-line index implementation
        class SweeplineIndex {
            // Implementation as you described earlier
        };
        
        // Custom join resolution method for overlap joins
        void ResolveOverlapJoin(ExecutionContext &context, DataChunk &result, 
                               LocalSourceState &state) const;
    };
    
    } // namespace duckdb