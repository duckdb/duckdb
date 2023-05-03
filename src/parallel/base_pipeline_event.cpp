#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

BasePipelineEvent::BasePipelineEvent(shared_ptr<Pipeline> pipeline_p)
    : Event(pipeline_p->executor), pipeline(std::move(pipeline_p)) {
}

BasePipelineEvent::BasePipelineEvent(Pipeline &pipeline_p)
    : Event(pipeline_p.executor), pipeline(pipeline_p.shared_from_this()) {
}

} // namespace duckdb
