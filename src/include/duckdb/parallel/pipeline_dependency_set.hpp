#pragma once
#include "duckdb/common/mutex.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include <vector>

namespace duckdb {


//! A lightweight holder for grouping producer pipelines that multiple consumers must wait for.
struct PipelineDependencySet {
public:
    explicit PipelineDependencySet() = default;

	//! Register a producer pipeline into this set.
    void AddProducer(shared_ptr<Pipeline> &producer_pipe) {
        lock_guard<mutex> guard(mtx);
        if (!producer_pipe) {
            return;
        }
        producers.push_back(producer_pipe);
    }

	//! Get all registered producer pipelines (read-only).
    const vector<shared_ptr<Pipeline>>& GetProducers() const {
        return producers;
    }

	//!  Wire up ALL producers as dependencies of consumer.
    void MakeDependenciesOf(Pipeline &consumer_pipeline) {
        lock_guard<mutex> guard(mtx);

        for (auto &prod : producers) {
            if (!prod) {
                continue;
            }

            consumer_pipeline.AddDependency(prod);
        }
    }

    idx_t ProducerCount() const {
        lock_guard<mutex> guard(mtx);
        return producers.size();
    }

private:
    mutable mutex mtx;
    vector<shared_ptr<Pipeline>> producers;
};

} // namespace duckdb