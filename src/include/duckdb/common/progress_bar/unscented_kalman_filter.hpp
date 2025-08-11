//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar/unscented_kalman_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include <chrono>

namespace duckdb {

class UnscentedKalmanFilter {
private:
	static constexpr size_t STATE_DIM = 2; // [progress, velocity]
	static constexpr size_t OBS_DIM = 1;   // [progress]
	static constexpr size_t SIGMA_POINTS = 2 * STATE_DIM + 1;

	// UKF parameters
	static constexpr double ALPHA = 0.1;
	static constexpr double BETA = 1.0;
	static constexpr double KAPPA = 0.0;

	double lambda;
	std::vector<double> wm, wc; // weights for mean and covariance

	// State: [progress (0-1), velocity (progress/second)]
	std::vector<double> x;              // state estimate
	std::vector<std::vector<double>> P; // covariance matrix
	std::vector<std::vector<double>> Q; // process noise
	std::vector<std::vector<double>> R; // measurement noise

	double last_time;
	bool initialized;

	// Helper functions
	std::vector<std::vector<double>> matrixSqrt(const std::vector<std::vector<double>> &mat);
	std::vector<std::vector<double>> generateSigmaPoints();
	std::vector<double> stateTransition(const std::vector<double> &state, double dt);
	std::vector<double> measurementFunction(const std::vector<double> &state);

public:
	UnscentedKalmanFilter();

	void initialize(double initial_progress, double current_time);
	void predict(double current_time);
	void update(double measured_progress);

	double getProgress() const;
	double getVelocity() const;
	double getEstimatedRemainingSeconds() const;
	double getProgressVariance() const;
	double getVelocityVariance() const;
};

} // namespace duckdb
