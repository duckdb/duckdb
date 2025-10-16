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
	static constexpr double ALPHA = 0.044;
	static constexpr double BETA = 1.0;
	static constexpr double KAPPA = 0.0;

	double lambda;
	vector<double> wm, wc; // weights for mean and covariance

	// State: [progress (0-1), velocity (progress/second)]
	vector<double> x;         // state estimate
	vector<vector<double>> P; // covariance matrix
	vector<vector<double>> Q; // process noise
	vector<vector<double>> R; // measurement noise

	double last_time;
	bool initialized;
	double last_progress;
	double scale_factor;

	// Helper functions
	vector<vector<double>> MatrixSqrt(const vector<vector<double>> &mat);
	vector<vector<double>> GenerateSigmaPoints();
	vector<double> StateTransition(const vector<double> &state, double dt);
	vector<double> MeasurementFunction(const vector<double> &state);

public:
	UnscentedKalmanFilter();

	void Update(double progress, double time);

	double GetEstimatedRemainingSeconds() const;

private:
	void Initialize(double initial_progress, double current_time);
	void Predict(double current_time);
	void UpdateInternal(double measured_progress);

	double GetProgress() const;
	double GetVelocity() const;
	double GetProgressVariance() const;
	double GetVelocityVariance() const;
};

} // namespace duckdb
