#include "duckdb/common/progress_bar/unscented_kalman_filter.hpp"
#include "duckdb.hpp"
namespace duckdb {

UnscentedKalmanFilter::UnscentedKalmanFilter()
    : x(STATE_DIM, 0.0), P(STATE_DIM, vector<double>(STATE_DIM, 0.0)), Q(STATE_DIM, vector<double>(STATE_DIM, 0.0)),
      R(OBS_DIM, vector<double>(OBS_DIM, 0.0)), last_time(0.0), initialized(false), last_progress(-1.0),
      scale_factor(1.0) {
	// Calculate UKF parameters
	lambda = ALPHA * ALPHA * (STATE_DIM + KAPPA) - STATE_DIM;

	// Initialize weights
	wm.resize(SIGMA_POINTS);
	wc.resize(SIGMA_POINTS);

	wm[0] = lambda / (STATE_DIM + lambda);
	wc[0] = lambda / (STATE_DIM + lambda) + (1 - ALPHA * ALPHA + BETA);

	for (size_t i = 1; i < SIGMA_POINTS; i++) {
		wm[i] = wc[i] = 0.5 / (STATE_DIM + lambda);
	}

	// Initialize covariance matrices
	P[0][0] = 0.165;  // progress variance
	P[1][1] = 0.0098; // velocity variance

	Q[0][0] = 0.01; // process noise for progress
	Q[1][1] = 0.0;  // process noise for velocity

	R[0][0] = 0.000077; // measurement noise for progress
}

void UnscentedKalmanFilter::Update(double progress, double time) {
	progress *= scale_factor;
	if (!initialized) {
		Initialize(progress, time);
		return;
	}
	Predict(time);
	if (last_progress != progress) {
		UpdateInternal(progress);
		last_progress = progress;
	}
}

void UnscentedKalmanFilter::Initialize(double initial_progress, double current_time) {
	// If the initial progress is zero we can't yet initalize, since the filter
	// wont' have a reasonable guess about query velocity, just wait until some
	// progress has been made.
	if (initial_progress == 0.0 || current_time == 0.0) {
		return;
	}
	// If the initial progress value is very small, it needs to be scaled up so
	// its the same relative magnitude of updates that was used to determine
	// the P, Q, and R parameters of the Kalman filter.
	//
	// The settings for the Kalman filter make assumptions around the magnitude of
	// measurement noise.  If the progress updates are very small, the updates
	// could be considered noise by the Kalman filter.
	scale_factor = std::max(1.0, 0.1 / initial_progress);
	initial_progress *= scale_factor;
	x[0] = initial_progress;
	x[1] = initial_progress / current_time; // initial velocity guess
	last_time = current_time;
	last_progress = initial_progress;
	initialized = true;
}

// Matrix operations
vector<vector<double>> UnscentedKalmanFilter::MatrixSqrt(const vector<vector<double>> &mat) {
	size_t n = mat.size();
	vector<vector<double>> L(n, vector<double>(n, 0.0));

	// Cholesky decomposition
	for (size_t i = 0; i < n; i++) {
		for (size_t j = 0; j <= i; j++) {
			if (i == j) {
				double sum = 0;
				for (size_t k = 0; k < j; k++) {
					sum += L[j][k] * L[j][k];
				}
				L[j][j] = std::sqrt(mat[j][j] - sum);
			} else {
				double sum = 0;
				for (size_t k = 0; k < j; k++) {
					sum += L[i][k] * L[j][k];
				}
				L[i][j] = (mat[i][j] - sum) / L[j][j];
			}
		}
	}
	return L;
}

// Generate sigma points
vector<vector<double>> UnscentedKalmanFilter::GenerateSigmaPoints() {
	vector<vector<double>> sigma_points(SIGMA_POINTS, vector<double>(STATE_DIM));

	// Calculate sqrt((n + lambda) * P)
	auto scaled_P = P;
	double scale = STATE_DIM + lambda;
	for (size_t i = 0; i < STATE_DIM; i++) {
		for (size_t j = 0; j < STATE_DIM; j++) {
			scaled_P[i][j] *= scale;
		}
	}

	auto sqrt_P = MatrixSqrt(scaled_P);

	// First sigma point is the mean
	sigma_points[0] = x;

	// Remaining sigma points
	for (size_t i = 0; i < STATE_DIM; i++) {
		for (size_t j = 0; j < STATE_DIM; j++) {
			sigma_points[i + 1][j] = x[j] + sqrt_P[j][i];
			sigma_points[i + 1 + STATE_DIM][j] = x[j] - sqrt_P[j][i];
		}
	}

	return sigma_points;
}

// State transition function
vector<double> UnscentedKalmanFilter::StateTransition(const vector<double> &state, double dt) {
	vector<double> new_state(STATE_DIM);
	new_state[0] = state[0] + state[1] * dt; // progress += velocity * dt
	new_state[1] = state[1];                 // velocity remains constant

	// Clamp progress to [0, 1]
	new_state[0] = std::max(0.0, std::min(1.0, new_state[0]));

	return new_state;
}

// Measurement function (identity for progress)
vector<double> UnscentedKalmanFilter::MeasurementFunction(const vector<double> &state) {
	return {state[0]};
}

void UnscentedKalmanFilter::Predict(double current_time) {
	D_ASSERT(initialized);

	double dt = current_time - last_time;
	last_time = current_time;

	if (dt <= 0) {
		return;
	}

	// Generate sigma points
	auto sigma_points = GenerateSigmaPoints();

	// Propagate sigma points through state transition
	vector<vector<double>> sigma_points_pred(SIGMA_POINTS, vector<double>(STATE_DIM));
	for (size_t i = 0; i < SIGMA_POINTS; i++) {
		sigma_points_pred[i] = StateTransition(sigma_points[i], dt);
	}

	// Predict state mean
	std::fill(x.begin(), x.end(), 0.0);
	for (size_t i = 0; i < SIGMA_POINTS; i++) {
		for (size_t j = 0; j < STATE_DIM; j++) {
			x[j] += wm[i] * sigma_points_pred[i][j];
		}
	}

	// Predict covariance
	for (size_t i = 0; i < STATE_DIM; i++) {
		std::fill(P[i].begin(), P[i].end(), 0.0);
	}

	for (size_t i = 0; i < SIGMA_POINTS; i++) {
		for (size_t j = 0; j < STATE_DIM; j++) {
			for (size_t k = 0; k < STATE_DIM; k++) {
				P[j][k] += wc[i] * (sigma_points_pred[i][j] - x[j]) * (sigma_points_pred[i][k] - x[k]);
			}
		}
	}

	// Add process noise
	for (size_t i = 0; i < STATE_DIM; i++) {
		for (size_t j = 0; j < STATE_DIM; j++) {
			P[i][j] += Q[i][j];
		}
	}
}

void UnscentedKalmanFilter::UpdateInternal(double measured_progress) {
	D_ASSERT(initialized);
	// Generate sigma points
	auto sigma_points = GenerateSigmaPoints();

	// Transform sigma points through measurement function
	vector<vector<double>> Z_sigma(SIGMA_POINTS, vector<double>(OBS_DIM));
	for (size_t i = 0; i < SIGMA_POINTS; i++) {
		Z_sigma[i] = MeasurementFunction(sigma_points[i]);
	}

	// Predict measurement mean
	vector<double> z_pred(OBS_DIM, 0.0);
	for (size_t i = 0; i < SIGMA_POINTS; i++) {
		for (size_t j = 0; j < OBS_DIM; j++) {
			z_pred[j] += wm[i] * Z_sigma[i][j];
		}
	}

	// Innovation covariance
	vector<vector<double>> S(OBS_DIM, vector<double>(OBS_DIM, 0.0));
	for (size_t i = 0; i < SIGMA_POINTS; i++) {
		for (size_t j = 0; j < OBS_DIM; j++) {
			for (size_t k = 0; k < OBS_DIM; k++) {
				S[j][k] += wc[i] * (Z_sigma[i][j] - z_pred[j]) * (Z_sigma[i][k] - z_pred[k]);
			}
		}
	}

	// Add measurement noise
	for (size_t i = 0; i < OBS_DIM; i++) {
		for (size_t j = 0; j < OBS_DIM; j++) {
			S[i][j] += R[i][j];
		}
	}

	// Cross correlation
	vector<vector<double>> T(STATE_DIM, vector<double>(OBS_DIM, 0.0));
	for (size_t i = 0; i < SIGMA_POINTS; i++) {
		for (size_t j = 0; j < STATE_DIM; j++) {
			for (size_t k = 0; k < OBS_DIM; k++) {
				T[j][k] += wc[i] * (sigma_points[i][j] - x[j]) * (Z_sigma[i][k] - z_pred[k]);
			}
		}
	}

	// Kalman gain (simplified for 1D measurement)
	vector<vector<double>> K(STATE_DIM, vector<double>(OBS_DIM));
	double S_inv = 1.0 / S[0][0];
	for (size_t i = 0; i < STATE_DIM; i++) {
		K[i][0] = T[i][0] * S_inv;
	}

	// Update state
	vector<double> innovation = {measured_progress - z_pred[0]};
	for (size_t i = 0; i < STATE_DIM; i++) {
		x[i] += K[i][0] * innovation[0];
	}

	// Update covariance
	for (size_t i = 0; i < STATE_DIM; i++) {
		for (size_t j = 0; j < STATE_DIM; j++) {
			P[i][j] -= K[i][0] * S[0][0] * K[j][0];
		}
	}

	// Ensure progress stays in bounds
	x[0] = std::max(0.0, std::min(scale_factor, x[0]));
}

double UnscentedKalmanFilter::GetProgress() const {
	return x[0] / scale_factor;
}

double UnscentedKalmanFilter::GetVelocity() const {
	return x[1];
}

double UnscentedKalmanFilter::GetEstimatedRemainingSeconds() const {
	if (!initialized) {
		return 2147483647.0;
	}
	if (x[1] <= 0) {
		// velocity is negative or zero - we estimate this will never finish
		return NumericLimits<double>::Maximum();
	}
	double remaining_progress = (1.0 * scale_factor) - x[0];
	return std::max(remaining_progress / x[1], 0.0);
}

double UnscentedKalmanFilter::GetProgressVariance() const {
	return P[0][0];
}

double UnscentedKalmanFilter::GetVelocityVariance() const {
	return P[1][1];
}

} // namespace duckdb
