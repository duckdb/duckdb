#include "duckdb/parallel/semaphore.hpp"

#include "concurrentqueue.h"
#include "lightweightsemaphore.h"
#include "duckdb/common/helper.hpp"

namespace duckdb {

#ifndef DUCKDB_NO_THREADS

semaphore::semaphore() : sem(make_unsafe_uniq<duckdb_moodycamel::LightweightSemaphore>()) {
}

bool semaphore::wait() {
	return sem->wait();
}

bool semaphore::wait(int64_t timeout_usecs) {
	return sem->wait(timeout_usecs);
}

void semaphore::signal(ssize_t count) {
	sem->signal(count);
}

#else

semaphore::semaphore() : sem(nullptr) {
}
bool semaphore::wait() {
	return true;
}
bool semaphore::wait(int64_t timeout_usecs) {
	return true;
}
void semaphore::signal(ssize_t count = 1) {
}

#endif

} // namespace duckdb
