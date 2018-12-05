/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Christopher Chan-Nui
 * - Cecil Reames
 */

#include "utilities/threading.h"
#include "main/unusedflag.h"

#include <stdexcept>
#include <iomanip>

#include "main/bucketsimulator.h"

namespace TPCE {

BucketSimulator::BucketSimulator(int iterstart, int itercount, TIdent iCustomerCount, INT64 simorders,
                                 TPCE::RNGSEED base_seed, BucketProgress &progress)
    : m_rnd(), m_buckets(NULL), m_custcount(iCustomerCount), m_iterstart(iterstart), m_itercount(itercount),
      m_baseseed(base_seed), m_simorders(simorders),
      m_maxbucket(static_cast<int>(iCustomerCount / iDefaultLoadUnitSize)), m_progress(progress) {
	if (iCustomerCount % iDefaultLoadUnitSize != 0) {
		throw std::range_error("The customer count must be an integral "
		                       "multiple of the load unit size!");
	}
	if (m_maxbucket < 2) {
		throw std::range_error("Bucket simulator must have at least 2 buckets!");
	}
	m_buckets = new INT64[m_maxbucket];
}

BucketSimulator::~BucketSimulator() {
	delete[] m_buckets;
}

// Calculate the standard deviation of the current bucket
// Use straightforward method rather than sum of squares...
double BucketSimulator::calc_stddev() {
	double sum = 0;
	for (int i = 0; i < m_maxbucket; ++i) {
		sum += m_buckets[i];
	}
	double mean = sum / (double)m_maxbucket;

	double sumdev2 = 0;
	for (int i = 0; i < m_maxbucket; ++i) {
		sumdev2 += (m_buckets[i] - mean) * (m_buckets[i] - mean);
	}
	double stddev = sqrt(sumdev2 / (double)(m_maxbucket - 1));

	return stddev;
}

// Simulates one run and returns the standard deviation of the buckets
// iterations - Number of "orders" to simulate (note that we don't take into
//              account the 1% rollbacks)
double BucketSimulator::simulate_onerun(INT64 iorders) {
	int bucket;

	memset(m_buckets, 0, sizeof(m_buckets[0]) * m_maxbucket);

	for (INT64 i = 0; i < iorders; ++i) {
		bucket = m_rnd.RndIntRange(0, static_cast<int>((m_custcount - 1) / static_cast<TIdent>(iDefaultLoadUnitSize)));
		m_buckets[bucket] += 1;
	}

	return calc_stddev();
}

// Perform all of the simulations this instance was constructed with and return
// the maximum standard deviation
double BucketSimulator::simulate() {
	double max_stddev = 0;
	TPCE::RNGSEED seed = m_rnd.RndNthElement(m_baseseed, (RNGSEED)(m_iterstart * m_simorders * RND_STEP_PER_ORDER));
	m_rnd.SetSeed(seed);
	for (int i = 0; i < m_itercount; ++i) {
		TPCE::RNGSEED current_seed = m_rnd.GetSeed();
		double stddev = simulate_onerun(m_simorders);

		std::ostringstream strm;
		strm << "StdDev for run " << i + m_iterstart << " is " << std::setprecision(3) << std::fixed << stddev
		     << ", seed was " << current_seed;
		m_progress.message(strm.str(), 1);

		if (max_stddev < stddev) {
			max_stddev = stddev;
		}
		if (!m_progress.report(stddev)) {
			break;
		}
	}
	return max_stddev;
}

// Entry point to be run as a thread
void BucketSimulator::run(void *thread UNUSED) {
	simulate();
}

//////////////////////////////////////////////////////////////////////////////

BucketProgress::BucketProgress(double acceptable_stddev, int total_in, int verbosity, std::ostream *output)
    : ProgressMeter(total_in, verbosity, output), acceptable_stddev_(acceptable_stddev), max_stddev_(0) {
}

void BucketProgress::display_message(std::ostream &out) const {
	ProgressMeter::display_message(out);
	out << " stddev=" << max_stddev_;
}

bool BucketProgress::report(double stddev) {
	inc();
	TPCE::Locker<ProgressMeter> locker(*this);
	if (max_stddev_ < stddev) {
		max_stddev_ = stddev;
	}
	return (max_stddev_ < acceptable_stddev_);
}

double BucketProgress::max_stddev() {
	TPCE::Locker<ProgressMeter> locker(*this);
	return max_stddev_;
}

} // namespace TPCE
