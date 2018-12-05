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
 */

#ifndef BUCKETSIM_H_INCLUDED
#define BUCKETSIM_H_INCLUDED

#include "EGenTables_stdafx.h"
#include "CustomerSelection.h"

#include "progressmeter.h"

namespace TPCE {

class CRandom;

class BucketProgress : public ProgressMeter {
private:
	double acceptable_stddev_;
	double max_stddev_;

public:
	BucketProgress(double acceptable_stddev, int total, int verbosity = 0, std::ostream *output = &std::cout);
	virtual void display_message(std::ostream &out) const;
	bool report(double stddev);
	double max_stddev();
};

// Class to simulate running N completed orders and calculate some metrics
// (Standard deviation) to verify that an actual run is "typical".
// Instantiate the class and then call simulate() to perform the actual
// simulation.
class BucketSimulator {
private:
	CRandom m_rnd;
	INT64 *m_buckets;
	TIdent m_custcount;
	int m_iterstart;
	int m_itercount;
	TPCE::RNGSEED m_baseseed;
	TTrade m_simorders;
	int m_maxbucket;
	BucketProgress &m_progress;

	static const UINT RND_STEP_PER_ORDER = 1; // Number of random number generator calls

public:
	// iStartFromCustomer  - Customer number to start with
	// iCustomerCount      - Number of customers to simulate
	// CRandom *rnd        - Pointer to random generator to use
	// UINT uiLoadUnit     - Size of the TPCE "Load Unit"
	BucketSimulator(int iterstart, int itercount, TIdent iCustomerCount, TTrade m_simorders, TPCE::RNGSEED base_seed,
	                BucketProgress &prog);
	~BucketSimulator();

	double calc_stddev();

	// iterations - Number of "orders" to simulate
	// Returns the Standard deviation of the load unit buckets after the
	// simulation
	double simulate_onerun(INT64 iterations);
	double simulate();

	void run(void *thread);
};
} // namespace TPCE

#endif // BUCKETSIM_H_INCLUDED
