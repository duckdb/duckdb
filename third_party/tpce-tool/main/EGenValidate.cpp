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

/*
 *  - 0.3 - Tue Oct  3 23:39:48 CDT 2006
 *       Extracted BucketSimulator class
 *       Extracted string parsing and format functions
 *       Re-vamped to use new threading
 *
 *  - 0.2 - Mon Sep 11 09:06:35 CDT 2006
 *       Incorporated Matthew Emmerton's suggestions:
 *          Fixed calculation of simorders (was off by 60)
 *          Base GlobalLock on CSyncLock
 *
 *  - 0.1 - Fri Sep  8 14:26:00 CDT 2006
 *       Initial release
 */

#include "utilities/threading.h"
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <cmath>
#include <cstdlib>

#include "main/EGenLoader_stdafx.h"
#include "utilities/DateTime.h"
#include "main/progressmeter.h"
#include "main/bucketsimulator.h"
#include "main/strutil.h"

using TPCE::int64totimestr;
using TPCE::strtodbl;
using TPCE::strtoint64;
using TPCE::timestrtoint64;

using TPCE::CCustomerSelection;
using TPCE::CRandom;
using TPCE::eCustomerTier;
using TPCE::iDefaultLoadUnitSize;
using TPCE::RNGSEED;

using std::cout;
using std::endl;

// Helper class to contain the run configuration options
class BucketSimOptions {
public:
	int verbose;
	int helplevel;
	RNGSEED base_seed;
	TIdent cust_count;
	double tpse;
	bool use_stddev;
	double stddev;
	INT64 run_length;
	UINT sim_count;
	UINT sim_first;
	UINT num_threads;
	UINT interval_time;

	// Calculate the number orders for this simulation
	INT64 calc_simorders() {
		return static_cast<INT64>(tpse * static_cast<double>(run_length));
	}

	BucketSimOptions()
	    : verbose(0), helplevel(0), base_seed(TPCE::RNGSeedBaseTxnInputGenerator), cust_count(0), tpse(10),
	      use_stddev(false), stddev(0), run_length(7200), sim_count(10000), sim_first(0), num_threads(1),
	      interval_time(10) {
	}
};

// Usage message
void usage(const char *progname, BucketSimOptions &options, bool usage_error = true) {
	cout << "Usage: " << progname << " [options] stddev custcount [TPS-E] [runtime]" << endl
	     << "    -h          This help message (-hh for more options)" << endl
	     << "    -v          Increase verbosity" << endl
	     << "    -c num      Number of customers to simulate (" << options.cust_count << ")" << endl
	     << "    -r runtime  Runtime to simulate (" << options.run_length << ")" << endl
	     << "    -e tpsE     tpsE to simulate (" << options.tpse << ")" << endl
	     << "    -s stddev   Standard Deviation to check against (none)" << endl
	     << "    -t threads  Number of threads to use (" << options.num_threads << ")" << endl;
	if (options.helplevel > 1) {
		cout << "    -S seed     Base Seed for random number generation (" << options.base_seed << ")" << endl
		     << "    -n num      Number of simulations to perform (" << options.sim_count << ")" << endl
		     << "    -i num      Initial simulation number to start with (" << options.sim_first << ")" << endl
		     << "    -u num      Progress report interval (" << options.interval_time << ")" << endl;
	}
	exit(usage_error ? ERROR_BAD_OPTION : 0);
}

// Parse command line, put discovered values into the options parameter
void parseCommandLine(int argc, const char *argv[], BucketSimOptions &options) {
	char flag = '-';
	try {
		bool tpse_set = false;
		options.tpse = 0;
		int argnum;
		for (argnum = 1; argnum < argc; ++argnum) {
			const char *ptr = argv[argnum];
			if (*ptr++ != '-') {
				break;
			}
			while (*ptr) {
				flag = *ptr++;

				// Prefetch an "argument" for the option in case it is needed
				// later
				bool use_arg = true;
				int arg_advance = 0;
				const char *arg = ptr;
				// If the argument is not bundled with the options' switch we
				// use the next argument.
				if (*ptr == '\0') {
					arg_advance = 1;
					arg = argv[argnum + 1];
					if (arg == NULL) {
						// If there wasn't an argument pretend it was just an
						// empty string
						arg = "";
					}
				}

				switch (flag) {
				case 'v':
					use_arg = false;
					options.verbose += 1;
					break;
				case '?':
				case 'h':
					use_arg = false;
					options.helplevel += 1;
					break;
				case 'S':
					options.base_seed = (RNGSEED)strtoint64(arg);
					break;
				case 'c':
					options.cust_count = strtoint64(arg);
					break;
				case 'n':
					options.sim_count = static_cast<UINT>(strtoint64(arg));
					break;
				case 'i':
					options.sim_first = static_cast<UINT>(strtoint64(arg));
					break;
				case 'r':
					options.run_length = timestrtoint64(arg);
					break;
				case 'e':
					tpse_set = true;
					options.tpse = strtodbl(arg);
					break;
				case 't':
					options.num_threads = static_cast<UINT>(strtoint64(arg));
					break;
				case 's':
					options.use_stddev = true;
					options.stddev = strtodbl(arg);
					break;
				case 'u':
					options.interval_time = static_cast<UINT>(timestrtoint64(arg));
					options.verbose += 1;
					break;
				default:
					std::ostringstream strm;
					throw std::runtime_error(std::string("Unknown option"));
				}

				// If we used an argument we need to consume it
				if (use_arg) {
					argnum += arg_advance;
					break;
				}
			}
		}

		// After all the switches are processed we consume the rest of the
		// command line as positional options
		if (argnum < argc) {
			options.use_stddev = true;
			options.stddev = strtodbl(argv[argnum++]);
		}
		if (argnum < argc) {
			flag = 'c';
			options.cust_count = strtoint64(argv[argnum++]);
		}
		if (argnum < argc) {
			flag = 't';
			tpse_set = true;
			options.tpse = strtodbl(argv[argnum++]);
		}
		if (argnum < argc) {
			flag = 'r';
			options.run_length = static_cast<UINT>(timestrtoint64(argv[argnum++]));
		}
		if (argnum < argc) {
			cout << "Too many command line arguments!" << endl;
			usage(argv[0], options);
		}

		if (!tpse_set) {
			options.tpse = options.cust_count / 500.0; // 500 is scale factor
		}

		// If no customers were specified that probably means the user
		// didn't know how to invoke us.
		if (options.helplevel || options.cust_count == 0 || !options.use_stddev) {
			usage(argv[0], options, false);
		}
	} catch (std::exception &e) {
		cout << "Error parsing command line option '" << flag << "': " << e.what() << endl;
		usage(argv[0], options);
		exit(ERROR_BAD_OPTION);
	}
}

int main(int argc, const char *argv[]) {
	try {
		TPCE::CDateTime start_time;
		BucketSimOptions options;
		std::vector<TPCE::Thread<TPCE::BucketSimulator> *> threads;

		// Output EGen version
		TPCE::PrintEGenVersion();

		parseCommandLine(argc, argv, options);

		TPCE::BucketProgress progress(options.stddev, options.sim_count, options.verbose);
		progress.set_display_interval(options.interval_time);

		// Start up simulation threads
		unsigned int sim_idx = 0;
		unsigned int sims_per_thread = options.sim_count / options.num_threads + 1;

		while (sim_idx < options.sim_count) {
			// Last thread only processes however many are left over
			if (sims_per_thread > options.sim_count - sim_idx) {
				sims_per_thread = options.sim_count - sim_idx;
			}
			TPCE::Thread<TPCE::BucketSimulator> *thr =
			    new TPCE::Thread<TPCE::BucketSimulator>(std::unique_ptr<TPCE::BucketSimulator>(
			        new TPCE::BucketSimulator(options.sim_first + sim_idx, sims_per_thread, options.cust_count,
			                                  options.calc_simorders(), options.base_seed, progress)));
			;
			thr->start();
			threads.push_back(thr);
			sim_idx += sims_per_thread;
		}

		// Wait for all threads and find maximum standard deviation
		while (!threads.empty()) {
			TPCE::Thread<TPCE::BucketSimulator> *thr = threads.back();
			threads.pop_back();
			thr->stop();
			delete thr;
		}

		TPCE::CDateTime now;
		cout << "Maximum Standard Deviation   = " << progress.max_stddev() << endl;
		cout << "Requested Standard Deviation = " << options.stddev << endl;
		cout << "Customer Count               = " << options.cust_count << endl;
		cout << "Iteration Count              = " << options.sim_count << endl;
		cout << "Iteration Start              = " << options.sim_first << endl;
		cout << "Iterations Completed         = " << progress.current() << endl;
		cout << "Base Seed                    = " << options.base_seed << endl;
		cout << "Simulation Duration          = " << options.run_length << endl;
		cout << "tpsE                         = " << options.tpse << endl;
		cout << "Simulation Duration          = " << int64totimestr(now.DiffInMilliSeconds(start_time) / 1000) << endl;
		cout << "Simulation completed at      = " << now.ToStr(11) << endl;
		cout << endl;

		// Test against supplied standard deviation
		if (progress.max_stddev() < options.stddev) {
			cout << "Failed!" << endl;
			exit(4); // Need a constant for this
		} else {
			cout << "Passed!" << endl;
		}

		// Successful run, exit nicely
		return 0;
	} catch (std::exception &e) {
		cout << "Caught exception: " << e.what() << endl;
	} catch (...) {
		cout << "Caught unknown exception" << endl;
	}
	// Failed run make sure we exit with an error
	exit(1);
}
