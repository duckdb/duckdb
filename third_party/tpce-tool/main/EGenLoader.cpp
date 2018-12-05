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
 * - Sergey Vasilevskiy, Matt Emmerton, Doug Johnson
 */

//
// Defines the entry point for the console application.
//
#include <string>

#include "main/EGenLoader_stdafx.h"
#include "input/DataFileManager.h"

using namespace TPCE;

// Driver Defaults
TIdent iStartFromCustomer = iDefaultStartFromCustomer;
TIdent iCustomerCount = iDefaultCustomerCount;      // # of customers for this instance
TIdent iTotalCustomerCount = iDefaultCustomerCount; // total number of customers in the database
UINT iLoadUnitSize = iDefaultLoadUnitSize;          // # of customers in one load unit
UINT iScaleFactor = 500;                            // # of customers for 1 tpsE
UINT iDaysOfInitialTrades = 300;

// These flags are used to control which tables get generated and loaded.
bool bTableGenerationFlagNotSpecified = true; // assume no flag is specified.
bool bGenerateFixedTables = false;
bool bGenerateGrowingTables = false;
bool bGenerateScalingTables = false;
bool bGenerateUsingCache = true;

char szInDir[iMaxPath];
#ifdef COMPILE_FLAT_FILE_LOAD
char szOutDir[iMaxPath];
FlatFileOutputModes FlatFileOutputMode;
#endif
#ifdef COMPILE_ODBC_LOAD
char szServer[iMaxHostname];
char szDB[iMaxDBName];
#endif
#if defined(COMPILE_ODBC_LOAD) || defined(COMPILE_CUSTOM_LOAD)
char szLoaderParms[1024];
#endif

#ifndef FLAT_IN_PATH
#define FLAT_IN_PATH "flat_in/"
#endif

#ifndef FLAT_OUT_PATH
#define FLAT_OUT_PATH "flat_out/"
#endif

enum eLoadImplementation {
	NULL_LOAD = 0, // no load - generate data only
	FLAT_FILE_LOAD,
	ODBC_LOAD,
	CUSTOM_LOAD // sponsor-provided load mechanism
};
#if defined(DEFAULT_LOAD_TYPE)
eLoadImplementation LoadType = DEFAULT_LOAD_TYPE;
#elif defined(COMPILE_FLAT_FILE_LOAD)
eLoadImplementation LoadType = FLAT_FILE_LOAD;
#elif defined(COMPILE_ODBC_LOAD)
eLoadImplementation LoadType = ODBC_LOAD;
#elif defined(COMPILE_CUSTOM_LOAD)
eLoadImplementation LoadType = CUSTOM_LOAD;
#else
eLoadImplementation LoadType = NULL_LOAD;
#endif

/*
 * Prints program usage to std error.
 */
void Usage() {
	fprintf(stderr, "Usage:\n");
	fprintf(stderr, "EGenLoader [options] \n\n");
	fprintf(stderr, " Where\n");
	fprintf(stderr, "  Option                       Default     Description\n");
	fprintf(stderr, "   -b number                   %" PRId64 "           Beginning customer ordinal position\n",
	        iStartFromCustomer);
	fprintf(stderr, "   -c number                   %" PRId64 "        Number of customers (for this instance)\n",
	        iCustomerCount);
	fprintf(stderr, "   -t number                   %" PRId64 "        Number of customers (total in the database)\n",
	        iTotalCustomerCount);
	fprintf(stderr,
	        "   -f number                   %d         Scale factor (customers "
	        "per 1 tpsE)\n",
	        iScaleFactor);
	fprintf(stderr,
	        "   -w number                   %d          Number of Workdays "
	        "(8-hour days) of \n",
	        iDaysOfInitialTrades);
	fprintf(stderr, "                                           initial trades "
	                "to populate\n");
	fprintf(stderr, "   -i dir                      %-11s Directory for input files\n", FLAT_IN_PATH);
	fprintf(stderr, "   -l [FLAT|ODBC|CUSTOM|NULL]  FLAT        Type of load\n");
#ifdef COMPILE_FLAT_FILE_LOAD
	fprintf(stderr, "   -m [APPEND|OVERWRITE]       OVERWRITE   Flat File output mode\n");
	fprintf(stderr, "   -o dir                      %-11s Directory for output files\n", FLAT_OUT_PATH);
#endif
#ifdef COMPILE_ODBC_LOAD
	fprintf(stderr, "   -s string                   localhost   Database server\n");
	fprintf(stderr, "   -d string                   tpce        Database name\n");
#endif
#if defined(COMPILE_ODBC_LOAD) || defined(COMPILE_CUSTOM_LOAD)
	fprintf(stderr, "   -p string                               Additional "
	                "parameters to loader\n");
#endif
	fprintf(stderr, "\n");
	fprintf(stderr, "   -x                          -x          Generate all tables\n");
	fprintf(stderr, "   -xf                                     Generate all "
	                "fixed-size tables\n");
	fprintf(stderr, "   -xd                                     Generate all "
	                "scaling and growing tables\n");
	fprintf(stderr, "                                           (equivalent to -xs -xg)\n");
	fprintf(stderr, "   -xs                                     Generate scaling tables\n");
	fprintf(stderr, "                                           (except BROKER)\n");
	fprintf(stderr, "   -xg                                     Generate "
	                "growing tables and BROKER\n");
	fprintf(stderr, "   -g                                      Disable "
	                "caching when generating growing tables\n");
}

void ParseCommandLine(int argc, char *argv[]) {
	int arg;
	char *sp;
	char *vp;

	/*
	 *  Scan the command line arguments
	 */
	for (arg = 1; arg < argc; ++arg) {

		/*
		 *  Look for a switch
		 */
		sp = argv[arg];
		if (*sp == '-') {
			++sp;
		}
		*sp = (char)tolower(*sp);

		/*
		 *  Find the switch's argument.  It is either immediately after the
		 *  switch or in the next argv
		 */
		vp = sp + 1;
		// Allow for switched that don't have any parameters.
		// Need to check that the next argument is in fact a parameter
		// and not the next switch that starts with '-'.
		//
		if ((*vp == 0) && ((arg + 1) < argc) && (argv[arg + 1][0] != '-')) {
			vp = argv[++arg];
		}

		/*
		 *  Parse the switch
		 */
		switch (*sp) {
		case 'b':
			sscanf(vp, "%" PRId64, &iStartFromCustomer);
			if (iStartFromCustomer <= 0) { // set back to default
				// either number parsing was unsuccessful
				// or a bad value was specified
				iStartFromCustomer = iDefaultStartFromCustomer;
			}
			break;
		case 'c':
			sscanf(vp, "%" PRId64, &iCustomerCount);
			break;
		case 't':
			sscanf(vp, "%" PRId64, &iTotalCustomerCount);
			break;
		case 'f':
			iScaleFactor = (UINT)atoi(vp);
			break;
		case 'w':
			iDaysOfInitialTrades = (UINT)atoi(vp);
			break;

		case 'i': // Location of input files.
			strncpy(szInDir, vp, sizeof(szInDir));
			if (('/' != szInDir[strlen(szInDir) - 1]) && ('\\' != szInDir[strlen(szInDir) - 1])) {
				strncat(szInDir, "/", sizeof(szInDir) - strlen(szInDir) - 1);
			}
			break;

		case 'l': // Load type.
#ifdef COMPILE_FLAT_FILE_LOAD
			if (0 == strcmp(vp, "FLAT")) {
				LoadType = FLAT_FILE_LOAD;
				break;
			}
#endif
#ifdef COMPILE_ODBC_LOAD
			if (0 == strcmp(vp, "ODBC")) {
				LoadType = ODBC_LOAD;
				break;
			}
#endif
			if (0 == strcmp(vp, "NULL")) {
				LoadType = NULL_LOAD;
				break;
			}
#ifdef COMPILE_CUSTOM_LOAD
			if (0 == strcmp(vp, "CUSTOM")) {
				LoadType = CUSTOM_LOAD;
				break;
			}
#endif
			Usage();
			exit(ERROR_BAD_OPTION);
			break;

#ifdef COMPILE_FLAT_FILE_LOAD
		case 'm': // Mode for output of flat files.
			if (0 == strcmp(vp, "APPEND")) {
				FlatFileOutputMode = FLAT_FILE_OUTPUT_APPEND;
			} else if (0 == strcmp(vp, "OVERWRITE")) {
				FlatFileOutputMode = FLAT_FILE_OUTPUT_OVERWRITE;
			} else {
				Usage();
				exit(ERROR_BAD_OPTION);
			}
			break;

		case 'o': // Location for output files.
			strncpy(szOutDir, vp, sizeof(szOutDir));
			if (('/' != szOutDir[strlen(szOutDir) - 1]) && ('\\' != szOutDir[strlen(szOutDir) - 1])) {
				strncat(szOutDir, "/", sizeof(szOutDir) - strlen(szOutDir) - 1);
			}
			break;
#endif
#ifdef COMPILE_ODBC_LOAD
		case 's': // Database server name.
			strncpy(szServer, vp, sizeof(szServer));
			break;

		case 'd': // Database name.
			strncpy(szDB, vp, sizeof(szDB));
			break;
#endif
#if defined(COMPILE_ODBC_LOAD) || defined(COMPILE_CUSTOM_LOAD)
		case 'p': // Loader Parameters
			strncpy(szLoaderParms, vp, sizeof(szLoaderParms));
			break;
#endif
		case 'g': // Disable Cache for Growing Tables (CTradeGen)
			bGenerateUsingCache = false;
			break;
		case 'x':                                     // Table Generation
			bTableGenerationFlagNotSpecified = false; // A -x flag has been used

			if (NULL == vp || *vp == '\0') {
				bGenerateFixedTables = true;
				bGenerateGrowingTables = true;
				bGenerateScalingTables = true;
			} else if (0 == strcmp(vp, "f")) {
				bGenerateFixedTables = true;
			} else if (0 == strcmp(vp, "g")) {
				bGenerateGrowingTables = true;
			} else if (0 == strcmp(vp, "s")) {
				bGenerateScalingTables = true;
			} else if (0 == strcmp(vp, "d")) {
				bGenerateGrowingTables = true;
				bGenerateScalingTables = true;
			} else {
				Usage();
				exit(ERROR_BAD_OPTION);
			}
			break;

		default:
			Usage();
			fprintf(stderr, "Error: Unrecognized option: %s\n", sp);
			exit(ERROR_BAD_OPTION);
		}
	}
}

/*
 * This function validates EGenLoader parameters that may have been
 * specified on the command line. It's purpose is to prevent passing of
 * parameter values that would cause the loader to produce incorrect data.
 *
 * Condition: needs to be called after ParseCommandLine.
 *
 * PARAMETERS:
 *       NONE
 *
 * RETURN:
 *       true                    - if all parameters are valid
 *       false                   - if at least one parameter is invalid
 */
bool ValidateParameters() {
	bool bRet = true;

	// Starting customer must be a non-zero integral multiple of load unit size
	// + 1.
	//
	if ((iStartFromCustomer % iLoadUnitSize) != 1) {
		cout << "The specified starting customer (-b " << iStartFromCustomer
		     << ") must be a non-zero integral multiple of the load unit size (" << iLoadUnitSize << ") + 1." << endl;

		bRet = false;
	}

	// Total number of customers in the database cannot be less
	// than the number of customers for this loader instance
	//
	if (iTotalCustomerCount < iStartFromCustomer + iCustomerCount - 1) {
		iTotalCustomerCount = iStartFromCustomer + iCustomerCount - 1;
	}

	// Customer count must be a non-zero integral multiple of load unit size.
	//
	if ((iLoadUnitSize > iCustomerCount) || (0 != iCustomerCount % iLoadUnitSize)) {
		cout << "The specified customer count (-c " << iCustomerCount
		     << ") must be a non-zero integral multiple of the load unit size (" << iLoadUnitSize << ")." << endl;

		bRet = false;
	}

	// Total customer count must be a non-zero integral multiple of load unit
	// size.
	//
	if ((iLoadUnitSize > iTotalCustomerCount) || (0 != iTotalCustomerCount % iLoadUnitSize)) {
		cout << "The total customer count (-t " << iTotalCustomerCount
		     << ") must be a non-zero integral multiple of the load unit size (" << iLoadUnitSize << ")." << endl;

		bRet = false;
	}

	// Completed trades in 8 hours must be a non-zero integral multiple of 100
	// so that exactly 1% extra trade ids can be assigned to simulate aborts.
	//
	if ((INT64)(HoursPerWorkDay * SecondsPerHour * iLoadUnitSize / iScaleFactor) % 100 != 0) {
		cout << "Incompatible value for Scale Factor (-f) specified." << endl;
		cout << HoursPerWorkDay << " * " << SecondsPerHour << " * Load Unit Size (" << iLoadUnitSize
		     << ") / Scale Factor (" << iScaleFactor << ") must be integral multiple of 100." << endl;

		bRet = false;
	}

	if (iDaysOfInitialTrades == 0) {
		cout << "The specified number of 8-Hour Workdays (-w " << (iDaysOfInitialTrades) << ") must be non-zero."
		     << endl;

		bRet = false;
	}

	return bRet;
}

/*
 *   Create loader class factory corresponding to the
 *   selected load type.
 */
CBaseLoaderFactory *CreateLoaderFactory(eLoadImplementation eLoadType) {
	switch (eLoadType) {
	case NULL_LOAD:
		return new CNullLoaderFactory();

#ifdef COMPILE_ODBC_LOAD
	case ODBC_LOAD:
		return new CODBCLoaderFactory(szServer, szDB, szLoaderParms);
#endif // #ifdef COMPILE_ODBC_LOAD

#ifdef COMPILE_FLAT_FILE_LOAD
	case FLAT_FILE_LOAD:
		return new CFlatLoaderFactory(szOutDir, FlatFileOutputMode);
#endif //#ifdef COMPILE_FLAT_FILE_LOAD

#ifdef COMPILE_CUSTOM_LOAD
	case CUSTOM_LOAD:
		return new CCustomLoaderFactory(szLoaderParms);
#endif //#ifdef COMPILE_CUSTOM_FILE_LOAD

	default:
		assert(false); // this should never happen
	}

	return NULL; // should never happen
}

int main(int argc, char *argv[]) {
	CBaseLoaderFactory *pLoaderFactory; // class factory that creates table loaders
	CGenerateAndLoadStandardOutput Output;
	CGenerateAndLoad *pGenerateAndLoad;
	CDateTime StartTime, EndTime, LoadTime; // to time the database load

	// Output EGen version
	PrintEGenVersion();

	// Establish defaults for command line options.
#ifdef COMPILE_ODBC_LOAD
	strncpy(szServer, "localhost", sizeof(szServer));
	strncpy(szDB, "tpce", sizeof(szDB));
#endif
	strncpy(szInDir, FLAT_IN_PATH, sizeof(szInDir));
#ifdef COMPILE_FLAT_FILE_LOAD
	strncpy(szOutDir, FLAT_OUT_PATH, sizeof(szOutDir));
	FlatFileOutputMode = FLAT_FILE_OUTPUT_OVERWRITE;
#endif

	// Get command line options.
	//
	ParseCommandLine(argc, argv);

	// Validate global parameters that may have been modified on
	// the command line.
	//
	if (!ValidateParameters()) {
		return ERROR_INVALID_OPTION_VALUE; // exit from the loader returning a
		                                   // non-zero code
	}

	// Let the user know what settings will be used.
	cout << endl << endl << "Using the following settings." << endl << endl;
	switch (LoadType) {
#ifdef COMPILE_ODBC_LOAD
	case ODBC_LOAD:
		cout << "\tLoad Type:\t\tODBC" << endl;
		cout << "\tServer Name:\t\t" << szServer << endl;
		cout << "\tDatabase:\t\t" << szDB << endl;
		break;
#endif
#ifdef COMPILE_FLAT_FILE_LOAD
	case FLAT_FILE_LOAD:
		cout << "\tLoad Type:\t\tFlat File" << endl;
		switch (FlatFileOutputMode) {
		case FLAT_FILE_OUTPUT_APPEND:
			cout << "\tOutput Mode:\t\tAPPEND" << endl;
			break;
		case FLAT_FILE_OUTPUT_OVERWRITE:
			cout << "\tOutput Mode:\t\tOVERWRITE" << endl;
			break;
		}
		cout << "\tOut Directory:\t\t" << szOutDir << endl;
		break;
#endif
#ifdef COMPILE_CUSTOM_LOAD
	case CUSTOM_LOAD:
		cout << "\tLoad Type:\t\tCustom" << endl;
		cout << "\tLoader Parms:\t\t" << szLoaderParms << endl;
		break;
#endif
	case NULL_LOAD:
	default:
		cout << "\tLoad Type:\t\tNull load (generation only)" << endl;
		break;
	}
	cout << "\tIn Directory:\t\t" << szInDir << endl;
	cout << "\tStart From Customer:\t" << iStartFromCustomer << endl;
	cout << "\tCustomer Count:\t\t" << iCustomerCount << endl;
	cout << "\tTotal customers:\t" << iTotalCustomerCount << endl;
	cout << "\tLoad Unit:\t\t" << iLoadUnitSize << endl;
	cout << "\tScale Factor:\t\t" << iScaleFactor << endl;
	cout << "\tInitial Trade Days:\t" << iDaysOfInitialTrades << endl;
	cout << "\tCaching Enabled:\t" << (bGenerateUsingCache ? "true" : "false") << endl;
	cout << endl << endl;

	// Know the load type => create the loader factory.
	//
	pLoaderFactory = CreateLoaderFactory(LoadType);

	try {
		// Create a unique log name for this loader instance.
		// Simultaneous loader instances are to be run with
		// disjoint customer ranges, so use this fact to
		// construct a unique name.
		//
		char szLogFileName[64];

		snprintf(&szLogFileName[0], sizeof(szLogFileName), "EGenLoaderFrom%" PRId64 "To%" PRId64 ".log",
		         iStartFromCustomer, (iStartFromCustomer + iCustomerCount) - 1);

		// Create log formatter and logger instance
		CLogFormatTab fmt;
		CEGenLogger logger(eDriverEGenLoader, 0, szLogFileName, &fmt);

		// Set up data file manager for lazy load.
		const DataFileManager dfm(std::string(szInDir), iTotalCustomerCount, iTotalCustomerCount);

		// Create the main class instance
		pGenerateAndLoad = new CGenerateAndLoad(dfm, iCustomerCount, iStartFromCustomer, iTotalCustomerCount,
		                                        iLoadUnitSize, iScaleFactor, iDaysOfInitialTrades, pLoaderFactory,
		                                        &logger, &Output, szInDir, bGenerateUsingCache);

		//  The generate and load phase starts here.
		//
		StartTime.Set();

		// Generate static tables if appropriate.
		if ((bTableGenerationFlagNotSpecified && (iStartFromCustomer == iDefaultStartFromCustomer)) ||
		    bGenerateFixedTables) {
			pGenerateAndLoad->GenerateAndLoadFixedTables();
		}

		// Generate dynamic scaling tables if appropriate.
		if (bTableGenerationFlagNotSpecified || bGenerateScalingTables) {
			pGenerateAndLoad->GenerateAndLoadScalingTables();
		}
		// Generate dynamic trade tables if appropriate.
		if (bTableGenerationFlagNotSpecified || bGenerateGrowingTables) {
			pGenerateAndLoad->GenerateAndLoadGrowingTables();
		}

		//  The generate and load phase ends here.
		//
		EndTime.Set();

		LoadTime.Set(0);                                             // clear time
		LoadTime.Add(0, (int)((EndTime - StartTime) * MsPerSecond)); // add ms

		cout << endl << "Generate and load time: " << LoadTime.ToStr(01) << endl << endl;

		delete pGenerateAndLoad; // don't really need to do that, but just for
		                         // good style
		delete pLoaderFactory;   // don't really need to do that, but just for
		                         // good style

	} catch (CBaseErr &err) {
		cout << endl << "Error " << err.ErrorNum() << ": " << err.ErrorText();
		if (err.ErrorLoc()) {
			cout << " at " << err.ErrorLoc() << endl;
		} else {
			cout << endl;
		}
		return 1;
	}
	// operator new will throw std::bad_alloc exception if there is no
	// sufficient memory for the request.
	//
	catch (std::bad_alloc &) {
		cout << endl << endl << "*** Out of memory ***" << endl;
		return 2;
	} catch (std::exception &err) {
		cout << endl << endl << "Caught Exception: " << err.what() << endl;
	} catch (...) {
		cout << endl << endl << "Caught Unknown Exception" << endl;
	}

	return 0;
}
