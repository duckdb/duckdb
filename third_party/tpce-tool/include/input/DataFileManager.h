#ifndef DATA_FILE_MANAGER_H
#define DATA_FILE_MANAGER_H

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
 * - Doug Johnson
 */

#include <istream>
#include <string>

#include "DataFileTypes.h"
#include "StreamSplitter.h"

#include "utilities/EGenStandardTypes.h"
#include "CompanyCompetitorFile.h"
#include "CompanyFile.h"
#include "SecurityFile.h"
#include "TaxRateFile.h"
#include "utilities/MiscConsts.h"

namespace TPCE {
//
// Description:
// A class for managing all data files. There are several important
// characteristics of this class.
//
// - Thread Saftey. This class is currently not thread safe. The onus is on
// the client to make sure this class is used safely in a multi-threaded
// environment.
//
// - Load Type. This class supports both immediate and lazy loading of the data
// files. The type of load can be specified at object creation time.
//
// - Const-ness. Because of the lazy-load, const-correctness is a bit funky. In
// general, logical constness is what is honored. Public interfaces that clearly
// alter things are not considered const but lazy loading of a file on file
// access is considered const even though that lazy load may trigger an
// exception (e.g. if the file doesn't exist).
//
// Exception Safety:
// The Basic guarantee is provided.
//
// Copy Behavior:
// Copying is disallowed.
//
class DataFileManager {
private:
	// Disallow copying.
	DataFileManager(const DataFileManager &);
	DataFileManager &operator=(const DataFileManager &);

	// Customer counts needed for scaling file abstractions.
	TIdent configuredCustomers;
	TIdent activeCustomers;

	// Data Files (mutable to support const-appearing lazy load)
	mutable AreaCodeDataFile_t *areaCodeDataFile;
	mutable ChargeDataFile_t *chargeDataFile;
	mutable CommissionRateDataFile_t *commissionRateDataFile;
	mutable CompanyCompetitorDataFile_t *companyCompetitorDataFile;
	mutable CompanyDataFile_t *companyDataFile;
	mutable CompanySPRateDataFile_t *companySPRateDataFile;
	mutable ExchangeDataFile_t *exchangeDataFile;
	mutable FemaleFirstNameDataFile_t *femaleFirstNameDataFile;
	mutable IndustryDataFile_t *industryDataFile;
	mutable LastNameDataFile_t *lastNameDataFile;
	mutable MaleFirstNameDataFile_t *maleFirstNameDataFile;
	mutable NewsDataFile_t *newsDataFile;
	mutable NonTaxableAccountNameDataFile_t *nonTaxableAccountNameDataFile;
	mutable SectorDataFile_t *sectorDataFile;
	mutable SecurityDataFile_t *securityDataFile;
	mutable StatusTypeDataFile_t *statusTypeDataFile;
	mutable StreetNameDataFile_t *streetNameDataFile;
	mutable StreetSuffixDataFile_t *streetSuffixDataFile;
	mutable TaxableAccountNameDataFile_t *taxableAccountNameDataFile;
	mutable TaxRateCountryDataFile_t *taxRateCountryDataFile;
	mutable TaxRateDivisionDataFile_t *taxRateDivisionDataFile;
	mutable TradeTypeDataFile_t *tradeTypeDataFile;
	mutable ZipCodeDataFile_t *zipCodeDataFile;

	// Scaling Data File Abstractions (mutable to support const-appearing lazy
	// load)
	mutable CCompanyCompetitorFile *companyCompetitorFile;
	mutable CCompanyFile *companyFile;
	mutable CSecurityFile *securityFile;
	mutable CTaxRateFile *taxRateFile;

	// Template for loading any file.
	// Logically const for lazy load.
	// If the file has already been loaded then no action is taken.
	template <class SourceFileT, class SplitterT, class DataFileT>
	void loadFile(SourceFileT sourceFile, DataFileT **dataFile) const {
		// Only load if it hasn't already been loaded.
		if (!(*dataFile)) {
			SplitterT splitter(sourceFile);
			*dataFile = new DataFileT(splitter);
		}
	}

	// Helper method for lazy loading (hence logically const) by file type.
	void loadFile(DataFileType fileType) const;

	// Centralized clean up of any allocated resources.
	void CleanUp();

public:
	// Constructor - default to lazy load from the current directory
	//               for the default number of customers.
	explicit DataFileManager(TIdent configuredCustomerCount = iDefaultCustomerCount,
	                         TIdent activeCustomerCount = iDefaultCustomerCount);

	~DataFileManager();

	// Load a file using an istream.
	void loadFile(std::istream &file, DataFileType fileType);

	// Load a file using a file type.
	void loadFile(DataFileType fileType);

	// Accessors for files.
	const AreaCodeDataFile_t &AreaCodeDataFile() const;
	const ChargeDataFile_t &ChargeDataFile() const;
	const CommissionRateDataFile_t &CommissionRateDataFile() const;
	const CompanyCompetitorDataFile_t &CompanyCompetitorDataFile() const;
	const CompanyDataFile_t &CompanyDataFile() const;
	const CompanySPRateDataFile_t &CompanySPRateDataFile() const;
	const ExchangeDataFile_t &ExchangeDataFile() const;
	const FemaleFirstNameDataFile_t &FemaleFirstNameDataFile() const;
	const IndustryDataFile_t &IndustryDataFile() const;
	const LastNameDataFile_t &LastNameDataFile() const;
	const MaleFirstNameDataFile_t &MaleFirstNameDataFile() const;
	const NewsDataFile_t &NewsDataFile() const;
	const NonTaxableAccountNameDataFile_t &NonTaxableAccountNameDataFile() const;
	const SectorDataFile_t &SectorDataFile() const;
	const SecurityDataFile_t &SecurityDataFile() const;
	const StatusTypeDataFile_t &StatusTypeDataFile() const;
	const StreetNameDataFile_t &StreetNameDataFile() const;
	const StreetSuffixDataFile_t &StreetSuffixDataFile() const;
	const TaxableAccountNameDataFile_t &TaxableAccountNameDataFile() const;
	const TaxRateCountryDataFile_t &TaxRateCountryDataFile() const;
	const TaxRateDivisionDataFile_t &TaxRateDivisionDataFile() const;
	const TradeTypeDataFile_t &TradeTypeDataFile() const;
	const ZipCodeDataFile_t &ZipCodeDataFile() const;

	// Data file abstractions.
	const CCompanyCompetitorFile &CompanyCompetitorFile() const;
	const CCompanyFile &CompanyFile() const;
	const CSecurityFile &SecurityFile() const;
	const CTaxRateFile &TaxRateFile() const;
};

} // namespace TPCE
#endif // DATA_FILE_MANAGER_H
