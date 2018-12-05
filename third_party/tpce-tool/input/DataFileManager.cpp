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

#include "input/DataFileManager.h"
#include "input/tpce_flat_input.hpp"

#include <assert.h>
#include <stdexcept>

//#include "utilities/MiscConsts.h"
#include "input/TextFileSplitter.h"

using namespace TPCE;

// Centralized clean up of any allocated resources.
void DataFileManager::CleanUp() {
	// Clean up any/all data files.
	if (areaCodeDataFile) {
		delete areaCodeDataFile;
	}
	if (chargeDataFile) {
		delete chargeDataFile;
	}
	if (commissionRateDataFile) {
		delete commissionRateDataFile;
	}
	if (companyCompetitorDataFile) {
		delete companyCompetitorDataFile;
	}
	if (companyDataFile) {
		delete companyDataFile;
	}
	if (companySPRateDataFile) {
		delete companySPRateDataFile;
	}
	if (exchangeDataFile) {
		delete exchangeDataFile;
	}
	if (femaleFirstNameDataFile) {
		delete femaleFirstNameDataFile;
	}
	if (industryDataFile) {
		delete industryDataFile;
	}
	if (lastNameDataFile) {
		delete lastNameDataFile;
	}
	if (maleFirstNameDataFile) {
		delete maleFirstNameDataFile;
	}
	if (newsDataFile) {
		delete newsDataFile;
	}
	if (nonTaxableAccountNameDataFile) {
		delete nonTaxableAccountNameDataFile;
	}
	if (sectorDataFile) {
		delete sectorDataFile;
	}
	if (securityDataFile) {
		delete securityDataFile;
	}
	if (statusTypeDataFile) {
		delete statusTypeDataFile;
	}
	if (streetNameDataFile) {
		delete streetNameDataFile;
	}
	if (streetSuffixDataFile) {
		delete streetSuffixDataFile;
	}
	if (taxableAccountNameDataFile) {
		delete taxableAccountNameDataFile;
	}
	if (taxRateCountryDataFile) {
		delete taxRateCountryDataFile;
	}
	if (taxRateDivisionDataFile) {
		delete taxRateDivisionDataFile;
	}
	if (tradeTypeDataFile) {
		delete tradeTypeDataFile;
	}
	if (zipCodeDataFile) {
		delete zipCodeDataFile;
	}

	// Clean up any/all file abstractions.
	if (companyCompetitorFile) {
		delete companyCompetitorFile;
	}
	if (companyFile) {
		delete companyFile;
	}
	if (securityFile) {
		delete securityFile;
	}
	if (taxRateFile) {
		delete taxRateFile;
	}
}

DataFileManager::DataFileManager(TIdent configuredCustomerCount, TIdent activeCustomerCount)
    : configuredCustomers(configuredCustomerCount), activeCustomers(activeCustomerCount), areaCodeDataFile(0),
      chargeDataFile(0), commissionRateDataFile(0), companyCompetitorDataFile(0), companyDataFile(0),
      companySPRateDataFile(0), exchangeDataFile(0), femaleFirstNameDataFile(0), industryDataFile(0),
      lastNameDataFile(0), maleFirstNameDataFile(0), newsDataFile(0), nonTaxableAccountNameDataFile(0),
      sectorDataFile(0), securityDataFile(0), statusTypeDataFile(0), streetNameDataFile(0), streetSuffixDataFile(0),
      taxableAccountNameDataFile(0), taxRateCountryDataFile(0), taxRateDivisionDataFile(0), tradeTypeDataFile(0),
      zipCodeDataFile(0), companyCompetitorFile(0), companyFile(0), securityFile(0), taxRateFile(0) {

	// WARNING: This code is "brittle" since it is highly dependant on
	// the enum definition.
	for (int fileType = AREA_CODE_DATA_FILE; fileType <= ZIPCODE_DATA_FILE; ++fileType) {
		loadFile((DataFileType)fileType);
	}
}

DataFileManager::~DataFileManager() {
	CleanUp();
}

// Load a file using an istream.
void DataFileManager::loadFile(std::istream &source, DataFileType fileType) {
	switch (fileType) {
	case AREA_CODE_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, AreaCodeDataFile_t>(source, &areaCodeDataFile);
		break;
	case CHARGE_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, ChargeDataFile_t>(source, &chargeDataFile);
		break;
	case COMMISSION_RATE_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, CommissionRateDataFile_t>(source, &commissionRateDataFile);
		break;
	case COMPANY_COMPETITOR_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, CompanyCompetitorDataFile_t>(source, &companyCompetitorDataFile);
		break;
	case COMPANY_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, CompanyDataFile_t>(source, &companyDataFile);
		break;
	case COMPANY_SP_RATE_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, CompanySPRateDataFile_t>(source, &companySPRateDataFile);
		break;
	case EXCHANGE_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, ExchangeDataFile_t>(source, &exchangeDataFile);
		break;
	case FEMALE_FIRST_NAME_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, FemaleFirstNameDataFile_t>(source, &femaleFirstNameDataFile);
		break;
	case INDUSTRY_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, IndustryDataFile_t>(source, &industryDataFile);
		break;
	case LAST_NAME_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, LastNameDataFile_t>(source, &lastNameDataFile);
		break;
	case MALE_FIRST_NAME_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, MaleFirstNameDataFile_t>(source, &maleFirstNameDataFile);
		break;
	case NEWS_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, NewsDataFile_t>(source, &newsDataFile);
		break;
	case NON_TAXABLE_ACCOUNT_NAME_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, NonTaxableAccountNameDataFile_t>(source,
		                                                                          &nonTaxableAccountNameDataFile);
		break;
	case SECTOR_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, SectorDataFile_t>(source, &sectorDataFile);
		break;
	case SECURITY_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, SecurityDataFile_t>(source, &securityDataFile);
		break;
	case STATUS_TYPE_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, StatusTypeDataFile_t>(source, &statusTypeDataFile);
		break;
	case STREET_NAME_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, StreetNameDataFile_t>(source, &streetNameDataFile);
		break;
	case STREET_SUFFIX_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, StreetSuffixDataFile_t>(source, &streetSuffixDataFile);
		break;
	case TAXABLE_ACCOUNT_NAME_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, TaxableAccountNameDataFile_t>(source, &taxableAccountNameDataFile);
		break;
	case TAX_RATE_COUNTRY_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, TaxRateCountryDataFile_t>(source, &taxRateCountryDataFile);
		break;
	case TAX_RATE_DIVISION_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, TaxRateDivisionDataFile_t>(source, &taxRateDivisionDataFile);
		break;
	case TRADE_TYPE_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, TradeTypeDataFile_t>(source, &tradeTypeDataFile);
		break;
	case ZIPCODE_DATA_FILE:
		loadFile<std::istream &, StreamSplitter, ZipCodeDataFile_t>(source, &zipCodeDataFile);
		break;
	default:
		// Should never get here.
		throw std::logic_error("Attempt to load by istream an unrecognized data file type.");
	}
}

// Load a file using a file type.
void DataFileManager::loadFile(DataFileType fileType) {
	// Under the covers, call the pseudo-const overload.
	const_cast<const DataFileManager *>(this)->loadFile(fileType);
}

// Helper method for lazy loading (hence logically const) by file type.
void DataFileManager::loadFile(DataFileType fileType) const {
	// Set up the appropriate file name.
	switch (fileType) {
	case AREA_CODE_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, AreaCodeDataFile_t>(AreaCodeConstantString, &areaCodeDataFile);
		break;
	case CHARGE_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, ChargeDataFile_t>(ChargeConstantString, &chargeDataFile);
		break;
	case COMMISSION_RATE_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, CommissionRateDataFile_t>(CommissionRateConstantString,
		                                                                    &commissionRateDataFile);
		break;
	case COMPANY_COMPETITOR_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, CompanyCompetitorDataFile_t>(CompanyCompetitorConstantString,
		                                                                       &companyCompetitorDataFile);
		break;
	case COMPANY_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, CompanyDataFile_t>(CompanyConstantString, &companyDataFile);
		break;
	case COMPANY_SP_RATE_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, CompanySPRateDataFile_t>(CompanySPRateConstantString,
		                                                                   &companySPRateDataFile);
		break;
	case EXCHANGE_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, ExchangeDataFile_t>(ExchangeConstantString, &exchangeDataFile);
		break;
	case FEMALE_FIRST_NAME_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, FemaleFirstNameDataFile_t>(FemaleFirstNameConstantString,
		                                                                     &femaleFirstNameDataFile);
		break;
	case INDUSTRY_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, IndustryDataFile_t>(IndustryConstantString, &industryDataFile);
		break;
	case LAST_NAME_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, LastNameDataFile_t>(LastNameConstantString, &lastNameDataFile);
		break;
	case MALE_FIRST_NAME_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, MaleFirstNameDataFile_t>(MaleFirstNameConstantString,
		                                                                   &maleFirstNameDataFile);
		break;
	case NEWS_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, NewsDataFile_t>(LastNameConstantString, &newsDataFile);
		break;
	case NON_TAXABLE_ACCOUNT_NAME_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, NonTaxableAccountNameDataFile_t>(NonTaxableAccountNameConstantString,
		                                                                           &nonTaxableAccountNameDataFile);
		break;
	case SECTOR_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, SectorDataFile_t>(SectorConstantString, &sectorDataFile);
		break;
	case SECURITY_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, SecurityDataFile_t>(SecurityConstantString, &securityDataFile);
		break;
	case STATUS_TYPE_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, StatusTypeDataFile_t>(StatusTypeConstantString, &statusTypeDataFile);
		break;
	case STREET_NAME_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, StreetNameDataFile_t>(StreetNameConstantString, &streetNameDataFile);
		break;
	case STREET_SUFFIX_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, StreetSuffixDataFile_t>(StreetSuffixConstantString,
		                                                                  &streetSuffixDataFile);
		break;
	case TAXABLE_ACCOUNT_NAME_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, TaxableAccountNameDataFile_t>(TaxableAccountNameConstantString,
		                                                                        &taxableAccountNameDataFile);
		break;
	case TAX_RATE_COUNTRY_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, TaxRateCountryDataFile_t>(TaxRatesCountryConstantString,
		                                                                    &taxRateCountryDataFile);
		break;
	case TAX_RATE_DIVISION_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, TaxRateDivisionDataFile_t>(TaxRatesDivisionConstantString,
		                                                                     &taxRateDivisionDataFile);
		break;
	case TRADE_TYPE_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, TradeTypeDataFile_t>(TradeTypeConstantString, &tradeTypeDataFile);
		break;
	case ZIPCODE_DATA_FILE:
		loadFile<std::string &, TextFileSplitter, ZipCodeDataFile_t>(ZipCodeConstantString, &zipCodeDataFile);
		break;
	default:
		// Should never get here.
		assert(!"Attempt to load an unrecognized data file type.");
	}
}

// Accessors for files.
const AreaCodeDataFile_t &DataFileManager::AreaCodeDataFile() const {
	if (!areaCodeDataFile) {
		// Need to load the file.
		loadFile(AREA_CODE_DATA_FILE);
	}
	return *areaCodeDataFile;
}

const ChargeDataFile_t &DataFileManager::ChargeDataFile() const {
	if (!chargeDataFile) {
		// Need to load the file.
		loadFile(CHARGE_DATA_FILE);
	}
	return *chargeDataFile;
}

const CommissionRateDataFile_t &DataFileManager::CommissionRateDataFile() const {
	if (!commissionRateDataFile) {
		// Need to load the file.
		loadFile(COMMISSION_RATE_DATA_FILE);
	}
	return *commissionRateDataFile;
}

const CompanyCompetitorDataFile_t &DataFileManager::CompanyCompetitorDataFile() const {
	if (!companyCompetitorDataFile) {
		// Need to load the file.
		loadFile(COMPANY_COMPETITOR_DATA_FILE);
	}
	return *companyCompetitorDataFile;
}

const CompanyDataFile_t &DataFileManager::CompanyDataFile() const {
	if (!companyDataFile) {
		// Need to load the file.
		loadFile(COMPANY_DATA_FILE);
	}
	return *companyDataFile;
}

const CompanySPRateDataFile_t &DataFileManager::CompanySPRateDataFile() const {
	if (!companySPRateDataFile) {
		// Need to load the file.
		loadFile(COMPANY_SP_RATE_DATA_FILE);
	}
	return *companySPRateDataFile;
}

const ExchangeDataFile_t &DataFileManager::ExchangeDataFile() const {
	if (!exchangeDataFile) {
		// Need to load the file.
		loadFile(EXCHANGE_DATA_FILE);
	}
	return *exchangeDataFile;
}

const FemaleFirstNameDataFile_t &DataFileManager::FemaleFirstNameDataFile() const {
	if (!femaleFirstNameDataFile) {
		// Need to load the file.
		loadFile(FEMALE_FIRST_NAME_DATA_FILE);
	}
	return *femaleFirstNameDataFile;
}

const IndustryDataFile_t &DataFileManager::IndustryDataFile() const {
	if (!industryDataFile) {
		// Need to load the file.
		loadFile(INDUSTRY_DATA_FILE);
	}
	return *industryDataFile;
}

const LastNameDataFile_t &DataFileManager::LastNameDataFile() const {
	if (!lastNameDataFile) {
		// Need to load the file.
		loadFile(LAST_NAME_DATA_FILE);
	}
	return *lastNameDataFile;
}

const MaleFirstNameDataFile_t &DataFileManager::MaleFirstNameDataFile() const {
	if (!maleFirstNameDataFile) {
		// Need to load the file.
		loadFile(MALE_FIRST_NAME_DATA_FILE);
	}
	return *maleFirstNameDataFile;
}

const NewsDataFile_t &DataFileManager::NewsDataFile() const {
	if (!newsDataFile) {
		// Need to load the file.
		loadFile(NEWS_DATA_FILE);
	}
	return *newsDataFile;
}

const NonTaxableAccountNameDataFile_t &DataFileManager::NonTaxableAccountNameDataFile() const {
	if (!nonTaxableAccountNameDataFile) {
		// Need to load the file.
		loadFile(NON_TAXABLE_ACCOUNT_NAME_DATA_FILE);
	}
	return *nonTaxableAccountNameDataFile;
}

const SectorDataFile_t &DataFileManager::SectorDataFile() const {
	if (!sectorDataFile) {
		// Need to load the file.
		loadFile(SECTOR_DATA_FILE);
	}
	return *sectorDataFile;
}

const SecurityDataFile_t &DataFileManager::SecurityDataFile() const {
	if (!securityDataFile) {
		// Need to load the file.
		loadFile(SECURITY_DATA_FILE);
	}
	return *securityDataFile;
}

const StatusTypeDataFile_t &DataFileManager::StatusTypeDataFile() const {
	if (!statusTypeDataFile) {
		// Need to load the file.
		loadFile(STATUS_TYPE_DATA_FILE);
	}
	return *statusTypeDataFile;
}

const StreetNameDataFile_t &DataFileManager::StreetNameDataFile() const {
	if (!streetNameDataFile) {
		// Need to load the file.
		loadFile(STREET_NAME_DATA_FILE);
	}
	return *streetNameDataFile;
}

const StreetSuffixDataFile_t &DataFileManager::StreetSuffixDataFile() const {
	if (!streetSuffixDataFile) {
		// Need to load the file.
		loadFile(STREET_SUFFIX_DATA_FILE);
	}
	return *streetSuffixDataFile;
}

const TaxableAccountNameDataFile_t &DataFileManager::TaxableAccountNameDataFile() const {
	if (!taxableAccountNameDataFile) {
		// Need to load the file.
		loadFile(TAXABLE_ACCOUNT_NAME_DATA_FILE);
	}
	return *taxableAccountNameDataFile;
}

const TaxRateCountryDataFile_t &DataFileManager::TaxRateCountryDataFile() const {
	if (!taxRateCountryDataFile) {
		// Need to load the file.
		loadFile(TAX_RATE_COUNTRY_DATA_FILE);
	}
	return *taxRateCountryDataFile;
}

const TaxRateDivisionDataFile_t &DataFileManager::TaxRateDivisionDataFile() const {
	if (!taxRateDivisionDataFile) {
		// Need to load the file.
		loadFile(TAX_RATE_DIVISION_DATA_FILE);
	}
	return *taxRateDivisionDataFile;
}

const TradeTypeDataFile_t &DataFileManager::TradeTypeDataFile() const {
	if (!tradeTypeDataFile) {
		// Need to load the file.
		loadFile(TRADE_TYPE_DATA_FILE);
	}
	return *tradeTypeDataFile;
}

const ZipCodeDataFile_t &DataFileManager::ZipCodeDataFile() const {
	if (!zipCodeDataFile) {
		// Need to load the file.
		loadFile(ZIPCODE_DATA_FILE);
	}
	return *zipCodeDataFile;
}

const CCompanyCompetitorFile &DataFileManager::CompanyCompetitorFile() const {
	if (!companyCompetitorFile) {
		// Need to create the "file". Fully qualify constructor to distinquish
		// from this method.
		companyCompetitorFile = new CCompanyCompetitorFile(CompanyCompetitorDataFile(), configuredCustomers,
		                                                   activeCustomers, CompanyDataFile().size());
	}
	return *companyCompetitorFile;
}

const CCompanyFile &DataFileManager::CompanyFile() const {
	if (!companyFile) {
		// Need to create the "file". Fully qualify constructor to distinquish
		// from this method.
		companyFile = new CCompanyFile(CompanyDataFile(), configuredCustomers, activeCustomers);
	}
	return *companyFile;
}

const CSecurityFile &DataFileManager::SecurityFile() const {
	if (!securityFile) {
		// Need to create the "file". Fully qualify constructor to distinquish
		// from this method.
		securityFile =
		    new CSecurityFile(SecurityDataFile(), configuredCustomers, activeCustomers, CompanyDataFile().size());
	}
	return *securityFile;
}

const CTaxRateFile &DataFileManager::TaxRateFile() const {
	if (!taxRateFile) {
		// Need to create the "file". Fully qualify constructor to distinquish
		// from this method.
		taxRateFile = new CTaxRateFile(TaxRateCountryDataFile(), TaxRateDivisionDataFile());
	}
	return *taxRateFile;
}
