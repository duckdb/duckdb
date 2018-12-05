#ifndef DATA_FILE_TYPES_H
#define DATA_FILE_TYPES_H

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

#include <string>

// General data file types.
#include "DataFile.h"
#include "WeightedDataFile.h"
#include "BucketedDataFile.h"

// Specific data file types.
#include "AreaCodeDataFileRecord.h"
#include "ChargeDataFileRecord.h"
#include "CommissionRateDataFileRecord.h"
#include "CompanyCompetitorDataFileRecord.h"
#include "CompanyDataFileRecord.h"
#include "CompanySPRateDataFileRecord.h"
#include "ExchangeDataFileRecord.h"
#include "FemaleFirstNameDataFileRecord.h"
#include "IndustryDataFileRecord.h"
#include "LastNameDataFileRecord.h"
#include "MaleFirstNameDataFileRecord.h"
#include "NewsDataFileRecord.h"
#include "NonTaxableAccountNameDataFileRecord.h"
#include "SectorDataFileRecord.h"
#include "SecurityDataFileRecord.h"
#include "StatusTypeDataFileRecord.h"
#include "StreetNameDataFileRecord.h"
#include "StreetSuffixDataFileRecord.h"
#include "TaxableAccountNameDataFileRecord.h"
#include "TaxRateCountryDataFileRecord.h"
#include "TaxRateDivisionDataFileRecord.h"
#include "TradeTypeDataFileRecord.h"
#include "ZipCodeDataFileRecord.h"

namespace TPCE {

// For convenience provide a type for each specific data file type.
typedef WeightedDataFile<AreaCodeDataFileRecord> AreaCodeDataFile_t;
typedef DataFile<ChargeDataFileRecord> ChargeDataFile_t;
typedef DataFile<CommissionRateDataFileRecord> CommissionRateDataFile_t;
typedef DataFile<CompanyCompetitorDataFileRecord> CompanyCompetitorDataFile_t;
typedef DataFile<CompanyDataFileRecord> CompanyDataFile_t;
typedef WeightedDataFile<CompanySPRateDataFileRecord> CompanySPRateDataFile_t;
typedef DataFile<ExchangeDataFileRecord> ExchangeDataFile_t;
typedef WeightedDataFile<FemaleFirstNameDataFileRecord> FemaleFirstNameDataFile_t;
typedef DataFile<IndustryDataFileRecord> IndustryDataFile_t;
typedef WeightedDataFile<LastNameDataFileRecord> LastNameDataFile_t;
typedef WeightedDataFile<MaleFirstNameDataFileRecord> MaleFirstNameDataFile_t;
typedef WeightedDataFile<NewsDataFileRecord> NewsDataFile_t;
typedef DataFile<NonTaxableAccountNameDataFileRecord> NonTaxableAccountNameDataFile_t;
typedef DataFile<SectorDataFileRecord> SectorDataFile_t;
typedef DataFile<SecurityDataFileRecord> SecurityDataFile_t;
typedef DataFile<StatusTypeDataFileRecord> StatusTypeDataFile_t;
typedef WeightedDataFile<StreetNameDataFileRecord> StreetNameDataFile_t;
typedef WeightedDataFile<StreetSuffixDataFileRecord> StreetSuffixDataFile_t;
typedef DataFile<TaxableAccountNameDataFileRecord> TaxableAccountNameDataFile_t;
typedef BucketedDataFile<TaxRateCountryDataFileRecord> TaxRateCountryDataFile_t;
typedef BucketedDataFile<TaxRateDivisionDataFileRecord> TaxRateDivisionDataFile_t;
typedef DataFile<TradeTypeDataFileRecord> TradeTypeDataFile_t;
typedef WeightedDataFile<ZipCodeDataFileRecord> ZipCodeDataFile_t;

// WARNING: the DataFileManager constructor is tightly coupled to the order of
// this enum. List of file types.
enum DataFileType {
	AREA_CODE_DATA_FILE,
	CHARGE_DATA_FILE,
	COMMISSION_RATE_DATA_FILE,
	COMPANY_COMPETITOR_DATA_FILE,
	COMPANY_DATA_FILE,
	COMPANY_SP_RATE_DATA_FILE,
	EXCHANGE_DATA_FILE,
	FEMALE_FIRST_NAME_DATA_FILE,
	INDUSTRY_DATA_FILE,
	LAST_NAME_DATA_FILE,
	MALE_FIRST_NAME_DATA_FILE,
	NEWS_DATA_FILE,
	NON_TAXABLE_ACCOUNT_NAME_DATA_FILE,
	SECTOR_DATA_FILE,
	SECURITY_DATA_FILE,
	STATUS_TYPE_DATA_FILE,
	STREET_NAME_DATA_FILE,
	STREET_SUFFIX_DATA_FILE,
	TAXABLE_ACCOUNT_NAME_DATA_FILE,
	TAX_RATE_COUNTRY_DATA_FILE,
	TAX_RATE_DIVISION_DATA_FILE,
	TRADE_TYPE_DATA_FILE,
	ZIPCODE_DATA_FILE
};

// Default data file names.
static const char *const DataFileNames[] = {"AreaCode",
                                            "Charge",
                                            "CommissionRate",
                                            "CompanyCompetitor",
                                            "Company",
                                            "CompanySPRate",
                                            "Exchange",
                                            "FemaleFirstName",
                                            "Industry",
                                            "LastName",
                                            "MaleFirstName",
                                            "LastName", // News uses last names as a source of words.
                                            "NonTaxableAccountName",
                                            "Sector",
                                            "Security",
                                            "StatusType",
                                            "StreetName",
                                            "StreetSuffix",
                                            "TaxableAccountName",
                                            "TaxRatesCountry",
                                            "TaxRatesDivision",
                                            "TradeType",
                                            "ZipCode"};

// Default data file extension.
static const std::string DataFileExtension(".txt");

} // namespace TPCE
#endif // DATA_FILE_TYPES_H
