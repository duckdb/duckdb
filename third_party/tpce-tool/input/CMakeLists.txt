add_library(tpce_input OBJECT
            AreaCodeDataFileRecord.cpp
            ChargeDataFileRecord.cpp
            CommissionRateDataFileRecord.cpp
            CompanyCompetitorDataFileRecord.cpp
            CompanyCompetitorFile.cpp
            CompanyDataFileRecord.cpp
            CompanyFile.cpp
            CompanySPRateDataFileRecord.cpp
            DataFileManager.cpp
            ExchangeDataFileRecord.cpp
            FemaleFirstNameDataFileRecord.cpp
            IndustryDataFileRecord.cpp
            LastNameDataFileRecord.cpp
            MaleFirstNameDataFileRecord.cpp
            NewsDataFileRecord.cpp
            NonTaxableAccountNameDataFileRecord.cpp
            SectorDataFileRecord.cpp
            SecurityDataFileRecord.cpp
            SecurityFile.cpp
            StatusTypeDataFileRecord.cpp
            StreamSplitter.cpp
            StreetNameDataFileRecord.cpp
            StreetSuffixDataFileRecord.cpp
            StringSplitter.cpp
            TaxRateCountryDataFileRecord.cpp
            TaxRateDivisionDataFileRecord.cpp
            TaxRateFile.cpp
            TaxableAccountNameDataFileRecord.cpp
            TextFileSplitter.cpp
            TradeTypeDataFileRecord.cpp
            Utilities.cpp
            ZipCodeDataFileRecord.cpp)
set(TPCE_OBJECT_FILES
    ${TPCE_OBJECT_FILES} $<TARGET_OBJECTS:tpce_input>
    PARENT_SCOPE)

disable_target_warnings(tpce_input)
