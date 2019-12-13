add_library(tpce_main OBJECT
            AddressTable.cpp
            BaseLogger.cpp
            CE.cpp
            CETxnInputGenerator.cpp
            CETxnMixGenerator.cpp
            ChargeTable.cpp
            CommissionRateTable.cpp
            CustomerSelection.cpp
            CustomerTable.cpp
            DM.cpp
            EGenGenerateAndLoad.cpp
            EGenLogFormatterTab.cpp
            ExchangeTable.cpp
            FlatFileLoader.cpp
            IndustryTable.cpp
            MEE.cpp
            MEEPriceBoard.cpp
            MEESecurity.cpp
            MEETickerTape.cpp
            MEETradingFloor.cpp
            Person.cpp
            SectorTable.cpp
            StatusTypeTable.cpp
            TaxRateTable.cpp
            TradeGen.cpp
            TradeTypeTable.cpp
            WheelTime.cpp
            ZipCodeTable.cpp
            bucketsimulator.cpp
            progressmeter.cpp
            progressmeterinterface.cpp
            strutil.cpp)
set(TPCE_OBJECT_FILES
    ${TPCE_OBJECT_FILES} $<TARGET_OBJECTS:tpce_main>
    PARENT_SCOPE)

disable_target_warnings(tpce_main)
