add_library_unity(
    error
    OBJECT
    CAutoTrace.cpp                    
    CErrorContext.cpp
    CException.cpp
    CFSimulator.cpp
    CLogger.cpp
    CLoggerStream.cpp
    CLoggerSyslog.cpp
    CMessage.cpp
    CMessageRepository.cpp
    CMessageTable.cpp
    CMiniDumper.cpp
    CSerializable.cpp
    ILogger.cpp
    CErrorHandlerStandard.cpp )
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:error>
    PARENT_SCOPE)