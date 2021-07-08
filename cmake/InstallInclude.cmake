set(DESTINATION_INCLUDE_DIR "${CMAKE_INSTALL_INCLUDEDIR}/bcos-ledger")

install(
        DIRECTORY "src/libledger"
        DESTINATION "${DESTINATION_INCLUDE_DIR}"
        FILES_MATCHING PATTERN "*.h"
)

install(
        DIRECTORY "src/libeventfilter"
        DESTINATION "${DESTINATION_INCLUDE_DIR}"
        FILES_MATCHING PATTERN "*.h"
)