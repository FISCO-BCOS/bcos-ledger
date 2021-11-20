set(DESTINATION_INCLUDE_DIR "${CMAKE_INSTALL_INCLUDEDIR}/bcos-ledger")
message("Ledger destination include dir " ${DESTINATION_INCLUDE_DIR})
install(
        DIRECTORY "src/libledger"
        DESTINATION "${DESTINATION_INCLUDE_DIR}"
        FILES_MATCHING PATTERN "*.h"
)