include(GNUInstallDirs)
set(DESTINATION_INCLUDE_DIR "${CMAKE_INSTALL_INCLUDEDIR}/bcos-ledger")
file(COPY ledger
        DESTINATION ${DESTINATION_INCLUDE_DIR}
        FILES_MATCHING PATTERN "*.h")