file(GLOB_RECURSE SRC_LIST "*.cpp")
file(GLOB_RECURSE HEADERS "*.h")

add_library(${BCOS_LEDGER_TARGET} ${SRC_LIST} ${HEADERS})
target_link_libraries(${BCOS_LEDGER_TARGET} PUBLIC bcos-framework::utilities)
target_compile_options(${BCOS_LEDGER_TARGET} PRIVATE -Wno-error -Wno-unused-variable)
