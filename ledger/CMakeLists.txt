file(GLOB SRC_LIST "*.cpp")
file(GLOB HEADERS "*.h")

add_library(${BCOS_LEDGER_TARGET} ${SRC_LIST} ${HEADERS})
target_compile_options(${BCOS_LEDGER_TARGET} PRIVATE -Wno-error -Wno-unused-variable)

