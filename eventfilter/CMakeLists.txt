file(GLOB SRC_LIST "*.cpp")
file(GLOB HEADERS "*.h")

add_library(${BCOS_EVENT_FILTER_TARGET} ${SRC_LIST} ${HEADERS})
target_link_libraries(${BCOS_EVENT_FILTER_TARGET} PUBLIC bcos-framework::utilities)
target_compile_options(${BCOS_EVENT_FILTER_TARGET} PRIVATE -Wno-error -Wno-unused-variable)
