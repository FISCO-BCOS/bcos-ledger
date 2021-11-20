macro(replace_if_different SOURCE DST)
    set(extra_macro_args ${ARGN})
    set(options CREATE)
    set(one_value_args)
    set(multi_value_args)
    cmake_parse_arguments(REPLACE_IF_DIFFERENT "${options}" "${one_value_args}" "${multi_value_args}" "${extra_macro_args}")

    if (REPLACE_IF_DIFFERENT_CREATE AND (NOT (EXISTS "${DST}")))
        file(WRITE "${DST}" "")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} -E compare_files "${SOURCE}" "${DST}" RESULT_VARIABLE DIFFERENT OUTPUT_QUIET ERROR_QUIET)

    if (DIFFERENT)
        execute_process(COMMAND ${CMAKE_COMMAND} -E rename "${SOURCE}" "${DST}")
    else()
        execute_process(COMMAND ${CMAKE_COMMAND} -E remove "${SOURCE}")
    endif()
endmacro()

function(create_auth_info)
    # Set Auth built-in bin and abi, set to AuthBuiltInInfo.h
    set(AUTH_SOURCE_DIR "${PROJECT_SOURCE_DIR}/bcos-auth")
    set(AUTH_OUTPUT_DIR "${PROJECT_BINARY_DIR}/auth-temp/output")
    set(AUTH_OUTPUT_SM_DIR "${PROJECT_BINARY_DIR}/auth-temp/output_sm")
    file(GLOB_RECURSE AUTH_SOURCE_SOL bcos-auth/*.sol)
    list(APPEND AUTH_SOURCE_LIST ${AUTH_SOURCE_SOL})

    set(AUTH_INTERCEPT_BIN_PATH           "${AUTH_OUTPUT_DIR}/ContractInterceptor.bin")
    set(AUTH_INTERCEPT_SM_BIN_PATH        "${AUTH_OUTPUT_SM_DIR}/ContractInterceptor.bin")
    set(AUTH_PROPOSAL_BIN_PATH      "${AUTH_OUTPUT_DIR}/ProposalManager.bin")
    set(AUTH_PROPOSAL_SM_BIN_PATH   "${AUTH_OUTPUT_SM_DIR}/ProposalManager.bin")
    set(AUTH_COMMITTEE_BIN_PATH     "${AUTH_OUTPUT_DIR}/CommitteeManager.bin")
    set(AUTH_COMMITTEE_SM_BIN_PATH  "${AUTH_OUTPUT_SM_DIR}/CommitteeManager.bin")

    # TODO: check solc exist first
    foreach(SOL_SOURCE ${AUTH_SOURCE_LIST})
        execute_process(COMMAND  $ENV{HOME}/.fisco/solc/0.4.25/keccak256/solc --bin --optimize --overwrite  ${SOL_SOURCE} -o ${AUTH_OUTPUT_DIR})
        execute_process(COMMAND  $ENV{HOME}/.fisco/solc/0.4.25/sm3/solc --bin --optimize --overwrite  ${SOL_SOURCE}  -o ${AUTH_OUTPUT_SM_DIR})
    endforeach()
    execute_process(COMMAND cat ${AUTH_INTERCEPT_BIN_PATH}
            OUTPUT_VARIABLE AUTH_INTERCEPT_BIN OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND cat ${AUTH_INTERCEPT_SM_BIN_PATH}
            OUTPUT_VARIABLE AUTH_INTERCEPT_SM_BIN OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND cat ${AUTH_PROPOSAL_BIN_PATH}
            OUTPUT_VARIABLE AUTH_PROPOSAL_BIN OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND cat ${AUTH_PROPOSAL_SM_BIN_PATH}
            OUTPUT_VARIABLE AUTH_PROPOSAL_SM_BIN OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND cat ${AUTH_COMMITTEE_BIN_PATH}
            OUTPUT_VARIABLE AUTH_COMMITTEE_BIN OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND cat ${AUTH_COMMITTEE_SM_BIN_PATH}
            OUTPUT_VARIABLE AUTH_COMMITTEE_SM_BIN OUTPUT_STRIP_TRAILING_WHITESPACE)

    # Generate header file containing auth bin hex string

    set(LEDGER_DST_DIR "${PROJECT_BINARY_DIR}/include")
    set(LEDGER_AUTH_INFO_IN "${CMAKE_CURRENT_SOURCE_DIR}/cmake/templates/AuthBuiltInInfo.h.in")
    set(TMP_FILE "${LEDGER_DST_DIR}/AuthBuiltInInfo.h.tmp")
    set(OUTFILE "${LEDGER_DST_DIR}/AuthBuiltInInfo.h")

    configure_file("${LEDGER_AUTH_INFO_IN}" "${TMP_FILE}")
    replace_if_different("${TMP_FILE}" "${OUTFILE}" CREATE)

    include_directories(BEFORE ${PROJECT_BINARY_DIR})
endfunction()
