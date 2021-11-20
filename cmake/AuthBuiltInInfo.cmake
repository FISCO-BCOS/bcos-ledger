function(create_auth_info BCOS_AUTH_SOURCE)
    # Set Auth built-in bin, set to AuthBuiltInInfo.h
    set(AUTH_SOURCE_DIR "${BCOS_AUTH_SOURCE}")
    set(AUTH_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/auth-temp/output")
    set(AUTH_OUTPUT_SM_DIR "${CMAKE_CURRENT_BINARY_DIR}/auth-temp/output_sm")
    list(APPEND AUTH_SOURCE_LIST ${AUTH_SOURCE_DIR}/ContractInterceptor.sol ${AUTH_SOURCE_DIR}/ProposalManager.sol ${AUTH_SOURCE_DIR}/Committee.sol)

    set(AUTH_INTERCEPT_BIN_PATH     "${AUTH_OUTPUT_DIR}/ContractInterceptor.bin")
    set(AUTH_INTERCEPT_SM_BIN_PATH  "${AUTH_OUTPUT_SM_DIR}/ContractInterceptor.bin")
    set(AUTH_PROPOSAL_BIN_PATH      "${AUTH_OUTPUT_DIR}/ProposalManager.bin")
    set(AUTH_PROPOSAL_SM_BIN_PATH   "${AUTH_OUTPUT_SM_DIR}/ProposalManager.bin")
    set(AUTH_COMMITTEE_BIN_PATH     "${AUTH_OUTPUT_DIR}/Committee.bin")
    set(AUTH_COMMITTEE_SM_BIN_PATH  "${AUTH_OUTPUT_SM_DIR}/Committee.bin")

    # TODO: check solc exist first
    foreach(SOL_SOURCE ${AUTH_SOURCE_LIST})
        message("solc compile: " "${SOL_SOURCE}")
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

    set(AUTH_DST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/libledger")
    set(LEDGER_AUTH_INFO_IN "${CMAKE_CURRENT_SOURCE_DIR}/cmake/templates/AuthBuiltInInfo.h.in")

    message("AUTH_DST_DIR: " ${AUTH_DST_DIR})
    message("LEDGER_AUTH_INFO_IN: " ${LEDGER_AUTH_INFO_IN})
    configure_file("${LEDGER_AUTH_INFO_IN}" "${AUTH_DST_DIR}/AuthBuiltInInfo.h")

    include_directories(BEFORE ${PROJECT_BINARY_DIR})
endfunction()
