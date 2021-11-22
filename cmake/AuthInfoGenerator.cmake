include(AuthBuiltInInfo)
message("fetch git submodule bcos-auth " ${CMAKE_CURRENT_SOURCE_DIR})
FetchContent_Declare(bcos_auth_project
        GIT_REPOSITORY https://${URL_BASE}/FISCO-BCOS/bcos-auth.git
        GIT_TAG        f7b1ea64becc39ee3574805e02d0851e23dd2314
        #SOURCE_DIR     ${CMAKE_CURRENT_SOURCE_DIR}/deps/src/bcos-auth
        )
if(NOT bcos_auth_projectPOPULATED)
    FetchContent_Populate(bcos_auth_project)
endif()
create_auth_info(${bcos_auth_project_SOURCE_DIR})