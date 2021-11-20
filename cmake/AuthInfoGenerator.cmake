include(AuthBuiltInInfo)
execute_process(WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMAND  git submodule update --remote --recursive)
create_auth_info()