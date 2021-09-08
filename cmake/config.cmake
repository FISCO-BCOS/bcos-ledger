hunter_config(bcos-framework VERSION 3.0.0-ca941668c0e91635ab3ef0e889574d3812ad78e0
        # URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/11014edcbc0b20d6eff9464bb7b613e9173f59a0.tar.gz
        URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/0e19068a8bdc4830e87677d84ba16bc1e54806d9.tar.gz
        SHA1 5fe869187a2a9bf6a3444ef1df9f4ab340ece98e
        CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON HUNTER_KEEP_PACKAGE_SOURCES=ON #DEBUG=ON
)

hunter_config(wedpr-crypto VERSION 1.1.0-10f314de
        URL https://${URL_BASE}/WeBankBlockchain/WeDPR-Lab-Crypto/archive/10f314de45ec31ce9e330922b522ce173662ed33.tar.gz
        SHA1 626df59f87ea2c6bb5128f7d104588179809910b
        CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=OFF HUNTER_PACKAGE_LOG_INSTALL=ON HUNTER_KEEP_PACKAGE_SOURCES=ON
)

hunter_config(bcos-crypto
        VERSION 3.0.0-local-43df7523
        URL https://${URL_BASE}/FISCO-BCOS/bcos-crypto/archive/255002b047b359a45c953d1dab29efd2ff6eb080.tar.gz
        SHA1 4d02de20be1f9bf79d762c5b8686368286504e07
        CMAKE_ARGS URL_BASE=${URL_BASE} HUNTER_KEEP_PACKAGE_SOURCES=ON
)
