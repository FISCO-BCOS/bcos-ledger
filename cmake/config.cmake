hunter_config(bcos-framework VERSION 3.0.0-ca941668c0e91635ab3ef0e889574d3812ad78e0
        # URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/11014edcbc0b20d6eff9464bb7b613e9173f59a0.tar.gz
        URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/35088cce667d76d9632a207bb9e4010500998f82.tar.gz
        SHA1 0590f6590e746c040b75a1702a0995a6982f0eae
        CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON #DEBUG=ON
)

hunter_config(wedpr-crypto VERSION 1.1.0-10f314de
        URL https://${URL_BASE}/WeBankBlockchain/WeDPR-Lab-Crypto/archive/10f314de45ec31ce9e330922b522ce173662ed33.tar.gz
        SHA1 626df59f87ea2c6bb5128f7d104588179809910b
        CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=OFF HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(bcos-crypto
        VERSION 3.0.0-local-43df7523
        URL https://${URL_BASE}/FISCO-BCOS/bcos-crypto/archive/255002b047b359a45c953d1dab29efd2ff6eb080.tar.gz
        SHA1 4d02de20be1f9bf79d762c5b8686368286504e07
        CMAKE_ARGS URL_BASE=${URL_BASE}
)
