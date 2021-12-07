hunter_config(bcos-framework VERSION 3.0.1-local
        URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/1003d35925569d25c84483aef4402556e6c74f18.tar.gz
	SHA1 502eb233c2ce6f7c0e7e70b8bf64e50d25545c31
        CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON HUNTER_KEEP_PACKAGE_SOURCES=ON #DEBUG=ON
)

# hunter_config(wedpr-crypto VERSION 1.1.0-10f314de
#         URL https://${URL_BASE}/WeBankBlockchain/WeDPR-Lab-Crypto/archive/10f314de45ec31ce9e330922b522ce173662ed33.tar.gz
#         SHA1 626df59f87ea2c6bb5128f7d104588179809910b
#         CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=OFF HUNTER_PACKAGE_LOG_INSTALL=ON HUNTER_KEEP_PACKAGE_SOURCES=ON
# )

hunter_config(bcos-crypto
        VERSION 3.0.0-local-43df7523
        URL https://${URL_BASE}/FISCO-BCOS/bcos-crypto/archive/255002b047b359a45c953d1dab29efd2ff6eb080.tar.gz
        SHA1 4d02de20be1f9bf79d762c5b8686368286504e07
        CMAKE_ARGS URL_BASE=${URL_BASE} HUNTER_KEEP_PACKAGE_SOURCES=ON
)
