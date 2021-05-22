/**
 *  Copyright (C) 2021 FISCO BCOS.
 *  SPDX-License-Identifier: Apache-2.0
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * @file StorageUtilitiesTest.cpp
 * @author: kyonRay
 * @date 2021-05-06
 */

#include "unittests/ledger/common/FakeTable.h"
#include "unittests/ledger/common/FakeBlock.h"
#include "../ledger/storage/StorageGetter.h"
#include "../ledger/storage/StorageSetter.h"
#include <bcos-framework/testutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

namespace bcos::test
{
class TableFactoryFixture : public TestPromptFixture{
public:
    TableFactoryFixture() :TestPromptFixture(){
        tableFactory = fakeTableFactory(0);
        BOOST_TEST(tableFactory!=nullptr);
        storageGetter =  StorageGetter::storageGetterFactory();
        storageSetter = StorageSetter::storageSetterFactory();
        storageSetter->createTables(tableFactory);
    }
    ~TableFactoryFixture(){}

    TableFactoryInterface::Ptr tableFactory = nullptr;
    StorageSetter::Ptr storageSetter = nullptr;
    StorageGetter::Ptr storageGetter = nullptr;
};

BOOST_FIXTURE_TEST_SUITE(StorageUtilitiesTest, TableFactoryFixture)
BOOST_AUTO_TEST_CASE(testTableSetterGetterByRowAndField)
{
    bool setterRet =
        storageSetter->syncTableSetter(tableFactory, SYS_HASH_2_NUMBER, "test", SYS_VALUE, "world");
    BOOST_CHECK(setterRet);

    storageGetter->asyncTableGetter(tableFactory, SYS_HASH_2_NUMBER, "test", SYS_VALUE,
        [&](Error::Ptr _error, std::shared_ptr<std::string> _ret) {
            BOOST_CHECK_EQUAL(_error->errorCode(), 0);
            BOOST_CHECK_EQUAL(*_ret, "world");
        });
}
BOOST_AUTO_TEST_CASE(testErrorOpenTable)
{
    auto tableFactory = fakeErrorTableFactory();
    auto storageSetter = StorageSetter::storageSetterFactory();
    BOOST_CHECK_THROW(
        storageSetter->syncTableSetter(tableFactory, "errorTable", "error", "error", ""),
                      OpenSysTableFailed);
    BOOST_CHECK_EQUAL(storageSetter->setSysConfig(tableFactory,"","",""),
        false);
}
BOOST_AUTO_TEST_CASE(testGetterSetter)
{
    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto block = fakeBlock(crypto, blockFactory, 10, 10);

    auto number = block->blockHeader()->number();
    auto numberStr = boost::lexical_cast<std::string>(number);
    auto hash = block->blockHeader()->hash();
    auto hashStr = hash.hex();

    // SYS_CURRENT_STATE
    auto setCurrentStateRet = storageSetter->setCurrentState(tableFactory, "test", "test2");
    BOOST_CHECK(setCurrentStateRet);
    storageGetter->getCurrentState("test", tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getCurrentStateRet) {
            BOOST_CHECK_EQUAL(_error->errorCode(), 0);
            BOOST_CHECK_EQUAL(*getCurrentStateRet, "test2");
        });

    // SYS_NUMBER_2_HEADER
    auto setNumber2HeaderRet = storageSetter->setNumber2Header(tableFactory, numberStr, "");
    BOOST_CHECK(setNumber2HeaderRet);
    storageGetter->getBlockHeaderFromStorage(number, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getBlockHeaderFromStorageRet) {
            BOOST_CHECK_EQUAL(_error->errorCode(), 0);
            BOOST_CHECK_EQUAL(*getBlockHeaderFromStorageRet, "");
        });

    // SYS_NUMBER_2_TXS
    auto setNumber2TxsRet = storageSetter->setNumber2Txs(tableFactory, numberStr, "");
    BOOST_CHECK(setNumber2TxsRet);
    storageGetter->getTxsFromStorage(number, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getTxsFromStorageRet) {
            BOOST_CHECK_EQUAL(_error->errorCode(), 0);
            BOOST_CHECK_EQUAL(*getTxsFromStorageRet, "");
        });

    // SYS_NUMBER_2_RECEIPTS
    auto setNumber2ReceiptsRet = storageSetter->setHashToReceipt(tableFactory, "txHash", "");
    BOOST_CHECK(setNumber2ReceiptsRet);
    storageGetter->getReceiptByTxHash("txHash", tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getReceiptsFromStorageRet) {
            BOOST_CHECK_EQUAL(_error->errorCode(), 0);
            BOOST_CHECK_EQUAL(*getReceiptsFromStorageRet, "");
        });

    // SYS_HASH_2_NUMBER
    auto setHash2NumberRet = storageSetter->setHash2Number(tableFactory, hashStr, "");
    BOOST_CHECK(setHash2NumberRet);
    storageGetter->getBlockNumberByHash(hashStr, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getBlockNumberByHashRet) {
            BOOST_CHECK_EQUAL(_error->errorCode(), 0);
            BOOST_CHECK_EQUAL(*getBlockNumberByHashRet, "");
        });

    // SYS_NUMBER_2_HASH
    auto setNumber2HashRet = storageSetter->setNumber2Hash(tableFactory, numberStr, "");
    BOOST_CHECK(setNumber2HashRet);
    storageGetter->getBlockHashByNumber(number, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getBlockHashByNumberRet) {
            BOOST_CHECK_EQUAL(_error->errorCode(), 0);
            BOOST_CHECK_EQUAL(*getBlockHashByNumberRet, "");
        });

    // SYS_NUMBER_NONCES
    auto setNumber2NoncesRet = storageSetter->setNumber2Nonces(tableFactory, numberStr, "");
    BOOST_CHECK(setNumber2NoncesRet);

    storageGetter->getNoncesFromStorage(number, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getNoncesFromStorageRet) {
            BOOST_CHECK_EQUAL(_error->errorCode(), 0);
            BOOST_CHECK_EQUAL(*getNoncesFromStorageRet, "");
        });

    // SYS_CONFIG
    auto setSysConfigRet = storageSetter->setSysConfig(tableFactory, "test", "test4", "0");
    BOOST_CHECK(setSysConfigRet);
    storageGetter->getSysConfig(
        "test", tableFactory, [&](Error::Ptr _error, std::shared_ptr<stringsPair> getSysConfigRet) {
            BOOST_CHECK_EQUAL(_error->errorCode(), 0);
            BOOST_CHECK_EQUAL(getSysConfigRet->first, "test4");
            BOOST_CHECK_EQUAL(getSysConfigRet->second, "0");
        });
}

BOOST_AUTO_TEST_SUITE_END()
}