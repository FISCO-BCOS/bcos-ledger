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
#include "unittests/ledger/common/FakeTransaction.h"
#include "bcos-ledger/ledger/storage/StorageGetter.h"
#include "bcos-ledger/ledger/storage/StorageSetter.h"
#include <bcos-test/libutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

namespace bcos::test
{
BOOST_FIXTURE_TEST_SUITE(StorageUtilitiesTest, TestPromptFixture)
BOOST_AUTO_TEST_CASE(testTableSetterGetterByRowAndField)
{
    auto tableFactory = fakeTableFactory(0);
    auto storageGetter =  StorageGetter::storageGetterFactory();
    auto storageSetter = StorageSetter::storageSetterFactory();

    bool setterRet = storageSetter->tableSetterByRowAndField(
        tableFactory, "_sys_config_", "test", "hello", "world");
    BOOST_CHECK(setterRet);

    auto ret =
        storageGetter->tableGetterByRowAndField(tableFactory, "_sys_config_", "test", "hello");
    BOOST_CHECK_EQUAL(ret, "world");
}
BOOST_AUTO_TEST_CASE(testErrorOpenTable)
{
    auto tableFactory = fakeErrorTableFactory();
    auto storageSetter = StorageSetter::storageSetterFactory();
    BOOST_CHECK_THROW(storageSetter->tableSetterByRowAndField(tableFactory, "errorTable", "error", "error", ""),
                      OpenSysTableFailed);
    BOOST_CHECK_EQUAL(storageSetter->setSysConfig(tableFactory,"","",""),
        false);
    BOOST_CHECK_THROW(storageSetter->writeTxToBlock(nullptr, tableFactory),
        OpenSysTableFailed);
}
BOOST_AUTO_TEST_CASE(testWritTx2Block){
    auto blockFactory = createBlockFactory();
    auto block = fakeBlock(blockFactory, 10, 10);
    auto txs = fakeTransactions(10);
    for (auto & tx : *txs)
    {
        block->appendTransaction(tx);
    }
    auto tableFactory = fakeTableFactory(block->blockHeader()->number());
    auto storageSetter = StorageSetter::storageSetterFactory();
    storageSetter->writeTxToBlock(block, tableFactory);

    auto storageGetter =  StorageGetter::storageGetterFactory();

    auto test_tx = txs->at(2);
    auto numberIndex =
        storageGetter->getBlockNumberAndIndexByHash(test_tx->hash().hex(), tableFactory);
    BOOST_CHECK_EQUAL(numberIndex->first, std::to_string(block->blockHeader()->number()));
    BOOST_CHECK_EQUAL(numberIndex->second, std::to_string(2));
}
BOOST_AUTO_TEST_CASE(testGetterSetter)
{
    auto blockFactory = createBlockFactory();
    auto block = fakeBlock(blockFactory, 10, 10);

    auto tableFactory = fakeTableFactory(block->blockHeader()->number());
    auto storageGetter =  StorageGetter::storageGetterFactory();
    auto storageSetter = StorageSetter::storageSetterFactory();

    auto number = block->blockHeader()->number();
    auto numberStr = boost::lexical_cast<std::string>(number);
    auto hash = block->blockHeader()->hash();
    auto hashStr = hash.hex();

    // SYS_NUMBER_2_BLOCK
    auto setNumber2BlockRet = storageSetter->setNumber2Block(tableFactory, numberStr, "test1");
    auto getFullBlockFromStorageRet = storageGetter->getFullBlockFromStorage(number, tableFactory);
    BOOST_CHECK(setNumber2BlockRet);
    BOOST_CHECK_EQUAL(getFullBlockFromStorageRet, "test1");

    auto setNumber2BlockRet2 = storageSetter->setNumber2Block(tableFactory, "123", "");
    auto getFullBlockFromStorageRet2 = storageGetter->getFullBlockFromStorage(123, tableFactory);
    BOOST_CHECK(setNumber2BlockRet2);
    BOOST_CHECK_EQUAL(getFullBlockFromStorageRet2, "");

    // SYS_CURRENT_STATE
    auto setCurrentStateRet = storageSetter->setCurrentState(tableFactory, "test", "test2");
    auto getCurrentStateRet = storageGetter->getCurrentState("test", tableFactory);
    BOOST_CHECK(setCurrentStateRet);
    BOOST_CHECK_EQUAL(getCurrentStateRet, "test2");

    // SYS_NUMBER_2_HEADER
    auto setNumber2HeaderRet = storageSetter->setNumber2Header(tableFactory, numberStr, "");
    auto getBlockHeaderFromStorageRet = storageGetter->getBlockHeaderFromStorage(number, tableFactory);
    BOOST_CHECK(setNumber2HeaderRet);
    BOOST_CHECK_EQUAL(getBlockHeaderFromStorageRet, "");

    // SYS_NUMBER_2_TXS
    auto setNumber2TxsRet = storageSetter->setNumber2Txs(tableFactory, numberStr, "");
    auto getTxsFromStorageRet = storageGetter->getTxsFromStorage(number, tableFactory);
    BOOST_CHECK(setNumber2TxsRet);
    BOOST_CHECK_EQUAL(getTxsFromStorageRet, "");

    // SYS_NUMBER_2_RECEIPTS
    auto setNumber2ReceiptsRet = storageSetter->setNumber2Receipts(tableFactory, numberStr, "");
    auto getReceiptsFromStorageRet = storageGetter->getReceiptsFromStorage(number, tableFactory);
    BOOST_CHECK(setNumber2ReceiptsRet);
    BOOST_CHECK_EQUAL(getReceiptsFromStorageRet, "");

    // SYS_HASH_2_NUMBER
    auto setHash2NumberRet = storageSetter->setHash2Number(tableFactory, hashStr, "");
    auto getBlockNumberByHashRet = storageGetter->getBlockNumberByHash(hashStr, tableFactory);
    BOOST_CHECK(setHash2NumberRet);
    BOOST_CHECK_EQUAL(getBlockNumberByHashRet, "");

    auto setHash2NumberRet2 = storageSetter->setHash2Number(tableFactory, "test", numberStr);
    auto getBlockHashByNumberRet = storageGetter->getBlockHashByNumber(numberStr, tableFactory);
    BOOST_CHECK(setHash2NumberRet2);
    // MockStorage return {1,2,3}
    BOOST_CHECK_EQUAL(getBlockHashByNumberRet, "1");

    // SYS_NUMBER_NONCES
    auto setNumber2NoncesRet = storageSetter->setNumber2Nonces(tableFactory, numberStr, "");
    auto getNoncesFromStorageRet = storageGetter->getNoncesFromStorage(number, tableFactory);
    BOOST_CHECK(setNumber2NoncesRet);
    BOOST_CHECK_EQUAL(getNoncesFromStorageRet, "");

    // SYS_CONFIG
    auto setSysConfigRet = storageSetter->setSysConfig(tableFactory, "test", "test4", "0");
    auto getSysConfigRet = storageGetter->getSysConfig("test", tableFactory);
    BOOST_CHECK(setSysConfigRet);
    BOOST_CHECK_EQUAL(getSysConfigRet->first, "test4");
    BOOST_CHECK_EQUAL(getSysConfigRet->second, "0");
}

BOOST_AUTO_TEST_SUITE_END()
}