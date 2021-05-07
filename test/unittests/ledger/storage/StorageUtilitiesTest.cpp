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


BOOST_AUTO_TEST_SUITE_END()
}