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
 * @file BlockUtilitiesTest.cpp
 * @author: kyonRay
 * @date 2021-05-06
 */

#include "bcos-ledger/ledger/utilities/BlockUtilities.h"
#include "unittests/ledger/common/FakeTransaction.h"
#include "unittests/ledger/common/FakeReceipt.h"
#include "unittests/ledger/common/FakeBlock.h"
#include <bcos-test/libutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

namespace bcos::test
{
BOOST_FIXTURE_TEST_SUITE(CommonTest, TestPromptFixture)

BOOST_AUTO_TEST_CASE(testBlockTxListSetterGetter)
{
    auto txs = fakeTransactions(10);
    auto blockFactory = createBlockFactory();
    auto block1 = fakeBlock(blockFactory, 10, 10);
    auto number = bcos::ledger::blockTransactionListSetter(block1, txs);
    BOOST_CHECK_EQUAL(number, 10);
    BOOST_CHECK_EQUAL(block1->transactionsSize(), 10);
    auto txs_get = bcos::ledger::blockTransactionListGetter(block1);
    BOOST_CHECK_EQUAL(txs_get->size(), 10);
}
BOOST_AUTO_TEST_CASE(testNullTxSetter){
    auto txs = fakeTransactions(10);
    auto error1 = bcos::ledger::blockTransactionListSetter(nullptr, txs);
    auto error2 = bcos::ledger::blockTransactionListSetter(nullptr, nullptr);

    auto blockFactory = createBlockFactory();
    auto block1 = fakeBlock(blockFactory, 10, 10);
    auto error3 = bcos::ledger::blockTransactionListSetter(block1, nullptr);

    auto txs_empty = fakeTransactions(0);
    auto error4 = bcos::ledger::blockTransactionListSetter(block1, txs_empty);

    BOOST_CHECK_EQUAL(error1, -1);
    BOOST_CHECK_EQUAL(error2, -1);
    BOOST_CHECK_EQUAL(error3, -1);
    BOOST_CHECK_EQUAL(error4, -1);
}

BOOST_AUTO_TEST_CASE(testEmptyBlockTxGetter){
    auto blockFactory = createBlockFactory();
    auto block1 = fakeBlock(blockFactory, 10, 10);
    auto errorTx1 = bcos::ledger::blockTransactionListGetter(block1);
    BOOST_CHECK_EQUAL(errorTx1->size(),0);

    auto errorTx2 = bcos::ledger::blockTransactionListGetter(nullptr);
    BOOST_CHECK_EQUAL(errorTx2, nullptr);
}

BOOST_AUTO_TEST_CASE(testBlockReceiptListSetterGetter)
{
    auto receipts = fakeReceipts(10);
    auto blockFactory = createBlockFactory();
    auto block1 = fakeBlock(blockFactory, 10, 10);
    auto number = bcos::ledger::blockReceiptListSetter(block1, receipts);
    BOOST_CHECK_EQUAL(number, 10);
    BOOST_CHECK_EQUAL(block1->receiptsSize(), 10);
    auto receipts_get = bcos::ledger::blockReceiptListGetter(block1);
    BOOST_CHECK_EQUAL(receipts_get->size(), 10);
}

BOOST_AUTO_TEST_CASE(testNullReceiptSetter){
    auto receipts = fakeReceipts(10);
    auto error1 = bcos::ledger::blockReceiptListSetter(nullptr, receipts);
    auto error2 = bcos::ledger::blockReceiptListSetter(nullptr, nullptr);

    auto blockFactory = createBlockFactory();
    auto block1 = fakeBlock(blockFactory, 10, 10);
    auto error3 = bcos::ledger::blockReceiptListSetter(block1, nullptr);

    auto receipts_empty = fakeReceipts(0);
    auto error4 = bcos::ledger::blockReceiptListSetter(block1, receipts_empty);

    BOOST_CHECK_EQUAL(error1, -1);
    BOOST_CHECK_EQUAL(error2, -1);
    BOOST_CHECK_EQUAL(error3, -1);
    BOOST_CHECK_EQUAL(error4, -1);
}
BOOST_AUTO_TEST_CASE(testEmptyBlockReceiptGetter){
    auto blockFactory = createBlockFactory();
    auto block1 = fakeBlock(blockFactory, 10, 10);
    auto errorReceipt1 = bcos::ledger::blockReceiptListGetter(block1);
    BOOST_CHECK_EQUAL(errorReceipt1->size(),0);

    auto errorReceipt2 = bcos::ledger::blockReceiptListGetter(nullptr);
    BOOST_CHECK_EQUAL(errorReceipt2, nullptr);
}

BOOST_AUTO_TEST_SUITE_END()
}
