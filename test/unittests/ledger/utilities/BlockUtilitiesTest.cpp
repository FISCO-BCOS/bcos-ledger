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

#include "bcos-ledger/libledger/utilities/BlockUtilities.h"
#include "unittests/ledger/common/FakeBlock.h"
#include "unittests/ledger/common/FakeReceipt.h"
#include "unittests/ledger/common/FakeTransaction.h"
#include <bcos-framework/testutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

namespace bcos::test
{
BOOST_FIXTURE_TEST_SUITE(BlockUtilitiesTest, TestPromptFixture)

BOOST_AUTO_TEST_CASE(testBlockTxListSetterGetter)
{
    auto txs = fakeTransactions(10);
    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto block1 = fakeBlock(crypto, blockFactory, 0, 0, 1);
    auto number = bcos::ledger::blockTransactionListSetter(block1, txs);
    BOOST_CHECK_EQUAL(number, 10);
    BOOST_CHECK_EQUAL(block1->transactionsSize(), 10);
    auto txs_get = bcos::ledger::blockTransactionListGetter(block1);
    BOOST_CHECK_EQUAL(txs_get->size(), 10);
    BOOST_CHECK_EQUAL(txs_get->at(5)->hash().hex(), txs->at(5)->hash().hex());
}
BOOST_AUTO_TEST_CASE(blockTxHashListSetterGetter)
{
    auto txs = fakeTransactions(10);
    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto block1 = fakeBlock(crypto, blockFactory, 0, 0, 1);
    auto number = bcos::ledger::blockTransactionListSetter(block1, txs);
    BOOST_CHECK_EQUAL(number, 10);
    BOOST_CHECK_EQUAL(block1->transactionsSize(), 10);
    auto tx_hash_get = bcos::ledger::blockTxHashListGetter(block1);
    BOOST_CHECK_EQUAL(tx_hash_get->size(), 10);
    BOOST_CHECK_EQUAL(tx_hash_get->at(5), txs->at(5)->hash().hex());
}
BOOST_AUTO_TEST_CASE(testNullTxSetter)
{
    auto txs = fakeTransactions(10);
    auto error1 = bcos::ledger::blockTransactionListSetter(nullptr, txs);
    auto error2 = bcos::ledger::blockTransactionListSetter(nullptr, nullptr);

    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto block1 = fakeBlock(crypto, blockFactory, 10, 10, 1);
    auto error3 = bcos::ledger::blockTransactionListSetter(block1, nullptr);

    auto txs_empty = fakeTransactions(0);
    auto txs_number = bcos::ledger::blockTransactionListSetter(block1, txs_empty);

    BOOST_CHECK_EQUAL(error1, -1);
    BOOST_CHECK_EQUAL(error2, -1);
    BOOST_CHECK_EQUAL(error3, -1);
    BOOST_CHECK_EQUAL(txs_number, block1->transactionsSize());
}

BOOST_AUTO_TEST_CASE(testEmptyBlockTxGetter)
{
    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto block1 = fakeBlock(crypto, blockFactory, 0, 0, 1);
    auto errorTx1 = bcos::ledger::blockTransactionListGetter(block1);
    BOOST_CHECK_EQUAL(errorTx1->size(), 0);

    auto errorTx2 = bcos::ledger::blockTransactionListGetter(nullptr);
    BOOST_CHECK_EQUAL(errorTx2, nullptr);
}

BOOST_AUTO_TEST_CASE(testBlockReceiptListSetterGetter)
{
    auto receipts = fakeReceipts(10);
    auto crypto = createCryptoSuite();

    auto blockFactory = createBlockFactory(crypto);
    auto block1 = fakeBlock(crypto, blockFactory, 0, 0, 1);
    auto number = bcos::ledger::blockReceiptListSetter(block1, receipts);
    BOOST_CHECK_EQUAL(number, 10);
    BOOST_CHECK_EQUAL(block1->receiptsSize(), 10);
    auto receipts_get = bcos::ledger::blockReceiptListGetter(block1);
    BOOST_CHECK_EQUAL(receipts_get->size(), 10);
    BOOST_CHECK_EQUAL(receipts_get->at(5)->hash().hex(), receipts->at(5)->hash().hex());
}

BOOST_AUTO_TEST_CASE(testDecode)
{
    auto blockFactory = createBlockFactory(createCryptoSuite());
    auto block = decodeBlock(blockFactory, "");
    auto tx = decodeTransaction(blockFactory->transactionFactory(), "");
    auto header = decodeBlockHeader(blockFactory->blockHeaderFactory(), "");
    auto receipt = decodeReceipt(blockFactory->receiptFactory(), "");
    BOOST_CHECK(block == nullptr);
    BOOST_CHECK(tx == nullptr);
    BOOST_CHECK(header == nullptr);
    BOOST_CHECK(receipt == nullptr);
}

BOOST_AUTO_TEST_CASE(testNullReceiptSetter)
{
    auto receipts = fakeReceipts(10);
    auto error1 = bcos::ledger::blockReceiptListSetter(nullptr, receipts);
    auto error2 = bcos::ledger::blockReceiptListSetter(nullptr, nullptr);

    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto block1 = fakeBlock(crypto, blockFactory, 10, 10, 1);
    auto error3 = bcos::ledger::blockReceiptListSetter(block1, nullptr);

    auto receipts_empty = fakeReceipts(0);
    auto rcpt_number = bcos::ledger::blockReceiptListSetter(block1, receipts_empty);

    BOOST_CHECK_EQUAL(error1, -1);
    BOOST_CHECK_EQUAL(error2, -1);
    BOOST_CHECK_EQUAL(error3, -1);
    BOOST_CHECK_EQUAL(rcpt_number, block1->receiptsSize());
}
BOOST_AUTO_TEST_CASE(testEmptyBlockReceiptGetter)
{
    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto block1 = fakeBlock(crypto, blockFactory, 0, 0, 1);
    auto errorReceipt1 = bcos::ledger::blockReceiptListGetter(block1);
    BOOST_CHECK_EQUAL(errorReceipt1->size(), 0);

    auto errorReceipt2 = bcos::ledger::blockReceiptListGetter(nullptr);
    BOOST_CHECK_EQUAL(errorReceipt2, nullptr);
}

BOOST_AUTO_TEST_SUITE_END()
}  // namespace bcos::test
