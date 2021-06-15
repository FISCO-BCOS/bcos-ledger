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
#include "mock/MockKeyFactor.h"
#include "bcos-ledger/ledger/storage/StorageGetter.h"
#include "bcos-ledger/ledger/storage/StorageSetter.h"
#include <bcos-framework/testutils/TestPromptFixture.h>
#include <bcos-framework/interfaces/ledger/LedgerTypeDef.h>
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

BOOST_AUTO_TEST_CASE(testCreateTable)
{
    auto table = tableFactory->openTable(FS_ROOT);
    BOOST_CHECK_EQUAL(table->getRow(FS_KEY_TYPE)->getField(SYS_VALUE), "directory");
    BOOST_CHECK_EQUAL(table->getRow(FS_KEY_SUB)->getField(SYS_VALUE),
        "{\"subdirectories\":[{\"fileName\":\"usr\",\"type\":\"directory\"},{\"fileName\":\"bin\",\"type\":\"directory\"},{\"fileName\":\"data\",\"type\":\"directory\"}]}\n");

    table = tableFactory->openTable("/usr");
    BOOST_CHECK_EQUAL(table->getRow(FS_KEY_TYPE)->getField(SYS_VALUE), "directory");
    BOOST_CHECK_EQUAL(table->getRow(FS_KEY_SUB)->getField(SYS_VALUE), "{\"subdirectories\":[{\"fileName\":\"bin\",\"type\":\"directory\"},{\"fileName\":\"local\",\"type\":\"directory\"}]}\n");

    table = tableFactory->openTable("/bin");
    BOOST_CHECK_EQUAL(table->getRow(FS_KEY_TYPE)->getField(SYS_VALUE), "directory");
    BOOST_CHECK_EQUAL(table->getRow(FS_KEY_SUB)->getField(SYS_VALUE), "{\"subdirectories\":[{\"fileName\":\"extensions\",\"type\":\"directory\"}]}\n");
}
BOOST_AUTO_TEST_CASE(testTableSetterGetterByRowAndField)
{
    bool setterRet =
        storageSetter->syncTableSetter(tableFactory, SYS_HASH_2_NUMBER, "test", SYS_VALUE, "world");
    BOOST_CHECK(setterRet);

    storageGetter->asyncTableGetter(tableFactory, SYS_HASH_2_NUMBER, "test", SYS_VALUE,
        [&](Error::Ptr _error, std::shared_ptr<std::string> _ret) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(*_ret, "world");
        });
}
BOOST_AUTO_TEST_CASE(testErrorOpenTable)
{
    auto tableFactory = fakeErrorTableFactory();
    auto storageSetter = StorageSetter::storageSetterFactory();
    BOOST_CHECK_THROW(storageSetter->createTables(tableFactory), CreateSysTableFailed);
    BOOST_CHECK_THROW(
        storageSetter->syncTableSetter(tableFactory, "errorTable", "error", "error", ""),
                      OpenSysTableFailed);
    BOOST_CHECK_THROW(
        storageSetter->setConsensusConfig(tableFactory, CONSENSUS_SEALER, consensus::ConsensusNodeList(), "error"),
        OpenSysTableFailed);
    BOOST_CHECK_THROW(storageSetter->setSysConfig(tableFactory, "", "", ""), OpenSysTableFailed);

    auto storageGetter = StorageGetter::storageGetterFactory();
    storageGetter->asyncTableGetter(tableFactory, "errorTable", "row", "filed",
        [&](Error::Ptr _error, std::shared_ptr<std::string> _value) {
            BOOST_CHECK(_error->errorCode() == -1);
            BOOST_CHECK_EQUAL(_value, nullptr);
        });

    auto fakeHashList = std::make_shared<std::vector<std::string>>();
    storageGetter->getBatchTxByHashList(
        fakeHashList, tableFactory, nullptr, [&](Error::Ptr _error, TransactionsPtr _txs) {
            BOOST_CHECK(_error->errorCode() == -1);
            BOOST_CHECK_EQUAL(_txs, nullptr);
        });

    storageGetter->getBatchReceiptsByHashList(
        fakeHashList, tableFactory, nullptr, [&](Error::Ptr _error, ReceiptsPtr _receipts) {
            BOOST_CHECK(_error->errorCode() == -1);
            BOOST_CHECK_EQUAL(_receipts, nullptr);
        });

    storageGetter->getNoncesBatchFromStorage(0, 1, tableFactory, nullptr,
        [&](Error::Ptr _error,
            std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>> _nonce) {
            BOOST_CHECK(_error->errorCode() == -1);
            BOOST_CHECK_EQUAL(_nonce, nullptr);
        });

    storageGetter->getConsensusConfig("", 0, tableFactory, nullptr,
        [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodes) {
            BOOST_CHECK(_error->errorCode() == -1);
            BOOST_CHECK_EQUAL(_nodes, nullptr);
        });

    storageGetter->getSysConfig(
        "", tableFactory, [&](Error::Ptr _error, std::string _value, std::string _number) {
          BOOST_CHECK(_error->errorCode() == -1);
          BOOST_CHECK_EQUAL(_value, "");
          BOOST_CHECK_EQUAL(_number, "");
        });
}
BOOST_AUTO_TEST_CASE(testGetterSetter)
{
    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto block = fakeBlock(crypto, blockFactory, 10, 10, 1);

    auto number = block->blockHeader()->number();
    auto numberStr = boost::lexical_cast<std::string>(number);
    auto hash = block->blockHeader()->hash();
    auto hashStr = hash.hex();

    // SYS_CURRENT_STATE
    auto setCurrentStateRet = storageSetter->setCurrentState(tableFactory, "test", "test2");
    BOOST_CHECK(setCurrentStateRet);
    storageGetter->getCurrentState("test", tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getCurrentStateRet) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(*getCurrentStateRet, "test2");
        });

    // SYS_NUMBER_2_HEADER
    auto setNumber2HeaderRet = storageSetter->setNumber2Header(tableFactory, numberStr, "");
    BOOST_CHECK(setNumber2HeaderRet);
    storageGetter->getBlockHeaderFromStorage(number, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getBlockHeaderFromStorageRet) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(*getBlockHeaderFromStorageRet, "");
        });

    // SYS_NUMBER_2_TXS
    auto setNumber2TxsRet = storageSetter->setNumber2Txs(tableFactory, numberStr, "");
    BOOST_CHECK(setNumber2TxsRet);
    storageGetter->getTxsFromStorage(number, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getTxsFromStorageRet) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(*getTxsFromStorageRet, "");
        });

    // SYS_NUMBER_2_RECEIPTS
    auto setNumber2ReceiptsRet = storageSetter->setHashToReceipt(tableFactory, "txHash", "");
    BOOST_CHECK(setNumber2ReceiptsRet);
    storageGetter->getReceiptByTxHash("txHash", tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getReceiptsFromStorageRet) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(*getReceiptsFromStorageRet, "");
        });

    // SYS_HASH_2_NUMBER
    auto setHash2NumberRet = storageSetter->setHash2Number(tableFactory, hashStr, "");
    BOOST_CHECK(setHash2NumberRet);
    storageGetter->getBlockNumberByHash(hashStr, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getBlockNumberByHashRet) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(*getBlockNumberByHashRet, "");
        });

    // SYS_NUMBER_2_HASH
    auto setNumber2HashRet = storageSetter->setNumber2Hash(tableFactory, numberStr, "");
    BOOST_CHECK(setNumber2HashRet);
    storageGetter->getBlockHashByNumber(number, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getBlockHashByNumberRet) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(*getBlockHashByNumberRet, "");
        });

    // SYS_NUMBER_NONCES
    auto setNumber2NoncesRet = storageSetter->setNumber2Nonces(tableFactory, numberStr, "");
    BOOST_CHECK(setNumber2NoncesRet);

    storageGetter->getNoncesFromStorage(number, tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> getNoncesFromStorageRet) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(*getNoncesFromStorageRet, "");
        });

    // SYS_CONFIG
    auto setSysConfigRet = storageSetter->setSysConfig(tableFactory, "test", "test4", "0");
    BOOST_CHECK(setSysConfigRet);
    storageGetter->getSysConfig(
        "test", tableFactory, [&](Error::Ptr _error, std::string _value, std::string _number) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(_value, "test4");
            BOOST_CHECK_EQUAL(_number, "0");
        });

    // SYS_CONSENSUS
    auto signImpl = std::make_shared<Secp256k1SignatureImpl>();
    consensus::ConsensusNodeList consensusNodeList;
    consensus::ConsensusNodeList observerNodeList;
    for (int i = 0; i < 4; ++i)
    {
        auto node = std::make_shared<consensus::ConsensusNode>(
            signImpl->generateKeyPair()->publicKey(), 10 + i);
        consensusNodeList.emplace_back(node);
    }
    auto keyFactory = std::make_shared<MockKeyFactory>();
    auto setConsensusConfigRet =
        storageSetter->setConsensusConfig(tableFactory, CONSENSUS_SEALER, consensusNodeList, "0");
    BOOST_CHECK(setConsensusConfigRet);
    storageGetter->getConsensusConfig(
        CONSENSUS_SEALER, 0, tableFactory, keyFactory, [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
          BOOST_CHECK_EQUAL(_error, nullptr);
          BOOST_CHECK(!_nodeList->empty());
        });
}

BOOST_AUTO_TEST_SUITE_END()
}