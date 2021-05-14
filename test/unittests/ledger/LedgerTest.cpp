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
 * @file LedgerTest.cpp
 * @author: kyonRay
 * @date 2021-05-07
 */
#include "common/FakeTable.h"
#include "common/FakeBlock.h"
#include "../ledger/Ledger.h"
#include <bcos-crypto/signature/key/KeyFactoryImpl.h>
#include <bcos-crypto/signature/secp256k1/Secp256k1Crypto.h>
#include <bcos-test/libutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::crypto;

namespace bcos::test
{

class LedgerFixture : public TestPromptFixture{
public:
    LedgerFixture() :TestPromptFixture(){
        auto hashImpl = std::make_shared<Keccak256Hash>();
        m_storage = std::make_shared<MockStorage>();
        BOOST_TEST(m_storage != nullptr);
        m_storageGetter =  StorageGetter::storageGetterFactory();
        m_storageSetter = StorageSetter::storageSetterFactory();
        m_blockFactory = createBlockFactory(createCryptoSuite());
        auto keyFactor = std::make_shared<crypto::KeyFactoryImpl>();
        m_blockFactory->cryptoSuite()->setKeyFactory(keyFactor);

        m_headerFactory = std::make_shared<PBBlockHeaderFactory>(createCryptoSuite());
        BOOST_CHECK(m_blockFactory != nullptr);
        BOOST_CHECK(m_headerFactory != nullptr);
        m_ledger = std::make_shared<Ledger>(m_blockFactory, m_headerFactory, m_storage);
        BOOST_CHECK(m_ledger != nullptr);
    }
    ~LedgerFixture(){}

    inline void initFixture(){
        m_param = std::make_shared<LedgerConfig>();
        m_param->setBlockNumber(0);
        m_param->setHash(HashType(""));
        m_param->setBlockTxCountLimit(1000);
        m_param->setConsensusTimeout(1000);

        auto signImpl = std::make_shared<crypto::Secp256k1Crypto>();
        consensus::ConsensusNodeList consensusNodeList;
        consensus::ConsensusNodeList observerNodeList;
        for (int i = 0; i < 4; ++i)
        {
            auto node = std::make_shared<consensus::ConsensusNode>(signImpl->generateKeyPair()->publicKey(), 10 + i);
            consensusNodeList.emplace_back(node);
        }
        auto observer_node = std::make_shared<consensus::ConsensusNode>(signImpl->generateKeyPair()->publicKey(), -1);
        observerNodeList.emplace_back(observer_node);

        m_param->setConsensusNodeList(consensusNodeList);
        m_param->setObserverNodeList(observerNodeList);

        std::shared_ptr<TableFactory> tableFactory = getTableFactory(0);
        m_storageSetter->createTables(tableFactory);
        m_storage->addStateCache(0, nullptr, tableFactory);
        auto result = m_ledger->buildGenesisBlock(m_param);
        BOOST_CHECK(result);
    }

    inline void initEmptyFixture(){
        m_param = std::make_shared<LedgerConfig>();
        m_param->setBlockNumber(-1);
        m_param->setHash(HashType(""));
        m_param->setBlockTxCountLimit(-1);
        m_param->setConsensusTimeout(-1);

        consensus::ConsensusNodeList consensusNodeList;
        consensus::ConsensusNodeList observerNodeList;
        m_param->setConsensusNodeList(consensusNodeList);
        m_param->setObserverNodeList(observerNodeList);

        std::shared_ptr<TableFactory> tableFactory = getTableFactory(0);
        m_storageSetter->createTables(tableFactory);
        m_storage->addStateCache(0, nullptr, tableFactory);
        auto result = m_ledger->buildGenesisBlock(m_param);
        BOOST_CHECK(result);
    }

    inline void initBlocks(int _number) {
        m_fakeBlocks = fakeBlocks(m_blockFactory->cryptoSuite(), m_blockFactory, 1, 1, _number);
        for (int i = 0; i < _number; i++)
        {
            auto & block = m_fakeBlocks->at(i);
            block->blockHeader()->setNumber(1 + i);
        }
    }

    inline void initChain(int _number) {
        initBlocks(_number);
        for (int i = 0; i < _number; ++i)
        {
            auto table = getTableFactory(i + 1);
            m_storage->addStateCache(i + 1, m_fakeBlocks->at(i), table);
            m_ledger->asyncCommitBlock(i + 1, m_fakeBlocks->at(i)->blockHeader()->signatureList(),
                [&](Error::Ptr _error, LedgerConfig::Ptr) {
                    BOOST_CHECK_EQUAL(_error->errorCode(), 0);
//                    BOOST_CHECK_EQUAL(_config->blockNumber(), i+1);
                });
        }
    }
    inline TableFactory::Ptr getStateTable(const BlockNumber& _number)
    {
        auto hashImpl = std::make_shared<Keccak256Hash>();
        auto table = m_storage->getStateCache(_number);
        BOOST_CHECK(table != nullptr);
        return table;
    }
    inline TableFactory::Ptr getTableFactory(const BlockNumber& _number)
    {
        auto hashImpl = std::make_shared<Keccak256Hash>();
        auto table = std::make_shared<TableFactory>(m_storage, hashImpl, _number);
        BOOST_CHECK(table != nullptr);
        return table;
    }

    storage::StorageInterface::Ptr m_storage = nullptr;
    StorageSetter::Ptr m_storageSetter = nullptr;
    StorageGetter::Ptr m_storageGetter = nullptr;
    BlockFactory::Ptr m_blockFactory = nullptr;
    BlockHeaderFactory::Ptr m_headerFactory = nullptr;
    std::shared_ptr<Ledger> m_ledger = nullptr;
    LedgerConfig::Ptr m_param;
    BlocksPtr m_fakeBlocks;
};

BOOST_FIXTURE_TEST_SUITE(LedgerTest, LedgerFixture)

BOOST_AUTO_TEST_CASE(testFixtureLedger)
{
    initFixture();
    BlockNumber number = -1;
    m_ledger->asyncGetBlockNumber([&](Error::Ptr _error, BlockNumber _number) {
        BOOST_CHECK(_error == nullptr);
        number = _number;
    });
    BOOST_CHECK_EQUAL(number, 0);

    HashType hash = HashType("");
    m_ledger->asyncGetBlockHashByNumber(
        number, [&](Error::Ptr _error, const crypto::HashType _hash) {
            BOOST_CHECK(_error == nullptr);
            hash = _hash;
        });
    BOOST_CHECK(hash != HashType("") );

    m_ledger->asyncGetBlockNumberByHash(hash, [&](Error::Ptr _error, BlockNumber _number) {
        BOOST_CHECK(_error == nullptr);
        BOOST_CHECK_EQUAL(number, _number);
    });

    auto block = m_blockFactory->createBlock();
    m_ledger->asyncGetBlockDataByNumber(
        number, HEADER, [&](Error::Ptr _error, Block::Ptr _block) {
            BOOST_CHECK(_error == nullptr);
            block = _block;
        });
    BOOST_CHECK(block != nullptr);
    BOOST_CHECK_EQUAL(block->blockHeader()->number(), 0);

    m_ledger->asyncGetTotalTransactionCount(
        [&](Error::Ptr _error, int64_t _totalTxCount, int64_t _failedTxCount,
            protocol::BlockNumber _latestBlockNumber) {
            BOOST_CHECK(_error == nullptr);
            BOOST_CHECK_EQUAL(_totalTxCount, 0);
            BOOST_CHECK_EQUAL(_failedTxCount, 0);
            BOOST_CHECK_EQUAL(_latestBlockNumber, 0);
        });

    m_ledger->asyncGetSystemConfigByKey(
        SYSTEM_KEY_TX_COUNT_LIMIT, [&](Error::Ptr _error, std::string _value, BlockNumber _number) {
          BOOST_CHECK(_error == nullptr);
          BOOST_CHECK_EQUAL(_value, "1000");
          BOOST_CHECK_EQUAL(_number, 0);
        });

    auto nodeList = m_param->observerNodeList();
    m_ledger->asyncGetNodeListByType(
        CONSENSUS_OBSERVER, [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
          BOOST_CHECK(_error == nullptr);
          BOOST_CHECK_EQUAL(_nodeList->at(0)->nodeID()->hex(), nodeList.at(0)->nodeID()->hex());
        });
}

BOOST_AUTO_TEST_CASE(getBlockNumber)
{
    auto tableFactory = getTableFactory(0);
    m_storageSetter->createTables(tableFactory);
    m_storage->addStateCache(0, nullptr, tableFactory);
    m_storageSetter->setCurrentState(getStateTable(0), SYS_KEY_CURRENT_NUMBER, "-1");
    m_ledger->asyncGetBlockNumber(
        [&](Error::Ptr _error, BlockNumber _number) {
            BOOST_CHECK(_error != nullptr);
            BOOST_CHECK_EQUAL(_error->errorCode(), -1);
            BOOST_CHECK_EQUAL(_number, -1);
        });
}

BOOST_AUTO_TEST_CASE(getBlockHashByNumber)
{
    initFixture();
    m_ledger->asyncGetBlockHashByNumber(-1, [&](Error::Ptr _error, HashType _hash){
        BOOST_CHECK_EQUAL(_error->errorCode(), -1);
        BOOST_CHECK_EQUAL(_hash, HashType(""));
    });

    m_ledger->asyncGetBlockHashByNumber(1000, [&](Error::Ptr _error, HashType _hash) {
        BOOST_CHECK_EQUAL(_error->errorCode(), -1);
        BOOST_CHECK_EQUAL(_hash, HashType(""));
    });

    auto table = getStateTable(0);
    m_storageSetter->setNumber2Hash(getStateTable(0), "0", "");
    table->commit();
    m_ledger->asyncGetBlockHashByNumber(0, [&](Error::Ptr _error, HashType _hash) {
      BOOST_CHECK(_error != nullptr);
      BOOST_CHECK_EQUAL(_hash, HashType(""));
    });
}

BOOST_AUTO_TEST_CASE(getBlockNumberByHash)
{
    initFixture();
    auto hash = m_storageGetter->getBlockHashByNumber(0, getStateTable(0));

    m_ledger->asyncGetBlockNumberByHash(HashType(""), [&](Error::Ptr _error, BlockNumber _number){
      BOOST_CHECK_EQUAL(_error->errorCode(), -1);
      BOOST_CHECK_EQUAL(_number, -1);
    });

    auto table = getStateTable(0);
    m_storageSetter->setHash2Number(getStateTable(0), hash, "");
    table->commit();
    m_ledger->asyncGetBlockNumberByHash(HashType(hash), [&](Error::Ptr _error, BlockNumber _number) {
      BOOST_CHECK(_error != nullptr);
      BOOST_CHECK_EQUAL(_number, -1);
    });
}

BOOST_AUTO_TEST_CASE(getTotalTransactionCount){
    auto tableFactory = getTableFactory(0);
    m_storageSetter->createTables(tableFactory);
    m_storage->addStateCache(0, nullptr, tableFactory);
    m_storageSetter->setCurrentState(getStateTable(0), SYS_KEY_TOTAL_TRANSACTION_COUNT, "");
    m_storageSetter->setCurrentState(getStateTable(0), SYS_KEY_TOTAL_FAILED_TRANSACTION, "");
    tableFactory->commit();
    m_ledger->asyncGetTotalTransactionCount(
        [&](Error::Ptr _error, int64_t totalCount, int64_t totalFailed,
            bcos::protocol::BlockNumber _number) {
            BOOST_CHECK(_error != nullptr);
            BOOST_CHECK_EQUAL(totalCount, -1);
            BOOST_CHECK_EQUAL(totalFailed, -1);
            BOOST_CHECK_EQUAL(_number, -1);
        });
}

BOOST_AUTO_TEST_CASE(getNodeListByType)
{
    initEmptyFixture();
    m_ledger->asyncGetNodeListByType(
        "test", [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
          BOOST_CHECK(_error != nullptr);
          BOOST_CHECK_EQUAL(_nodeList, nullptr);
        });
    m_ledger->asyncGetNodeListByType(
        CONSENSUS_OBSERVER, [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
          BOOST_CHECK(_error != nullptr);
          BOOST_CHECK_EQUAL(_nodeList, nullptr);
        });
}
BOOST_AUTO_TEST_CASE(commit)
{
    initFixture();
    initChain(5);

    // test isBlockShouldCommit
    m_ledger->asyncCommitBlock(4, m_fakeBlocks->at(3)->blockHeader()->signatureList(),
        [&](Error::Ptr _error, LedgerConfig::Ptr) { BOOST_CHECK_EQUAL(_error->errorCode(), -1); });

    // null block in storage
    m_ledger->asyncCommitBlock(10, m_fakeBlocks->at(3)->blockHeader()->signatureList(),
        [&](Error::Ptr _error, LedgerConfig::Ptr) { BOOST_CHECK_EQUAL(_error->errorCode(), -1); });

    gsl::span<const Signature> sig;
    m_ledger->asyncCommitBlock(4, sig,
                               [&](Error::Ptr _error, LedgerConfig::Ptr) { BOOST_CHECK_EQUAL(_error->errorCode(), -1); });
}

BOOST_AUTO_TEST_CASE(getBlockDataByNumber)
{
    initFixture();
    // test cache
    initChain(20);

    // error number
    m_ledger->asyncGetBlockDataByNumber(1000, FULL_BLOCK, [&](Error::Ptr _error, Block::Ptr _block)
        {
            BOOST_CHECK_EQUAL(_error->errorCode(), -1);
            BOOST_CHECK_EQUAL(_block, nullptr);
        });

    // cache hit
    m_ledger->asyncGetBlockDataByNumber(15, FULL_BLOCK, [&](Error::Ptr _error, Block::Ptr _block)
    {
      BOOST_CHECK_EQUAL(_error, nullptr);
      BOOST_CHECK(_block->blockHeader()!= nullptr);
      BOOST_CHECK(_block->transactionsSize()!= 0);
      BOOST_CHECK(_block->receiptsSize()!=0);
    });

    // cache not hit
    m_ledger->asyncGetBlockDataByNumber(3, FULL_BLOCK, [&](Error::Ptr _error, Block::Ptr _block)
    {
      BOOST_CHECK_EQUAL(_error, nullptr);
      BOOST_CHECK(_block->blockHeader()!= nullptr);
      BOOST_CHECK(_block->transactionsSize()!= 0);
      BOOST_CHECK(_block->receiptsSize()!=0);
    });

    m_ledger->asyncGetBlockDataByNumber(0, TRANSACTIONS, [&](Error::Ptr _error, Block::Ptr _block)
    {
      BOOST_CHECK_EQUAL(_error->errorCode(), -1);
      BOOST_CHECK_EQUAL(_block, nullptr);
    });
    m_ledger->asyncGetBlockDataByNumber(0, RECEIPTS, [&](Error::Ptr _error, Block::Ptr _block)
    {
      BOOST_CHECK_EQUAL(_error->errorCode(), -1);
      BOOST_CHECK_EQUAL(_block, nullptr);
    });
}

BOOST_AUTO_TEST_CASE(getTransactionByHash)
{
    initFixture();
    initChain(5);

    m_ledger->asyncGetTransactionByHash(m_fakeBlocks->at(3)->transactionHash(0), true,
        [&](Error::Ptr _error, Transaction::ConstPtr _tx, MerkleProofPtr _merkle) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(_tx->hash().hex(), m_fakeBlocks->at(3)->transaction(0)->hash().hex());
          BOOST_CHECK(_merkle != nullptr);
        });
}

BOOST_AUTO_TEST_CASE(getTransactionReceiptByHash)
{
    initFixture();
    initChain(5);

    m_ledger->asyncGetTransactionReceiptByHash(m_fakeBlocks->at(3)->transactionHash(0), true,
        [&](Error::Ptr _error, TransactionReceipt::ConstPtr _receipt, MerkleProofPtr _proof) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(
                _receipt->hash().hex(), m_fakeBlocks->at(3)->receipt(0)->hash().hex());
            BOOST_CHECK(_proof != nullptr);
        });

    // size == 0
    auto table = getTableFactory(6);
    auto block = fakeBlock(m_blockFactory->cryptoSuite(), m_blockFactory, 0, 0);
    m_storage->addStateCache(6, block, table);
    m_ledger->asyncCommitBlock(6, block->blockHeader()->signatureList(),
        [&](Error::Ptr _error, LedgerConfig::Ptr) { BOOST_CHECK_EQUAL(_error->errorCode(), 0); });
    m_ledger->asyncGetTransactionReceiptByHash(block->blockHeader()->hash(), false,
        [&](Error::Ptr _error, TransactionReceipt::ConstPtr _receipt, MerkleProofPtr _proof) {
            BOOST_CHECK_EQUAL(_error->errorCode(), -1);
            BOOST_CHECK_EQUAL(_receipt, nullptr);
            BOOST_CHECK(_proof == nullptr);
        });
}

BOOST_AUTO_TEST_CASE(getTransactionByBlockNumberAndIndex){
    initFixture();
    initChain(5);
    m_ledger->asyncGetTransactionByBlockNumberAndIndex(
        3, 0, true, [&](Error::Ptr _error, Transaction::ConstPtr _tx, MerkleProofPtr _proof) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(_tx->hash().hex(), m_fakeBlocks->at(2)->transaction(0)->hash().hex());
            BOOST_CHECK(_proof != nullptr);
        });
    // without proof
    m_ledger->asyncGetTransactionByBlockNumberAndIndex(
        3, 0, false, [&](Error::Ptr _error, Transaction::ConstPtr _tx, MerkleProofPtr _proof) {
          BOOST_CHECK_EQUAL(_error, nullptr);
          BOOST_CHECK_EQUAL(_tx->hash().hex(), m_fakeBlocks->at(2)->transaction(0)->hash().hex());
          BOOST_CHECK(_proof == nullptr);
        });

    // error param
    m_ledger->asyncGetTransactionByBlockNumberAndIndex(
        -1, -1, true, [&](Error::Ptr _error, Transaction::ConstPtr _tx, MerkleProofPtr _proof) {
          BOOST_CHECK_EQUAL(_error->errorCode(), -1);
          BOOST_CHECK_EQUAL(_tx, nullptr);
          BOOST_CHECK(_proof == nullptr);
        });
}

BOOST_AUTO_TEST_CASE(getReceiptByBlockNumberAndIndex){
    initFixture();
    initChain(5);
    m_ledger->asyncGetReceiptByBlockNumberAndIndex(3, 0, true,
        [&](Error::Ptr _error, TransactionReceipt::ConstPtr _receipt, MerkleProofPtr _proof) {
          BOOST_CHECK_EQUAL(_error, nullptr);
          BOOST_CHECK_EQUAL(_receipt->hash().hex(), m_fakeBlocks->at(2)->receipt(0)->hash().hex());
          BOOST_CHECK(_proof != nullptr);
        });

    // without proof
    m_ledger->asyncGetReceiptByBlockNumberAndIndex(3, 0, false,
        [&](Error::Ptr _error, TransactionReceipt::ConstPtr _receipt, MerkleProofPtr _proof) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(
                _receipt->hash().hex(), m_fakeBlocks->at(2)->receipt(0)->hash().hex());
            BOOST_CHECK(_proof == nullptr);
        });
    // error param
    m_ledger->asyncGetReceiptByBlockNumberAndIndex(-1, -1, false,
        [&](Error::Ptr _error, TransactionReceipt::ConstPtr _receipt, MerkleProofPtr _proof) {
            BOOST_CHECK_EQUAL(_error->errorCode(), -1);
            BOOST_CHECK_EQUAL(_receipt, nullptr);
            BOOST_CHECK(_proof == nullptr);
        });
}

BOOST_AUTO_TEST_CASE(getNonceList) {
    initFixture();
    initChain(5);
    m_ledger->asyncGetNonceList(3, 5,
        [&](Error::Ptr _error,
            std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>> _nonceMap) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK(_nonceMap != nullptr);
            BOOST_CHECK_EQUAL(_nonceMap->size(), 3);
        });
}

BOOST_AUTO_TEST_CASE(preStoreTransaction)
{
    initFixture();
    initBlocks(5);
    bytesPointer tx = std::make_shared<bytes>();
    auto hash = m_fakeBlocks->at(3)->transaction(0)->hash();
    m_fakeBlocks->at(3)->transaction(0)->encode(*tx);
    m_ledger->asyncPreStoreTransaction(
        tx, hash, [&](Error::Ptr _error) { BOOST_CHECK_EQUAL(_error->errorCode(), 0); });

    // null tx
    m_ledger->asyncPreStoreTransaction(
        nullptr, hash, [&](Error::Ptr _error) { BOOST_CHECK_EQUAL(_error->errorCode(), -1); });

    // null hash
    m_ledger->asyncPreStoreTransaction(
        tx, HashType(""), [&](Error::Ptr _error) { BOOST_CHECK_EQUAL(_error->errorCode(), -1); });
}

BOOST_AUTO_TEST_SUITE_END()
}
