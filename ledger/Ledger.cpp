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
 * @file Ledger.cpp
 * @author: kyonRay
 * @date 2021-04-13
 */

#include "Ledger.h"
#include "utilities/MerkleProofUtility.h"
#include "utilities/BlockUtilities.h"
#include <bcos-framework/libprotocol/ParallelMerkleProof.h>
#include <boost/lexical_cast.hpp>
#include <tbb/parallel_invoke.h>
#include <tbb/parallel_for.h>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::crypto;

void Ledger::asyncCommitBlock(bcos::protocol::BlockNumber _blockNumber,
    const gsl::span<const protocol::Signature>& _signList,
    std::function<void(Error::Ptr, LedgerConfig::Ptr)> _onCommitBlock)
{
    auto start_time = utcTime();
    auto record_time = utcTime();
    if (!isBlockShouldCommit(_blockNumber))
    {
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "error number");
        _onCommitBlock(error, nullptr);
        return;
    }
    // TODO: check parentHash
    auto parentHash = HashType(getLatestBlockHash());

    // get block from storage cache
    auto block = getState()->getBlock(_blockNumber);

    if (block == nullptr)
    {
        LEDGER_LOG(FATAL) << LOG_DESC("Get block null in storage cache")
                            << LOG_KV("number", _blockNumber);
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onCommitBlock(error, nullptr);
        return;
    }

    // if empty then sync call commitBlock
    if (!_signList.empty())
    {
        block->blockHeader()->setSignatureList(_signList);
    }
    else
    {
        LEDGER_LOG(WARNING) << LOG_DESC("Commit block without signature list")
                            << LOG_KV("number", _blockNumber);
    }

    auto ledgerConfig = getLedgerConfig(_blockNumber, block->blockHeader()->hash());
    try
    {
        auto before_write_time_cost = utcTime() - record_time;
        record_time = utcTime();
        {
            // std::lock_guard<std::mutex> l(commitMutex);
            auto write_record_time = utcTime();

            TableFactoryInterface::Ptr tableFactory = getState()->getStateCache(_blockNumber);

            // FIXME: writeNumber2Transactions is not necessary
            tbb::parallel_invoke(
                [this, _blockNumber, tableFactory]() { writeNumber(_blockNumber, tableFactory); },
                [this, block, tableFactory]() { writeTotalTransactionCount(block, tableFactory); },
                [this, block, tableFactory]() { writeTxToBlock(block, tableFactory); },
                [this, block, tableFactory]() { writeNoncesToBlock(block, tableFactory); },
                [this, block, tableFactory]() { writeHash2Number(block, tableFactory); },
                [this, block, tableFactory]() { writeNumber2BlockHeader(block, tableFactory); },
                [this, block, _blockNumber, tableFactory]() {writeNumber2Transactions(block, _blockNumber, tableFactory);},
                [this, block, _blockNumber, tableFactory]() {writeNumber2Receipts(block, _blockNumber, tableFactory);});

            auto write_table_time = utcTime() - write_record_time;

            write_record_time = utcTime();

            auto ret = tableFactory->commit();
            if(ret == 0)
            {
                LEDGER_LOG(ERROR) << LOG_DESC("Commit Block failed")
                                  << LOG_KV("number", _blockNumber);
                // TODO: add error code and error msg
                auto error = std::make_shared<Error>(-1, "");
                _onCommitBlock(error, nullptr);
                return;
            }

            auto dbCommit_time_cost = utcTime() - write_record_time;
            write_record_time = utcTime();
            {
                WriteGuard ll(m_blockNumberMutex);
                m_blockNumber = _blockNumber;
            }
            auto updateBlockNumber_time_cost = utcTime() - write_record_time;
            LEDGER_LOG(DEBUG) << LOG_BADGE("Commit") << LOG_DESC("Commit block time record(write)")
                              << LOG_KV("writeTableTime", write_table_time)
                              << LOG_KV("dbCommitTimeCost", dbCommit_time_cost)
                              << LOG_KV("updateBlockNumberTimeCost", updateBlockNumber_time_cost);
        }
        auto writeBlock_time_cost = utcTime() - record_time;
        record_time = utcTime();

        m_blockCache.add(_blockNumber, block);
        m_blockHeaderCache.add(_blockNumber, block->blockHeader());
        m_transactionsCache.add(_blockNumber, blockTransactionListGetter(block));
        m_receiptCache.add(_blockNumber, blockReceiptListGetter(block));

        auto addBlockCache_time_cost = utcTime() - record_time;
        record_time = utcTime();
        // TODO: push msg to tx pool
        // TODO: broadcast msg to module
        auto noteReady_time_cost = utcTime() - record_time;

        LEDGER_LOG(DEBUG) << LOG_BADGE("Commit") << LOG_DESC("Commit block time record")
                          << LOG_KV("beforeTimeCost", before_write_time_cost)
                          << LOG_KV("writeBlockTimeCost", writeBlock_time_cost)
                          << LOG_KV("addBlockCacheTimeCost", addBlockCache_time_cost)
                          << LOG_KV("noteReadyTimeCost", noteReady_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    catch (OpenSysTableFailed const& e)
    {
        LEDGER_LOG(FATAL)
            << LOG_DESC("[commitBlock]System meets error when try to write block to storage")
            << LOG_KV("EINFO", boost::diagnostic_information(e));
        raise(SIGTERM);
        BOOST_THROW_EXCEPTION(
            OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
    }

    // TODO: add success code and msg
    auto success = std::make_shared<Error>(0, "");
    _onCommitBlock(success, ledgerConfig);
}

void Ledger::asyncPreStoreTransaction(
    bytesConstRef _txToStore, const crypto::HashType& _txHash,
    std::function<void(Error::Ptr)> _onTxStored)
{
    if (_txHash == HashType(""))
    {
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onTxStored(error);
        return;
    }
    auto number = getLatestBlockNumber();
    auto start_time = utcTime();
    try
    {
        auto write_record_time = utcTime();
        TableFactoryInterface::Ptr tableFactory = getState()->getStateCache(number);
        getStorageSetter()->setHashToTx(tableFactory, _txHash.hex(), asString(_txToStore));

        auto ret = tableFactory->commit();
        if(ret == 0)
        {
            LEDGER_LOG(ERROR) << LOG_DESC("PreStoreTx failed")
                              << LOG_KV("txHash", _txHash);
            // TODO: add error code and error msg
            auto error = std::make_shared<Error>(-1, "");
            _onTxStored(error);
            return;
        }

        auto write_table_time = utcTime() - write_record_time;
        LEDGER_LOG(DEBUG) << LOG_BADGE("PreStoreTx") << LOG_DESC("PreStore Txs time record")
                          << LOG_KV("writeTableTime", write_table_time)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    catch (OpenSysTableFailed const& e)
    {
        LEDGER_LOG(FATAL)
                << LOG_DESC("[#asyncPreStoreTransaction]System meets error when try to write block to storage")
                << LOG_KV("EINFO", boost::diagnostic_information(e));
        raise(SIGTERM);
        BOOST_THROW_EXCEPTION(
            OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
    }
    // TODO: add success code and msg
    auto success = std::make_shared<Error>(0, "");
    _onTxStored(success);
}

void Ledger::asyncGetBlockDataByNumber(bcos::protocol::BlockNumber _blockNumber, int32_t _blockFlag,
    std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
{
    auto currentNum = getLatestBlockNumber();
    if (_blockNumber > currentNum)
    {
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
        return;
    }
    auto block = getBlock(_blockNumber, _blockFlag);
    if (block)
    {
        _onGetBlock(nullptr, block);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetBlockByNumber]Can't find block, return nullptr")
                          << LOG_KV("blockNumber", _blockNumber);

        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
    }
}

void Ledger::asyncGetBlockNumber(
    std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock)
{
    auto blockNumber = getLatestBlockNumber();
    if (blockNumber == -1)
    {
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, -1);
        return;
    }
    _onGetBlock(nullptr, blockNumber);
}

void Ledger::asyncGetBlockHashByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, const bcos::crypto::HashType&)> _onGetBlock)
{
    if (_blockNumber < 0 || _blockNumber > getLatestBlockNumber())
    {
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, HashType(""));
        return;
    }
    auto hashStr = getStorageGetter()->getBlockHashByNumber(_blockNumber, getMemoryTableFactory(0));
    if (!hashStr.empty())
    {
        _onGetBlock(nullptr, HashType(hashStr));
        return;
    }
    LEDGER_LOG(ERROR)
        << LOG_DESC("[#asyncGetBlockHashByNumber] error happened in open table or get entry")
        << LOG_KV("blockNumber", _blockNumber);

    // TODO: add error code and msg
    auto error = std::make_shared<Error>(-1, "");
    _onGetBlock(error, HashType(""));
}

void Ledger::asyncGetBlockNumberByHash(const crypto::HashType& _blockHash,
    std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock)
{
    if (_blockHash == HashType(""))
    {
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, -1);
        return;
    }
    auto numberStr =
        getStorageGetter()->getBlockNumberByHash(_blockHash.hex(), getMemoryTableFactory(0));
    if (!numberStr.empty())
    {
        _onGetBlock(nullptr, boost::lexical_cast<BlockNumber>(numberStr));
        return;
    }
    LEDGER_LOG(ERROR)
        << LOG_DESC("[#asyncGetBlockHashByNumber] error happened in open table or get entry")
        << LOG_KV("blockHash", _blockHash.hex());

    // TODO: add error code and msg
    auto error = std::make_shared<Error>(-1, "");
    _onGetBlock(error, -1);
}

// FIXME: use pointer to map to return in callback
void Ledger::asyncGetBatchTxsByHashList(crypto::HashListPtr _txHashList, bool _withProof,
    std::function<void(Error::Ptr, std::shared_ptr<std::vector<bytesPointer>>,
        std::map<std::string, MerkleProofPtr>)>
        _onGetTx)
{
    // use concurrent vector
    auto con_txByteList = std::make_shared<tbb::concurrent_vector<bytesPointer>>();
    // use concurrent map
    tbb::concurrent_unordered_map<std::string, MerkleProofPtr> con_proofMap;

    tbb::parallel_for(tbb::blocked_range<size_t>(0, _txHashList->size()),
        [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end() ; ++i)
            {
                auto txHash = _txHashList->at(i);
                auto txData = getStorageGetter()->getTxByTxHash(txHash.hex(), getMemoryTableFactory(0));
                auto txPointer = std::make_shared<bytes>(asBytes(txData));
                con_txByteList->emplace_back(txPointer);
                if(_withProof){
                    auto proof = getTxProof(txHash);
                    con_proofMap.emplace(std::make_pair(txHash.hex(), proof));
                }
            }
        });
    auto txByteList =
        std::make_shared<std::vector<bytesPointer>>(con_txByteList->begin(), con_txByteList->end());
    std::map<std::string, MerkleProofPtr> proofMap(con_proofMap.begin(), con_proofMap.end()), emptyMap;
    if(_txHashList->size() != txByteList->size()){
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetTx(error, nullptr, emptyMap);
        return;
    }
    if (_withProof)
    {
        if (proofMap.size() != _txHashList->size())
        {
            // TODO: add error code and msg
            auto error = std::make_shared<Error>(-1, "");
            _onGetTx(error, nullptr, emptyMap);
        }
        else
        {
            LEDGER_LOG(INFO) << LOG_DESC("") << LOG_KV("txHashListSize", _txHashList->size())
                             << LOG_KV("withProof", _withProof);
            _onGetTx(nullptr, txByteList, proofMap);
        }
    }
    else
    {
        LEDGER_LOG(INFO) << LOG_DESC("") << LOG_KV("txHashListSize", _txHashList->size())
                          << LOG_KV("withProof", _withProof);
        _onGetTx(nullptr, txByteList, emptyMap);
    }
}

void Ledger::asyncGetTransactionReceiptByHash(bcos::crypto::HashType const& _txHash,
    bool _withProof,
    std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)>
        _onGetTx)
{
    auto numIndexPair =
        getStorageGetter()->getBlockNumberAndIndexByHash(_txHash.hex(), getMemoryTableFactory(0));
    if (numIndexPair && !numIndexPair->first.empty() && !numIndexPair->second.empty())
    {
        auto blockNumber = boost::lexical_cast<BlockNumber>(numIndexPair->first);
        auto index = boost::lexical_cast<uint>(numIndexPair->second);
        auto receipts = getReceipts(blockNumber);
        if (receipts->size() > index)
        {
            if (_withProof)
            {
                auto merkleProof = std::make_shared<MerkleProof>();
                auto receipt = receipts->at(index);
                auto parent2ChildList =
                    getParent2ChildListByReceiptProofCache(blockNumber, receipts);
                auto child2Parent = getChild2ParentCacheByReceipt(parent2ChildList, blockNumber);
                getMerkleProof(receipt->hash(), *parent2ChildList, *child2Parent, *merkleProof);
                _onGetTx(nullptr, receipts->at(index), merkleProof);
                return;
            }
            else
            {
                _onGetTx(nullptr, receipts->at(index), nullptr);
                return;
            }
        }
    }
    LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("txHash", _txHash);
    // TODO: add error code and message
    auto error = std::make_shared<Error>(-1, "");
    _onGetTx(error, nullptr, nullptr);
}

void Ledger::asyncGetTransactionByBlockNumberAndIndex(protocol::BlockNumber _blockNumber,
    int64_t _index, bool _withProof,
    std::function<void(Error::Ptr, protocol::Transaction::ConstPtr, MerkleProofPtr)> _onGetTx)
{
    if (_blockNumber < 0 || _index < 0 || _blockNumber > getLatestBlockNumber())
    {
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetTx(error, nullptr, nullptr);
        return;
    }
    auto txs = getTxs(_blockNumber);
    if(_index > (int64_t)txs->size()){
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetTx(error, nullptr, nullptr);
        return;
    }
    else{
        if(_withProof){
            auto merkleProof = std::make_shared<MerkleProof>();
            auto tx = txs->at(_index);
            auto parent2ChildList = getParent2ChildListByTxsProofCache(_blockNumber, txs);
            auto child2Parent = getChild2ParentCacheByTransaction(parent2ChildList, _blockNumber);
            getMerkleProof(tx->hash(), *parent2ChildList, *child2Parent, *merkleProof);

            // TODO: full judge merkle proof
            if (merkleProof)
            {
                _onGetTx(nullptr, (*txs)[_index], merkleProof);
                return;
            }
        }
        else
        {
            _onGetTx(nullptr, (*txs)[_index], nullptr);
            return;
        }
    }
}

void Ledger::asyncGetReceiptByBlockNumberAndIndex(protocol::BlockNumber _blockNumber,
    int64_t _index, bool _withProof,
    std::function<void(Error::Ptr, protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)>
        _onGetTx)
{
    if (_blockNumber < 0 || _index < 0 || _blockNumber > getLatestBlockNumber())
    {
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetTx(error, nullptr, nullptr);
        return;
    }
    auto receipts = getReceipts(_blockNumber);

    if(_index > (int64_t)receipts->size()){
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetTx(error, nullptr, nullptr);
        return;
    }
    else{
        if(_withProof){
            auto merkleProof = std::make_shared<MerkleProof>();
            auto receipt = receipts->at(_index);
            auto parent2ChildList = getParent2ChildListByReceiptProofCache(_blockNumber, receipts);
            auto child2Parent = getChild2ParentCacheByReceipt(parent2ChildList, _blockNumber);
            getMerkleProof(receipt->hash(), *parent2ChildList, *child2Parent, *merkleProof);

            // TODO: full judge merkle proof
            if (merkleProof)
            {
                _onGetTx(nullptr, (*receipts)[_index], merkleProof);
                return;
            }
        }
        else
        {
            _onGetTx(nullptr, (*receipts)[_index], nullptr);
            return;
        }
    }
}

void Ledger::asyncGetTotalTransactionCount(
    std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
{
    auto totalCountStr = getStorageGetter()->getCurrentState(
        SYS_KEY_TOTAL_TRANSACTION_COUNT, getMemoryTableFactory(0));
    auto totalFailedStr = getStorageGetter()->getCurrentState(
        SYS_KEY_TOTAL_FAILED_TRANSACTION, getMemoryTableFactory(0));
    if(!totalCountStr.empty() && !totalFailedStr.empty()){
        auto totalCount = boost::lexical_cast<int64_t>(totalCountStr);
        auto totalFailed = boost::lexical_cast<int64_t>(totalFailedStr);
        _callback(nullptr, totalCount, totalFailed, getLatestBlockNumber());
        return;
    }
    LEDGER_LOG(ERROR)
            << LOG_DESC(
                "[#asyncGetTotalTransactionCount] error happened in get data");
    // TODO: add error code and msg
    auto error = std::make_shared<Error>(-1, "");
    _callback(error, -1, -1, -1);
}

void Ledger::asyncGetSystemConfigByKey(const std::string& _key,
    std::function<void(Error::Ptr, std::string, bcos::protocol::BlockNumber)> _onGetConfig)
{
    auto currentNumber = getLatestBlockNumber();
    // FIXME: async deadlock

    UpgradableGuard l(m_ledgerConfigMutex);
    auto it = m_ledgerConfigMap.find(_key);
    if (it != m_ledgerConfigMap.end() && it->second.curBlockNum == currentNumber)
    {
        // get value from cache
        // TODO: add success code and msg
        l.unlock();
        auto success = std::make_shared<Error>(0, "");
        _onGetConfig(success, it->second.value, it->second.enableNumber);
        return;
    }

    // cannot find the system config key or need to update the value with different block height
    // get value from db

    auto ret = getStorageGetter()->getSysConfig(_key, getMemoryTableFactory(0));
    if (ret->second.empty())
    {
        LEDGER_LOG(ERROR) << LOG_DESC("[#asyncGetSystemConfigByKey] Null pointer of getSysConfig")
                          << LOG_KV("key", _key);
        // TODO: add error code and error msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetConfig(error, "", -1);
        return;
    }

        auto number = boost::lexical_cast<BlockNumber>(ret->second);
        // update cache
        {
            UpgradeGuard ul(l);
            LedgerConfigCache ledgerConfigCache(ret->first, nullptr, number, currentNumber);
            if (it != m_ledgerConfigMap.end())
            {
                it->second = ledgerConfigCache;
            }
            else
            {
                m_ledgerConfigMap.insert(
                    std::pair<std::string, LedgerConfigCache>(_key, ledgerConfigCache));
            }
        }


    LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetSystemConfigByKey]Data in db") << LOG_KV("key", _key)
                      << LOG_KV("value", ret->first);
    l.unlock();
    // TODO: add success code and msg
    auto success = std::make_shared<Error>(0, "");
    _onGetConfig(success, ret->first, boost::lexical_cast<BlockNumber>(ret->second));
}

void Ledger::asyncGetNonceList(bcos::protocol::BlockNumber _startNumber, int64_t _offset,
    std::function<void(Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
        _onGetList)
{
    auto latestNumber = getLatestBlockNumber();
    if (_startNumber < 0 || _offset < 0 || _startNumber > latestNumber)
    {
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetList(error, nullptr);
        return;
    }
    auto endNumber =
        (_startNumber + _offset > latestNumber) ? latestNumber : (_startNumber + _offset);
    auto tableFactory = getMemoryTableFactory(0);
    auto retMap = std::make_shared<std::map<protocol::BlockNumber, protocol::NonceListPtr>>();
    auto block = m_blockFactory->createBlock();

    for (BlockNumber i = _startNumber; i <= endNumber; ++i)
    {
        auto noncesStr = getStorageGetter()->getNoncesFromStorage(i, tableFactory);
        if (!noncesStr.empty())
        {
            block->decode(noncesStr, false, false);
            auto nonceList = std::make_shared<protocol::NonceList>(block->nonceList());
            retMap->emplace(std::make_pair(i, nonceList));
        }
        else{
            LEDGER_LOG(ERROR)
                    << LOG_DESC("[#asyncGetBlockHashByNumber] error happened in open table or get entry")
                    << LOG_KV("blockNumber", i);
            // TODO: add error code and msg
            auto error = std::make_shared<Error>(-1, "");
            _onGetList(error, nullptr);
            return;
        }
    }

    if (retMap->size() == size_t(endNumber - _startNumber + 1))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetBlockHashByNumber] get nonceList enough")
                          << LOG_KV("listSize", endNumber - _startNumber + 1);
        _onGetList(nullptr, retMap);
    } else {
        LEDGER_LOG(ERROR) << LOG_DESC("[#asyncGetBlockHashByNumber] not get enough nonceLists")
                          << LOG_KV("startBlockNumber", _startNumber)
                          << LOG_KV("endBlockNumber", endNumber);
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetList(error, nullptr);
    }
}

void Ledger::asyncGetNodeListByType(const std::string& _type,
    std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig)
{
    if (_type != CONSENSUS_SEALER && _type != CONSENSUS_OBSERVER)
    {
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetConfig(error, nullptr);
        return;
    }
    auto number = getLatestBlockNumber();
    UpgradableGuard l(m_ledgerConfigMutex);
    auto it = m_ledgerConfigMap.find(_type);
    if (it != m_ledgerConfigMap.end() && it->second.curBlockNum == number)
    {
        // get value from cache
        // TODO: add success code and msg
        l.unlock();
        auto success = std::make_shared<Error>(0, "");
        _onGetConfig(success, it->second.nodeList);
        return;
    }

    consensus::ConsensusNodeListPtr nodeList = getStorageGetter()->getConsensusConfig(
        _type, number, getMemoryTableFactory(0), m_blockFactory->cryptoSuite()->keyFactory());
    if (nodeList == nullptr || nodeList->empty())
    {
        LEDGER_LOG(ERROR)
            << LOG_DESC("[#asyncGetNodeListByType] error happened in open table or get entry")
            << LOG_KV("blockNumber", number);

        // TODO: add error code and msg
        l.unlock();
        auto error = std::make_shared<Error>(-1, "");
        _onGetConfig(error, nullptr);
        return;
    }
    {
        UpgradeGuard ul(l);
        LedgerConfigCache ledgerConfigCache(_type, nodeList, number, number);
        if (it != m_ledgerConfigMap.end())
        {
            it->second = ledgerConfigCache;
        }
        else
        {
            m_ledgerConfigMap.insert(
                std::pair<std::string, LedgerConfigCache>(_type, ledgerConfigCache));
        }
    }
    l.unlock();
    // TODO: add success code and msg
    auto success = std::make_shared<Error>(0, "");
    _onGetConfig(success, nodeList);
}

Block::Ptr Ledger::getBlock(const BlockNumber& _blockNumber, int32_t _blockFlag)
{
    if (_blockNumber > getLatestBlockNumber())
    {
        return nullptr;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Cache hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return cachedBlock.second;
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        auto block = m_blockFactory->createBlock();
        if(_blockFlag & HEADER)
        {
            auto header = getBlockHeader(_blockNumber);
            if(header){
                block->setBlockHeader(header);
            }
            else{
                LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Can't find the header")
                                  << LOG_KV("blockNumber", _blockNumber);
                return nullptr;
            }
        }
        if(_blockFlag & TRANSACTIONS)
        {
            auto txs = getTxs(_blockNumber);
            if(txs){
                if(txs->size() != blockTransactionListSetter(block, txs)){
                    LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock] insert block transactions error")
                                      << LOG_KV("blockNumber", _blockNumber);
                }
            }
            else{
                LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Can't find the Txs")
                                  << LOG_KV("blockNumber", _blockNumber);
                return nullptr;
            }
        }
        if(_blockFlag & RECEIPTS)
        {
            auto receipts = getReceipts(_blockNumber);
            if(receipts){
                if(receipts->size() != blockReceiptListSetter(block, receipts)){
                    LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock] insert block receipts error")
                                      << LOG_KV("blockNumber", _blockNumber);
                }
            }else{
                LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Can't find the Txs")
                                  << LOG_KV("blockNumber", _blockNumber);
                return nullptr;
            }
        }
        if(!(_blockFlag ^ FULL_BLOCK)){
            // get full block data
            LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Write to cache");
            auto blockPtr = m_blockCache.add(_blockNumber, block);
            return blockPtr;
        }
        auto assemble_block = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_DESC("Get block from db")
                          << LOG_KV("getCacheTimeCost", getCache_time_cost)
                          << LOG_KV("constructBlockTimeCost", assemble_block)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
        return block;
    }
}

BlockNumber Ledger::getLatestBlockNumber()
{
    UpgradableGuard ul(m_blockNumberMutex);
    if (m_blockNumber == -1)
    {
        BlockNumber num = getNumberFromStorage();
        UpgradeGuard l(ul);
        m_blockNumber = num;
    }
    return m_blockNumber;
}

BlockNumber Ledger::getNumberFromStorage()
{
    BlockNumber num = -1;
    std::string currentNumber =
        getStorageGetter()->getCurrentState(SYS_KEY_CURRENT_NUMBER, getMemoryTableFactory(0));
    if(!currentNumber.empty()){
        num = boost::lexical_cast<BlockNumber>(currentNumber);
    }
    return num;
}

std::string Ledger::getLatestBlockHash(){
    auto number = getLatestBlockNumber();
    return getStorageGetter()->getBlockHashByNumber(number, getMemoryTableFactory(number));
}

BlockHeader::Ptr Ledger::getBlockHeader(const bcos::protocol::BlockNumber& _blockNumber)
{
    if (_blockNumber < 0 || _blockNumber > getLatestBlockNumber())
    {
        return nullptr;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedHeader = m_blockHeaderCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second) && bool( cachedBlock.second->blockHeader()))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]CacheBlock hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return cachedBlock.second->blockHeader();
    }
    else if (bool(cachedHeader.second))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]CacheHeader hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return cachedHeader.second;
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        auto headerStr =
            getStorageGetter()->getBlockHeaderFromStorage(_blockNumber, getMemoryTableFactory(0));
        auto storage_getter_time = utcTime() - record_time;
        record_time = utcTime();
        if(!headerStr.empty()){
            auto headerPtr = decodeBlockHeader(headerStr);

            auto decode_header_time_cost = utcTime() - record_time;
            record_time = utcTime();

            LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]Write to cache");
            auto header = m_blockHeaderCache.add(_blockNumber, headerPtr);
            auto addCache_time_cost = utcTime() - record_time;
            LEDGER_LOG(DEBUG) << LOG_DESC("Get Txs from db")
                              << LOG_KV("getCacheTimeCost", getCache_time_cost)
                              << LOG_KV("storageGetterTimeCost", storage_getter_time)
                              << LOG_KV("decodeTimeCost", decode_header_time_cost)
                              << LOG_KV("addCacheTimeCost", addCache_time_cost)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
            return header;
        }
        LEDGER_LOG(ERROR) << LOG_DESC("[#getBlockHeader]Can't find header, return nullptr");
        return nullptr;
    }
}

bcos::protocol::TransactionsPtr Ledger::getTxs(const bcos::protocol::BlockNumber& _blockNumber)
{
    if (_blockNumber < 0 || _blockNumber > getLatestBlockNumber())
    {
        return nullptr;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedTransactions = m_transactionsCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second) && cachedBlock.second->transactionsSize() != 0)
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]CacheBlock hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return blockTransactionListGetter(cachedBlock.second);
    }
    else if (bool(cachedTransactions.second))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]CacheTxs hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return cachedTransactions.second;
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        auto blockStr =
            getStorageGetter()->getTxsFromStorage(_blockNumber, getMemoryTableFactory(0));
        auto storage_getter_time = utcTime() - record_time;
        record_time = utcTime();
        if (!blockStr.empty())
        {
            auto block = decodeBlock(blockStr);
            auto constTxs = blockTransactionListGetter(block);

            auto decode_txs_time_cost = utcTime() - record_time;
            record_time = utcTime();

            LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]Write to cache");
            auto txs = m_transactionsCache.add(_blockNumber, constTxs);
            auto addCache_time_cost = utcTime() - record_time;
            LEDGER_LOG(DEBUG) << LOG_DESC("Get Txs from db")
                              << LOG_KV("getCacheTimeCost", getCache_time_cost)
                              << LOG_KV("storageGetterTimeCost", storage_getter_time)
                              << LOG_KV("decodeTxsTimeCost", decode_txs_time_cost)
                              << LOG_KV("addCacheTimeCost", addCache_time_cost)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
            return txs;
        }
        LEDGER_LOG(ERROR) << LOG_DESC("[#getTxs]Can't find txs, return nullptr");
        return nullptr;
    }
}

bcos::protocol::ReceiptsPtr Ledger::getReceipts(
    const bcos::protocol::BlockNumber& _blockNumber)
{
    if (_blockNumber < 0 || _blockNumber > getLatestBlockNumber())
    {
        return nullptr;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedReceipts = m_receiptCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second) && cachedBlock.second->receiptsSize() != 0)
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]CacheBlock hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return blockReceiptListGetter( cachedBlock.second);
    }
    else if (bool(cachedReceipts.second))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]Cache Receipts hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return cachedReceipts.second;
    }
    else
    {
        auto blockStr =
            getStorageGetter()->getReceiptsFromStorage(_blockNumber, getMemoryTableFactory(0));
        auto storage_getter_time = utcTime() - record_time;
        record_time = utcTime();
        LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        if(!blockStr.empty()){
            auto block = decodeBlock(blockStr);
            auto constReceipts = blockReceiptListGetter(block);

            auto decode_receipts_time_cost = utcTime() - record_time;
            record_time = utcTime();

            LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]Write to cache");
            auto receipts = m_receiptCache.add(_blockNumber, constReceipts);
            auto addCache_time_cost = utcTime() - record_time;
            LEDGER_LOG(DEBUG) << LOG_DESC("Get Receipts from db")
                              << LOG_KV("getCacheTimeCost", getCache_time_cost)
                              << LOG_KV("storageGetterTimeCost", storage_getter_time)
                              << LOG_KV("decodeTxsTimeCost", decode_receipts_time_cost)
                              << LOG_KV("addCacheTimeCost", addCache_time_cost)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
            return receipts;
        }
        LEDGER_LOG(ERROR) << LOG_DESC("[#getTxs]Can't receipts txs, return nullptr");
        return nullptr;
    }
}

MerkleProofPtr Ledger::getTxProof(const HashType& _txHash)
{
    auto numIndexPair =
        getStorageGetter()->getBlockNumberAndIndexByHash(_txHash.hex(), getMemoryTableFactory(0));
    if (numIndexPair && !numIndexPair->first.empty() && !numIndexPair->second.empty())
    {
        auto blockNumber = boost::lexical_cast<BlockNumber>(numIndexPair->first);
        auto index = boost::lexical_cast<uint>(numIndexPair->second);
        auto txs = getTxs(blockNumber);
        if (!txs)
        {
            LEDGER_LOG(TRACE) << LOG_DESC("[#getTxProof] get txs error")
                              << LOG_KV("blockNumber", blockNumber) << LOG_KV("txHash", _txHash);
            return nullptr;
        }
        if (txs->size() > index)
        {
            auto merkleProof = std::make_shared<MerkleProof>();
            auto tx = txs->at(index);
            auto parent2ChildList = getParent2ChildListByTxsProofCache(blockNumber, txs);
            auto child2Parent = getChild2ParentCacheByTransaction(parent2ChildList, blockNumber);
            getMerkleProof(tx->hash(), *parent2ChildList, *child2Parent, *merkleProof);
            return merkleProof;
        }
    }
    return nullptr;
}

LedgerConfig::Ptr Ledger::getLedgerConfig(protocol::BlockNumber _number, const crypto::HashType& _hash){
    auto ledgerConfig = std::make_shared<LedgerConfig>();
    ledgerConfig->setBlockNumber(_number);
    ledgerConfig->setHash(_hash);
    asyncGetSystemConfigByKey(SYSTEM_KEY_CONSENSUS_TIMEOUT, [&](Error::Ptr _error,
                                                                std::string _value, BlockNumber) {
        if (_error->errorCode() == 0)
        {
            ledgerConfig->setConsensusTimeout(boost::lexical_cast<uint64_t>(_value));
            asyncGetNodeListByType(CONSENSUS_SEALER,
                [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
                    if (_error->errorCode() == 0)
                    {
                        ledgerConfig->setConsensusNodeList(*_nodeList);
                    }
                    else
                    {
                        LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("getKey", CONSENSUS_SEALER);
                    }
                });
        }
        else
        {
            LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("getKey", SYSTEM_KEY_CONSENSUS_TIMEOUT);
        }
    });
    asyncGetSystemConfigByKey(
        SYSTEM_KEY_TX_COUNT_LIMIT, [&](Error::Ptr _error, std::string _value, BlockNumber)
        {
          if (_error->errorCode() == 0)
          {
              ledgerConfig->setBlockTxCountLimit(boost::lexical_cast<uint64_t>(_value));
              asyncGetNodeListByType(
                  CONSENSUS_OBSERVER, [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
                    if (_error->errorCode() == 0)
                    {
                        ledgerConfig->setObserverNodeList(*_nodeList);
                    }
                    else
                    {
                        LEDGER_LOG(ERROR)
                                << LOG_DESC("") << LOG_KV("getKey", CONSENSUS_OBSERVER);
                    }
                  });
          }
          else
          {
              LEDGER_LOG(ERROR) << LOG_DESC("")
                                << LOG_KV("getKey", SYSTEM_KEY_TX_COUNT_LIMIT);
          }
        });
    return ledgerConfig;
}

std::shared_ptr<Child2ParentMap> Ledger::getChild2ParentCacheByReceipt(
    std::shared_ptr<Parent2ChildListMap> _parent2ChildList, BlockNumber _blockNumber)
{
    return getChild2ParentCache(
        x_receiptChild2ParentCache, m_receiptChild2ParentCache, _parent2ChildList, _blockNumber);
}

std::shared_ptr<Child2ParentMap> Ledger::getChild2ParentCacheByTransaction(
    std::shared_ptr<Parent2ChildListMap> _parent2Child, BlockNumber _blockNumber)
{
    return getChild2ParentCache(
        x_txsChild2ParentCache, m_txsChild2ParentCache, _parent2Child, _blockNumber);
}

std::shared_ptr<Child2ParentMap> Ledger::getChild2ParentCache(SharedMutex& _mutex,
    std::pair<BlockNumber, std::shared_ptr<Child2ParentMap>>& _cache,
    std::shared_ptr<Parent2ChildListMap> _parent2Child, BlockNumber _blockNumber)
{
    UpgradableGuard l(_mutex);
    if (_cache.second && _cache.first == _blockNumber)
    {
        return _cache.second;
    }
    UpgradeGuard ul(l);
    // After preempting the write lock, judge again whether m_receiptWithProof has been updated
    // to prevent lock competition
    if (_cache.second && _cache.first == _blockNumber)
    {
        return _cache.second;
    }
    std::shared_ptr<Child2ParentMap> child2Parent = std::make_shared<Child2ParentMap>();
    parseMerkleMap(_parent2Child, *child2Parent);
    _cache = std::make_pair(_blockNumber, child2Parent);
    return child2Parent;
}

std::shared_ptr<Parent2ChildListMap> Ledger::getParent2ChildListByReceiptProofCache(
    protocol::BlockNumber _blockNumber, protocol::ReceiptsPtr _receipts)
{
    UpgradableGuard l(m_receiptWithProofMutex);
    // cache for the block parent2ChildList
    if (m_receiptWithProof.second && m_receiptWithProof.first == _blockNumber)
    {
        return m_receiptWithProof.second;
    }

    UpgradeGuard ul(l);
    // After preempting the write lock, judge again whether m_receiptWithProof has been updated
    // to prevent lock competition
    if (m_receiptWithProof.second && m_receiptWithProof.first == _blockNumber)
    {
        return m_receiptWithProof.second;
    }

    auto parent2ChildList = getReceiptProof(m_blockFactory->cryptoSuite(), _receipts);
    m_receiptWithProof = std::make_pair(_blockNumber, parent2ChildList);
    return parent2ChildList;
}

std::shared_ptr<Parent2ChildListMap> Ledger::getParent2ChildListByTxsProofCache(
    protocol::BlockNumber _blockNumber, protocol::TransactionsPtr _txs)
{
    UpgradableGuard l(m_transactionWithProofMutex);
    // cache for the block parent2ChildList
    if (m_transactionWithProof.second &&
        m_transactionWithProof.first == _blockNumber)
    {
        return m_transactionWithProof.second;
    }
    UpgradeGuard ul(l);
    // After preempting the write lock, judge again whether m_transactionWithProof has been
    // updated to prevent lock competition
    if (m_transactionWithProof.second &&
        m_transactionWithProof.first == _blockNumber)
    {
        return m_transactionWithProof.second;
    }
    auto parent2ChildList = getTransactionProof(m_blockFactory->cryptoSuite(), _txs);
    m_transactionWithProof = std::make_pair(_blockNumber, parent2ChildList);
    return parent2ChildList;
}

Block::Ptr Ledger::decodeBlock(const std::string& _blockStr)
{
    Block::Ptr block = nullptr;
    auto blockBytes = asBytes(_blockStr);
    block = m_blockFactory->createBlock(blockBytes, false, false);
    return block;
}

BlockHeader::Ptr Ledger::decodeBlockHeader(const std::string& _headerStr)
{
    BlockHeader::Ptr header = nullptr;
    header = m_headerFactory->createBlockHeader(asBytes(_headerStr));
    return header;
}

bool Ledger::isBlockShouldCommit(const BlockNumber& _blockNumber)
{
    auto number = getLatestBlockNumber();
    if (_blockNumber != number + 1)
    {
        LEDGER_LOG(WARNING) << LOG_DESC(
            "[#commitBlock]Commit fail due to incorrect block number")
                                << LOG_KV("needNumber", number + 1)
                                << LOG_KV("committedNumber", _blockNumber);
        return false;
    }
    return true;
}

void Ledger::writeNumber(
    const BlockNumber& blockNumber, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    bool ret = getStorageSetter()->setCurrentState(
        _tableFactory, SYS_KEY_CURRENT_NUMBER, boost::lexical_cast<std::string>(blockNumber));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNumber2Txs")
                          << LOG_DESC("Write row in SYS_NUMBER_2_TXS error")
                          << LOG_KV("blockNumber", blockNumber);
    }
}
void Ledger::writeTxToBlock(
    const Block::Ptr& block, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    getStorageSetter()->writeTxToBlock(block, _tableFactory);
}

void Ledger::writeNoncesToBlock(
    const Block::Ptr& block, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto blockNumberStr = boost::lexical_cast<std::string>(block->blockHeader()->number());
    auto emptyBlock = m_blockFactory->createBlock();
    emptyBlock->setNonceList(block->nonceList());

    std::shared_ptr<bytes> nonceData = std::make_shared<bytes>();
    emptyBlock->encode(*nonceData);
    bool ret =
        getStorageSetter()->setNumber2Nonces(_tableFactory, blockNumberStr, asString(*nonceData));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNoncesToBlock")
                          << LOG_DESC("Write row in SYS_BLOCK_NUMBER_2_NONCES error")
                          << LOG_KV("blockNumber", blockNumberStr);
    }
}

void Ledger:: writeHash2Number(
    const Block::Ptr& block, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    bool ret = getStorageSetter()->setHash2Number(_tableFactory, block->blockHeader()->hash().hex(),
        boost::lexical_cast<std::string>(block->blockHeader()->number()));
    ret = ret && getStorageSetter()->setNumber2Hash(_tableFactory,
                     boost::lexical_cast<std::string>(block->blockHeader()->number()),
                     block->blockHeader()->hash().hex());
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteHash2Number")
                          << LOG_DESC("Write row in SYS_HASH_2_NUMBER error")
                          << LOG_KV("blockHash", block->blockHeader()->hash().hex());
    }
}

void Ledger::writeNumber2BlockHeader(
    const Block::Ptr& _block, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto encodedBlockHeader = std::make_shared<bytes>();
    auto emptyBlock = m_blockFactory->createBlock();
    emptyBlock->setBlockHeader(_block->blockHeader());
    emptyBlock->blockHeader()->encode(*encodedBlockHeader);

    bool ret = getStorageSetter()->setNumber2Header(_tableFactory,
        boost::lexical_cast<std::string>(_block->blockHeader()->number()),
        asString(*encodedBlockHeader));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNumber2Header")
                          << LOG_DESC("Write row in SYS_NUMBER_2_BLOCK_HEADER error")
                          << LOG_KV("blockNumber", _block->blockHeader()->number());
    }
}
void Ledger::writeTotalTransactionCount(
    const Block::Ptr& block, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    std::string totalTxStr =
        getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, _tableFactory);
    size_t totalTxCount = 0;
    if(!totalTxStr.empty()){
        totalTxCount = boost::lexical_cast<size_t>(totalTxStr);
    }
    totalTxCount += block->transactionsSize();
    auto ret = getStorageSetter()->setCurrentState(_tableFactory, SYS_KEY_TOTAL_TRANSACTION_COUNT,
        boost::lexical_cast<std::string>(totalTxCount));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteCurrentState")
                          << LOG_DESC("Write SYS_KEY_TOTAL_TRANSACTION_COUNT error")
                          << LOG_KV("blockNumber", block->blockHeader()->number());
        return;
    }

    auto receipts = blockReceiptListGetter(block);
    size_t failedTransactions = 0;
    for (auto& receipt : *receipts)
    {
        // TODO: check receipt status
        if (receipt->status() != 0)
        {
            ++failedTransactions;
        }
    }

    std::string totalFailedTxsStr =
        getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION, _tableFactory);
    if(!totalFailedTxsStr.empty()){
        failedTransactions += boost::lexical_cast<size_t>(totalFailedTxsStr);
    }

    ret = getStorageSetter()->setCurrentState(_tableFactory, SYS_KEY_TOTAL_FAILED_TRANSACTION,
        boost::lexical_cast<std::string>(failedTransactions));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteCurrentState")
                          << LOG_DESC("Write SYS_KEY_TOTAL_TRANSACTION_COUNT error")
                          << LOG_KV("blockNumber", block->blockHeader()->number());
        return;
    }
}
void Ledger::writeNumber2Transactions(
    const Block::Ptr& _block, const BlockNumber& _number, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto encodeBlock = std::make_shared<bytes>();
    auto emptyBlock = m_blockFactory->createBlock();
    for (size_t i = 0; i < _block->transactionsSize(); i++)
    {
        emptyBlock->appendTransaction(
            std::const_pointer_cast<protocol::Transaction>(_block->transaction(i)));
    }

    emptyBlock->encode(*encodeBlock);
    bool ret = getStorageSetter()->setNumber2Txs(
        _tableFactory, boost::lexical_cast<std::string>(_number), asString(*encodeBlock));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNumber2Txs")
                          << LOG_DESC("Write row in SYS_NUMBER_2_TXS error")
                          << LOG_KV("blockNumber", _number);
    }
}
void Ledger::writeNumber2Receipts(const bcos::protocol::Block::Ptr& _block,
    const BlockNumber& _number, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto encodeBlock = std::make_shared<bytes>();
    auto emptyBlock = m_blockFactory->createBlock();

    for (size_t i = 0; i < _block->receiptsSize(); ++i)
    {
        emptyBlock->appendReceipt(
            std::const_pointer_cast<protocol::TransactionReceipt>(_block->receipt(i)));
    }

    emptyBlock->encode(*encodeBlock);
    bool ret = getStorageSetter()->setNumber2Receipts(_tableFactory,
                                                 boost::lexical_cast<std::string>(_number),
                                                 asString(*encodeBlock));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNumber2Receipts")
                          << LOG_DESC("Write row in SYS_NUMBER_2_RECEIPTS error")
                          << LOG_KV("blockNumber", _number);
    }
}
bool Ledger::buildGenesisBlock(LedgerConfig::Ptr _ledgerConfig)
{
    LEDGER_LOG(INFO) << LOG_DESC("[#buildGenesisBlock]");
    // TODO: to check NUMBER_2_HEADER table is created
    // TODO: creat tables
    auto block = getBlock(0, HEADER);

    // to build genesis block
    if(block == nullptr)
    {
        auto txLimit = _ledgerConfig->blockTxCountLimit();
        LEDGER_LOG(TRACE) << LOG_DESC("test") << LOG_KV("txLimit", txLimit);
        auto tableFactory = getState()->getStateCache(0);
        // build a block
        block = m_blockFactory->createBlock();
        auto header = m_headerFactory->createBlockHeader();
        header->setNumber(0);
        // TODO: add genesisMark
        header->setExtraData(asBytes(""));
        block->setBlockHeader(header);
        try
        {
            // TODO: concurrent write these
            // TODO: set cache
            // write in HASH_2_NUMBER
            writeHash2Number(block, tableFactory);
            // write in SYS_CONFIG
            // SYSTEM_KEY_TX_COUNT_LIMIT
            getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_TX_COUNT_LIMIT,
                boost::lexical_cast<std::string>(_ledgerConfig->blockTxCountLimit()), "0");
            // SYSTEM_KEY_CONSENSUS_TIMEOUT
            getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_CONSENSUS_TIMEOUT,
                boost::lexical_cast<std::string>(_ledgerConfig->consensusTimeout()), "0");
            // write in SYS_CONSENSUS
            getStorageSetter()->setConsensusConfig(
                tableFactory, CONSENSUS_SEALER, _ledgerConfig->consensusNodeList(), "0");
            getStorageSetter()->setConsensusConfig(
                tableFactory, CONSENSUS_OBSERVER, _ledgerConfig->observerNodeList(), "0");
            // write in NUMBER_2_HEADER
            writeNumber2BlockHeader(block, tableFactory);
            // write in SYS_CURRENT_STATE
            getStorageSetter()->setCurrentState(
                tableFactory, SYS_KEY_CURRENT_NUMBER, "0");
            getStorageSetter()->setCurrentState(tableFactory, SYS_KEY_TOTAL_TRANSACTION_COUNT, "0");
            getStorageSetter()->setCurrentState(tableFactory, SYS_KEY_TOTAL_FAILED_TRANSACTION, "0");
            // db commit
            auto ret = tableFactory->commit();
            if(ret == 0)
            {
                LEDGER_LOG(ERROR) << LOG_DESC("[#buildGenesisBlock]Storage commit error");
            }
        }
        catch (OpenSysTableFailed const& e){
            LEDGER_LOG(FATAL)
                    << LOG_DESC("[#buildGenesisBlock]System meets error when try to write block to storage")
                    << LOG_KV("EINFO", boost::diagnostic_information(e));
            raise(SIGTERM);
            BOOST_THROW_EXCEPTION(
                OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
        }
    }
    else{
        // TODO: check 0th block
        LEDGER_LOG(INFO) << LOG_DESC(
            "[#buildGenesisBlock]Already have the 0th block");
        return true;
    }
    return true;
}
