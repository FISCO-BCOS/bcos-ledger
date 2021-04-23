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
#include <boost/lexical_cast.hpp>
#include <tbb/parallel_invoke.h>
#include <tbb/parallel_for.h>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::crypto;

void Ledger::asyncCommitBlock(bcos::protocol::BlockNumber _blockNumber,
                              bcos::protocol::SignatureListPtr _signList, std::function<void(Error::Ptr)> _onCommitBlock)
{}
void Ledger::asyncGetTransactionByHash(const bcos::crypto::HashType& _txHash,
    std::function<void(Error::Ptr, bcos::protocol::Transaction::ConstPtr)> _onGetTx)
{
    auto table = getMemoryTableFactory()->openTable(SYS_TX_HASH_2_BLOCK_NUMBER);
    if (table)
    {
        auto entry = table->getRow(_txHash.hex());
        std::string blockNumberStr = entry->getField(SYS_VALUE);
        auto txIndex = boost::lexical_cast<uint>(entry->getField("index"));
        auto txs = getTxs(boost::lexical_cast<BlockNumber>(blockNumberStr));
        if (!txs)
        {
            LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs] get txs error")
                              << LOG_KV("blockNumber", blockNumberStr) << LOG_KV("txHash", _txHash);
            // TODO: add error code and msg
            auto error = std::make_shared<Error>(-1, "");
            _onGetTx(error, nullptr);
            return;
        }
        if (txs->size() > txIndex)
        {
            _onGetTx(nullptr, txs->at(txIndex));
            return;
        }
    }
    LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("txHash", _txHash);
    // TODO: add error code and message
    auto error = std::make_shared<Error>(-1, "");
    _onGetTx(error, nullptr);
}

void Ledger::asyncGetTransactionReceiptByHash(const bcos::crypto::HashType& _txHash,
    std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr)> _onGetTx)
{
    auto table = getMemoryTableFactory()->openTable(SYS_TX_HASH_2_BLOCK_NUMBER);
    if (table)
    {
        auto entry = table->getRow(_txHash.hex());
        if (entry)
        {
            auto blockNumber = boost::lexical_cast<BlockNumber>(entry->getField(SYS_VALUE));
            auto txIndex = boost::lexical_cast<uint>(entry->getField("index"));
            auto receipts = getReceipts(blockNumber);
            if (!receipts)
            {
                LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts] get receipts error")
                                  << LOG_KV("blockNumber", blockNumber)
                                  << LOG_KV("txHash", _txHash);
                // TODO: add error code and message
                auto error = std::make_shared<Error>(-1, "");
                _onGetTx(error, nullptr);
                return;
            }
            if (receipts->size() > txIndex)
            {
                _onGetTx(nullptr, receipts->at(txIndex));
                return;
            }
        }
    }
    LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("txHash", _txHash);
    // TODO: add error code and message
    auto error = std::make_shared<Error>(-1, "");
    _onGetTx(error, nullptr);
}

void Ledger::asyncPreStoreTransactions(
    const Blocks& _txsToStore, std::function<void(Error::Ptr)> _onTxsStored)
{}
void Ledger::asyncGetTotalTransactionCount(
    std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
{}
void Ledger::asyncGetTransactionReceiptProof(const crypto::HashType& _blockHash,
                                             const int64_t& _index, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{}
void Ledger::getTransactionProof(const crypto::HashType& _blockHash, const int64_t& _index,
                                 std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{}
void Ledger::asyncGetTransactionProofByHash(
    const bcos::crypto::HashType& _txHash, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{}

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
void Ledger::asyncGetBlockHashByNumber(bcos::protocol::BlockNumber number,
    std::function<void(Error::Ptr, std::shared_ptr<const bcos::crypto::HashType>)> _onGetBlock)
{}
void Ledger::asyncGetBlockByHash(
    const bcos::crypto::HashType& _blockHash, std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
{
    if(_blockHash == bcos::crypto::HashType(""))
    {
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
        return;
    }
    auto block = getBlock(_blockHash);
    if(block){
        _onGetBlock(nullptr, block);
    } else{
        LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetBlockByHash]Can't find block, return nullptr")
                              << LOG_KV("blockHash", _blockHash);

        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
    }
}
void Ledger::asyncGetBlockByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
{
    auto currentNum = getLatestBlockNumber();
    if(_blockNumber > currentNum) {
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
        return;
    }
    auto block = getBlock(_blockNumber);
    if(block){
        _onGetBlock(nullptr, block);
    } else{
        LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetBlockByNumber]Can't find block, return nullptr")
                          << LOG_KV("blockNumber", _blockNumber);

        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
    }
}
void Ledger::asyncGetBlockEncodedByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, std::shared_ptr<const bytes>)> _onGetBlock)
{
    if (_blockNumber > getLatestBlockNumber())
    {
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
        return;
    }
    auto block = getEncodeBlock(_blockNumber);
    if (bool(block))
    {
        _onGetBlock(nullptr, block);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetBlockEncodedByNumber]Can't find block, return nullptr");

        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
    }
}
void Ledger::asyncGetBlockHeaderByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, std::shared_ptr<const std::pair<bcos::protocol::BlockHeader::Ptr,
                                       bcos::protocol::SignatureListPtr>>)>
        _onGetBlock)
{
    if (_blockNumber > getLatestBlockNumber() || _blockNumber < 0)
    {
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
        return;
    }
    auto header = getBlockHeader(_blockNumber);
    if (header)
    {
        _onGetBlock(nullptr, std::make_shared<const std::pair<BlockHeader::Ptr, SignatureListPtr>>(
                                 header, header->signatureList()));
        return;
    }
    LEDGER_LOG(ERROR)
        << LOG_DESC("[#asyncGetBlockHeaderByNumber] error happened in open table or get entry")
        << LOG_KV("block number", _blockNumber);
    // TODO: add error code and msg
    auto error = std::make_shared<Error>(-1, "");
    _onGetBlock(error, nullptr);
}

void Ledger::asyncGetBlockHeaderByHash(const bcos::crypto::HashType& _blockHash,
    std::function<void(Error::Ptr, std::shared_ptr<const std::pair<bcos::protocol::BlockHeader::Ptr,
                                       bcos::protocol::SignatureListPtr>>)>
        _onGetBlock)
{
    if (_blockHash != HashType(""))
    {
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
        return;
    }
    auto number = getBlockNumberByHash(_blockHash);
    if (number >= 0)
    {
        auto header = getBlockHeader(number);
        if (header)
        {
            _onGetBlock(
                nullptr, std::make_shared<const std::pair<BlockHeader::Ptr, SignatureListPtr>>(
                             header, header->signatureList()));
            return;
        }
        LEDGER_LOG(ERROR)
            << LOG_DESC("[#asyncGetBlockHeaderByNumber] error happened in open table or get entry")
            << LOG_KV("block hash", _blockHash);
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, nullptr);
    }
}
void Ledger::asyncGetCode(const std::string& _tableID, bcos::Address _codeAddress,
    std::function<void(Error::Ptr, std::shared_ptr<const bytes>)> _onGetCode)
{}
void Ledger::asyncGetSystemConfigByKey(const std::string& _key,
    std::function<void(
        Error::Ptr, std::shared_ptr<const std::pair<std::string, bcos::protocol::BlockNumber>>)>
        _onGetConfig)
{
    auto currentNumber = getLatestBlockNumber();
    UpgradableGuard l(m_systemConfigMutex);
    auto it = m_systemConfigRecordMap.find(_key);
    if (it != m_systemConfigRecordMap.end() && it->second.curBlockNum == currentNumber)
    {
        // get value from cache
        auto pair = std::make_shared<const std::pair<std::string, bcos::protocol::BlockNumber>>(
            it->second.value, it->second.enableNumber);
        _onGetConfig(nullptr, pair);
    }

    auto result = std::make_shared<std::pair<std::string, BlockNumber>>(std::make_pair("", -1));
    // cannot find the system config key or need to update the value with different block height
    // get value from db
    try
    {
        TableInterface::Ptr tb = getMemoryTableFactory()->openTable(SYS_CONFIG);
        if (!tb)
        {
            LEDGER_LOG(ERROR) << LOG_DESC("[#asyncGetSystemConfigByKey]Open table error");
            // TODO: add error code and error msg
            auto error = std::make_shared<Error>(-1, "");
            _onGetConfig(error, nullptr);
        }
        auto entry = tb->getRow(_key);
        if (!entry)
        {
            LEDGER_LOG(ERROR) << LOG_DESC("[#asyncGetSystemConfigByKey] Null pointer of entry")
                              << LOG_KV("key", _key);
            // TODO: add error code and error msg
            auto error = std::make_shared<Error>(-1, "");
            _onGetConfig(error, nullptr);
        }
        auto enableNum =
            boost::lexical_cast<BlockNumber>(entry->getField(SYS_CONFIG_ENABLE_BLOCK_NUMBER));
        if (enableNum <= currentNumber)
        {
            result->first = entry->getField(SYS_VALUE);
            result->second = enableNum;
        }
        _onGetConfig(nullptr, result);
    }
    catch (std::exception& e)
    {
        LEDGER_LOG(ERROR) << LOG_DESC("[#asyncGetSystemConfigByKey]Failed")
                          << LOG_KV("EINFO", boost::diagnostic_information(e));
        // TODO: add error code and error msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetConfig(error, nullptr);
    }

    // update cache
    {
        UpgradeGuard ul(l);
        SystemConfigRecordCache systemConfigRecordCache(
            result->first, result->second, currentNumber);
        if (it != m_systemConfigRecordMap.end())
        {
            it->second = systemConfigRecordCache;
        }
        else
        {
            m_systemConfigRecordMap.insert(
                std::pair<std::string, SystemConfigRecordCache>(_key, systemConfigRecordCache));
        }
    }

    LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetSystemConfigByKey]Data in db") << LOG_KV("key", _key)
                      << LOG_KV("value", result->first);
}
void Ledger::asyncGetNonceList(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, bcos::protocol::NonceListPtr)> _onGetList)
{}

Block::Ptr Ledger::getFullBlock(BlockNumber const& _blockNumber)
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
        TableInterface::Ptr tb = getMemoryTableFactory()->openTable(SYS_NUMBER_2_BLOCK);
        auto openTable_time_cost = utcTime() - record_time;
        record_time = utcTime();
        if (tb)
        {
            auto entry = tb->getRow(boost::lexical_cast<std::string>(_blockNumber));
            auto select_time_cost = utcTime() - record_time;
            if (entry != nullptr)
            {
                record_time = utcTime();
                auto block = decodeBlock(entry);

                auto constructBlock_time_cost = utcTime() - record_time;
                record_time = utcTime();

                LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Write to cache");
                auto blockPtr = m_blockCache.add(_blockNumber, block);
                auto addCache_time_cost = utcTime() - record_time;
                LEDGER_LOG(DEBUG) << LOG_DESC("Get block from db")
                                  << LOG_KV("getCacheTimeCost", getCache_time_cost)
                                  << LOG_KV("openTableTimeCost", openTable_time_cost)
                                  << LOG_KV("selectTimeCost", select_time_cost)
                                  << LOG_KV("constructBlockTimeCost", constructBlock_time_cost)
                                  << LOG_KV("addCacheTimeCost", addCache_time_cost)
                                  << LOG_KV("totalTimeCost", utcTime() - start_time);
                return blockPtr;
            }
        }

        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Can't find the block")
                          << LOG_KV("blockNumber", _blockNumber);
        return nullptr;
    }
}
Block::Ptr Ledger::getBlock(BlockNumber const& _blockNumber)
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
        auto header = getBlockHeader(_blockNumber);
        auto constTxs = getTxs(_blockNumber);
        auto constReceipts = getReceipts(_blockNumber);
        auto get_full_data_cost = utcTime() - record_time;
        record_time = utcTime();

        if (header && constTxs && constReceipts)
        {
            auto txs = std::make_shared<Transactions>(*constTxs);
            auto receipts = std::make_shared<Receipts>(*constReceipts);
            auto block = m_blockFactory->createBlock();
            block->setBlockHeader(header);
            block->setTransactions(txs);
            block->setReceipts(receipts);

            auto assemble_block = utcTime() - record_time;
            record_time = utcTime();

            LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Write to cache");
            auto blockPtr = m_blockCache.add(_blockNumber, block);
            auto addCache_time_cost = utcTime() - record_time;
            LEDGER_LOG(DEBUG) << LOG_DESC("Get block from db")
                              << LOG_KV("getCacheTimeCost", getCache_time_cost)
                              << LOG_KV("getFullBlockDataCost", get_full_data_cost)
                              << LOG_KV("constructBlockTimeCost", assemble_block)
                              << LOG_KV("addCacheTimeCost", addCache_time_cost)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
            return blockPtr;
        }
        else
        {
            LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Can't find the block")
                              << LOG_KV("blockNumber", _blockNumber);
            return nullptr;
        }
    }
}

Block::Ptr Ledger::getBlock(const bcos::crypto::HashType& _blockHash)
{
    if(_blockHash == HashType(""))
    {
        return nullptr;
    }
    else
    {
        auto blockNumber = getBlockNumberByHash(_blockHash);
        if (blockNumber != -1)
        {
            return getBlock(blockNumber);
        }
    }
    LEDGER_LOG(WARNING) << LOG_DESC("[getBlock]Can't find block")
                        << LOG_KV("hash", _blockHash);
    return nullptr;
}

bytesPointer Ledger::getEncodeBlock(const HashType& _blockHash)
{
    if(!_blockHash || _blockHash == HashType("")){
        return nullptr;
    }
    TableInterface::Ptr tb = getMemoryTableFactory()->openTable(SYS_HASH_2_NUMBER);
    if(tb)
    {
        auto entry = tb->getRow(boost::lexical_cast<std::string>( _blockHash));
        auto blockNumber = boost::lexical_cast<BlockNumber>(entry->getField(SYS_VALUE));
        return getEncodeBlock(blockNumber);
    }
    return nullptr;
}

// FIXME: drop main logic, use getBlock
bytesPointer Ledger::getEncodeBlock(const bcos::protocol::BlockNumber& _blockNumber)
{
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (_blockNumber > getLatestBlockNumber())
    {
        return nullptr;
    }
    if (bool(cachedBlock.first))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getEncodeBlock]Cache hit, read from cache");
        std::shared_ptr<bytes> encodedBlock = std::make_shared<bytes>();
        cachedBlock.second->encode(*encodedBlock);
        LEDGER_LOG(DEBUG) << LOG_DESC("Get block from cache")
                              << LOG_KV("getCacheTimeCost", getCache_time_cost)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
        return encodedBlock;
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getEncodeBlock]Cache missed, read from storage");
        TableInterface::Ptr tb = getMemoryTableFactory()->openTable(SYS_NUMBER_2_BLOCK);
        auto openTable_time_cost = utcTime() - record_time;
        record_time = utcTime();
        if (tb)
        {
            auto entry = tb->getRow(boost::lexical_cast<std::string>(_blockNumber));
            auto select_time_cost = utcTime() - record_time;
            if (entry != nullptr)
            {
                record_time = utcTime();
                auto encodedBlock = std::make_shared<bytes>(asBytes(entry->getField(SYS_VALUE)));

                auto encode_time_cost = utcTime() - record_time;

                LEDGER_LOG(DEBUG) << LOG_DESC("Get encoded block from db")
                                  << LOG_KV("getCacheTimeCost", getCache_time_cost)
                                  << LOG_KV("openTableTimeCost", openTable_time_cost)
                                  << LOG_KV("getRowTimeCost", select_time_cost)
                                  << LOG_KV("constructEncodeBytesTimeCost", encode_time_cost)
                                  << LOG_KV("totalTimeCost", utcTime() - start_time);
                return encodedBlock;
            }
        }

        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Can't find the block")
                              << LOG_KV("blockNumber", _blockNumber);
        return nullptr;
    }
}
bcos::protocol::BlockNumber Ledger::getBlockNumberByHash(bcos::crypto::HashType const& _hash)
{
    BlockNumber number = -1;
    auto tb = m_tableFactory->openTable(SYS_HASH_2_NUMBER);
    if (tb)
    {
        auto entry = tb->getRow(boost::lexical_cast<std::string>(_hash));
        number = boost::lexical_cast<BlockNumber>(entry->getField(SYS_VALUE));
    }
    return number;
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
    auto tb = m_tableFactory->openTable(SYS_CURRENT_STATE);
    if (tb)
    {
        auto entry = tb->getRow(SYS_KEY_CURRENT_NUMBER);
        std::string currentNumber = entry->getField(SYS_VALUE);
        num = boost::lexical_cast<BlockNumber>(currentNumber);
    }
    return num;
}


BlockHeader::Ptr Ledger::getBlockHeader(const bcos::protocol::BlockNumber& _blockNumber)
{
    if (_blockNumber > getLatestBlockNumber())
    {
        return nullptr;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedHeader = m_blockHeaderCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second) && cachedBlock.second->transactions() &&
        !cachedBlock.second->transactions()->empty())
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
        TableInterface::Ptr table = getMemoryTableFactory()->openTable(SYS_NUMBER_2_BLOCK_HEADER);
        auto openTable_time_cost = utcTime() - record_time;
        record_time = utcTime();
        if (table)
        {
            auto entry = table->getRow(boost::lexical_cast<std::string>(_blockNumber));
            auto select_time_cost = utcTime() - record_time;
            if (entry)
            {
                record_time = utcTime();
                auto headerBytes = asBytes(entry->getField(SYS_VALUE));
                auto signListBytes = asBytes(entry->getField(SYS_SIG_LIST));
                // TODO: decode sign list
                SignatureListPtr signList = std::make_shared<SignatureList>();

                auto headerPtr = m_headerFactory->createBlockHeader(headerBytes);
                headerPtr->setSignatureList(signList);
                auto decode_header_time_cost = utcTime() - record_time;
                record_time = utcTime();

                LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]Write to cache");
                auto header = m_blockHeaderCache.add(_blockNumber, headerPtr);
                auto addCache_time_cost = utcTime() - record_time;
                LEDGER_LOG(DEBUG) << LOG_DESC("Get Txs from db")
                                  << LOG_KV("getCacheTimeCost", getCache_time_cost)
                                  << LOG_KV("openTableTimeCost", openTable_time_cost)
                                  << LOG_KV("selectTimeCost", select_time_cost)
                                  << LOG_KV("decodeTxsTimeCost", decode_header_time_cost)
                                  << LOG_KV("addCacheTimeCost", addCache_time_cost)
                                  << LOG_KV("totalTimeCost", utcTime() - start_time);
                return header;
            }
        }
        LEDGER_LOG(ERROR) << LOG_DESC("[#getBlockHeader]Can't find header, return nullptr");
        return nullptr;
    }
}


BlockHeader::Ptr Ledger::getBlockHeaderFromBlock(const Block::Ptr& _block)
{
    //TODO: check this return
    if (!_block)
    {
        return nullptr;
    }
    return _block->blockHeader();
}
std::shared_ptr<Child2ParentMap> Ledger::getChild2ParentCacheByReceipt(
    std::shared_ptr<Parent2ChildListMap> _parent2ChildList, Block::Ptr _block)
{
    return std::shared_ptr<Child2ParentMap>();
}
std::shared_ptr<Child2ParentMap> Ledger::getChild2ParentCacheByTransaction(
    std::shared_ptr<Parent2ChildListMap> _parent2Child, Block::Ptr _block)
{
    return std::shared_ptr<Child2ParentMap>();
}
std::shared_ptr<Child2ParentMap> Ledger::getChild2ParentCache(SharedMutex& _mutex,
    std::pair<BlockNumber, std::shared_ptr<Child2ParentMap>>& _cache,
    std::shared_ptr<Parent2ChildListMap> _parent2Child, Block::Ptr _block)
{
    return std::shared_ptr<Child2ParentMap>();
}

void Ledger::getMerkleProof(const bytes& _txHash,
    std::map<std::string, std::vector<std::string>> const& parent2ChildList,
    Child2ParentMap const& child2Parent,
    std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>& merkleProof)
{}

bcos::protocol::TransactionsConstPtr Ledger::getTxs(bcos::protocol::BlockNumber const& _blockNumber)
{
    if (_blockNumber > getLatestBlockNumber())
    {
        return nullptr;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedTransactions = m_transactionsCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second) && cachedBlock.second->transactions() &&
        !cachedBlock.second->transactions()->empty())
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]CacheBlock hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return cachedBlock.second->transactions();
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
        TableInterface::Ptr table = getMemoryTableFactory()->openTable(SYS_NUMBER_2_TXS);
        auto openTable_time_cost = utcTime() - record_time;
        record_time = utcTime();
        if (table)
        {
            auto entry = table->getRow(boost::lexical_cast<std::string>(_blockNumber));
            auto select_time_cost = utcTime() - record_time;
            if (entry)
            {
                record_time = utcTime();
                auto block = decodeBlock(entry);
                auto constTxs = block->transactions();
                auto decode_txs_time_cost = utcTime() - record_time;
                record_time = utcTime();

                LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]Write to cache");
                auto txs = m_transactionsCache.add(_blockNumber, constTxs);
                auto addCache_time_cost = utcTime() - record_time;
                LEDGER_LOG(DEBUG) << LOG_DESC("Get Txs from db")
                                  << LOG_KV("getCacheTimeCost", getCache_time_cost)
                                  << LOG_KV("openTableTimeCost", openTable_time_cost)
                                  << LOG_KV("selectTimeCost", select_time_cost)
                                  << LOG_KV("decodeTxsTimeCost", decode_txs_time_cost)
                                  << LOG_KV("addCacheTimeCost", addCache_time_cost)
                                  << LOG_KV("totalTimeCost", utcTime() - start_time);
                return txs;
            }
        }
        LEDGER_LOG(ERROR) << LOG_DESC("[#getTxs]Can't find txs, return nullptr");
        return nullptr;
    }
}
bcos::protocol::ReceiptsConstPtr Ledger::getReceipts(
    bcos::protocol::BlockNumber const& _blockNumber)
{
    if (_blockNumber > getLatestBlockNumber())
    {
        return nullptr;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedReceipts = m_receiptCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second) && cachedBlock.second->transactions() &&
        !cachedBlock.second->transactions()->empty())
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]CacheBlock hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return cachedBlock.second->receipts();
    }
    else if (bool(cachedReceipts.second))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]Cache Receipts hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        return cachedReceipts.second;
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        TableInterface::Ptr table = getMemoryTableFactory()->openTable(SYS_NUMBER_2_RECEIPTS);
        auto openTable_time_cost = utcTime() - record_time;
        record_time = utcTime();
        if (table)
        {
            auto entry = table->getRow(boost::lexical_cast<std::string>(_blockNumber));
            auto select_time_cost = utcTime() - record_time;
            if (entry)
            {
                record_time = utcTime();
                auto block = decodeBlock(entry);
                auto constReceipts = block->receipts();
                auto decode_receipts_time_cost = utcTime() - record_time;
                record_time = utcTime();

                LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]Write to cache");
                auto receipts = m_receiptCache.add(_blockNumber, constReceipts);
                auto addCache_time_cost = utcTime() - record_time;
                LEDGER_LOG(DEBUG) << LOG_DESC("Get Receipts from db")
                                  << LOG_KV("getCacheTimeCost", getCache_time_cost)
                                  << LOG_KV("openTableTimeCost", openTable_time_cost)
                                  << LOG_KV("selectTimeCost", select_time_cost)
                                  << LOG_KV("decodeTxsTimeCost", decode_receipts_time_cost)
                                  << LOG_KV("addCacheTimeCost", addCache_time_cost)
                                  << LOG_KV("totalTimeCost", utcTime() - start_time);
                return receipts;
            }
        }
        LEDGER_LOG(ERROR) << LOG_DESC("[#getTxs]Can't receipts txs, return nullptr");
        return nullptr;
    }
}


bcos::protocol::Block::Ptr Ledger::decodeBlock(bcos::storage::Entry::ConstPtr _entry)
{
    Block::Ptr block = nullptr;
    auto blockStr = _entry->getField(SYS_VALUE);
    auto blockBytes = asBytes(blockStr);
    block = m_blockFactory->createBlock(blockBytes, false, false);
    return block;
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

void Ledger::writeBlockToField(const Block::Ptr& _block, bcos::storage::Entry::Ptr _entry)
{
    auto encodeBlock = std::make_shared<bytes>();
    _block->encode(*encodeBlock);
    writeBytesToField(_entry, SYS_VALUE, bytesConstRef(encodeBlock->data(), encodeBlock->size()));
}

void Ledger::writeBytesToField(
    bcos::storage::Entry::Ptr _entry, const std::string& _key, bytesConstRef _bytesValue)
{
    _entry->setField(_key, _bytesValue.toString());
}

void Ledger::writeNumber(
    const Block::Ptr& block, bcos::storage::TableFactoryInterface::Ptr _tableFactory)
{
    auto tb = _tableFactory->openTable(SYS_CURRENT_STATE);
    if (tb)
    {
        auto entry = tb->newEntry();
        entry->setField(SYS_VALUE, boost::lexical_cast<std::string>(block->blockHeader()->number()));
        // TODO: judge setRow result
        tb->setRow(SYS_KEY_CURRENT_NUMBER, entry);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CURRENT_STATE));
    }
}
void Ledger::writeTxToBlock(
    const Block::Ptr& block, bcos::storage::TableFactoryInterface::Ptr _tableFactory)
{
    auto start_time = utcTime();
    auto record_time = utcTime();
    TableInterface::Ptr tb = _tableFactory->openTable(SYS_TX_HASH_2_BLOCK_NUMBER);
    TableInterface::Ptr tb_nonces = _tableFactory->openTable(SYS_BLOCK_NUMBER_2_NONCES);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (tb && tb_nonces)
    {
        auto txs = block->transactions();
        auto constructVector_time_cost = utcTime() - record_time;
        record_time = utcTime();
        auto blockNumberStr = boost::lexical_cast<std::string>(block->blockHeader()->number());
        tbb::parallel_invoke(
            [tb, txs, blockNumberStr]() {
              tbb::parallel_for(tbb::blocked_range<size_t>(0, txs->size()),
                                [&](const tbb::blocked_range<size_t>& _r) {
                                  for (size_t i = _r.begin(); i != _r.end(); ++i)
                                  {
                                      auto entry = tb->newEntry();

                                      // entry: <blockNumber, txIndex>
                                      entry->setField(SYS_VALUE, blockNumberStr);
                                      entry->setField("index", boost::lexical_cast<std::string>(i));
                                      tb->setRow((*txs)[i]->hash().hex(), entry);
                                  }
                                });
            },
            [this, tb_nonces, txs, blockNumberStr]() {
              NonceList nonce_vector(txs->size());
              for (size_t i = 0; i < txs->size(); i++)
              {
                  nonce_vector[i] = (*txs)[i]->nonce();
              }
              // FIXME: encode nonce list to bytes
              std::shared_ptr<bytes> nonceData = std::make_shared<bytes>();
              // Entry::Ptr entry_tb2nonces = std::make_shared<Entry>();
              auto entry_tb2nonces = tb_nonces->newEntry();

              writeBytesToField(entry_tb2nonces, SYS_VALUE, bytesConstRef(nonceData->data(), nonceData->size()));
              tb_nonces->setRow(boost::lexical_cast<std::string>(blockNumberStr), entry_tb2nonces);
            });
        auto insertTable_time_cost = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteTxOnCommit")
                              << LOG_DESC("Write tx to block time record")
                              << LOG_KV("openTableTimeCost", openTable_time_cost)
                              << LOG_KV("constructVectorTimeCost", constructVector_time_cost)
                              << LOG_KV("insertTableTimeCost", insertTable_time_cost)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_TX_HASH_2_BLOCK_NUMBER));
    }
}
void Ledger::writeHash2Number(
    const Block::Ptr& block, bcos::storage::TableFactoryInterface::Ptr _tableFactory)
{
    TableInterface::Ptr tb = _tableFactory->openTable(SYS_HASH_2_NUMBER);
    if (tb)
    {
        // Entry::Ptr entry = std::make_shared<Entry>();
        auto entry = tb->newEntry();
        entry->setField(SYS_VALUE, boost::lexical_cast<std::string>(block->blockHeader()->number()));
        tb->setRow(block->blockHeader()->hash().hex(), entry);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_HASH_2_NUMBER));
    }
}
void Ledger::writeNumber2Block(
    const Block::Ptr& block, bcos::storage::TableFactoryInterface::Ptr _tableFactory)
{
    TableInterface::Ptr tb = _tableFactory->openTable(SYS_NUMBER_2_BLOCK);
    if (tb)
    {
        // Entry::Ptr entry = std::make_shared<Entry>();
        auto entry = tb->newEntry();
        writeBlockToField(block, entry);
        tb->setRow(boost::lexical_cast<std::string>(block->blockHeader()->number()), entry);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_NUMBER_2_BLOCK));
    }
}
void Ledger::writeNumber2BlockHeader(
    const Block::Ptr& _block, bcos::storage::TableFactoryInterface::Ptr _tableFactory)
{
    TableInterface::Ptr table = _tableFactory->openTable(SYS_NUMBER_2_BLOCK_HEADER);

    if(table)
    {
        auto entry = table->newEntry();
        // encode and write the block header into SYS_VALUE field
        auto encodedBlockHeader = std::make_shared<bytes>();
        _block->blockHeader()->encode(*encodedBlockHeader);
        entry->setField(SYS_VALUE, asString(*encodedBlockHeader));

        // encode and write the sigList into the SYS_SIG_LIST field
        auto encodedSigList = std::make_shared<bytes>(); // TODO: encode sign list
        entry->setField(SYS_SIG_LIST, asString(*encodedSigList));
        table->setRow(boost::lexical_cast<std::string>(_block->blockHeader()->number()), entry);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_NUMBER_2_BLOCK_HEADER));
    }

}
void Ledger::writeTotalTransactionCount(
    const Block::Ptr& block, bcos::storage::TableFactoryInterface::Ptr _tableFactory)
{
    auto tb = _tableFactory->openTable(SYS_CURRENT_STATE);
    if (tb)
    {
        auto entry = tb->getRow(SYS_KEY_TOTAL_TRANSACTION_COUNT);
        if (entry != nullptr)
        {
            auto currentCount = boost::lexical_cast<int64_t>(entry->getField(SYS_VALUE));
            currentCount += block->transactions()->size();

            auto updateEntry = tb->newEntry();
            updateEntry->setField(SYS_VALUE, boost::lexical_cast<std::string>(currentCount));
            // TODO: judge setRow result
            tb->setRow(SYS_KEY_TOTAL_TRANSACTION_COUNT, updateEntry);
        }
        else
        {
            auto insertEntry = tb->newEntry();
            entry->setField(SYS_VALUE, boost::lexical_cast<std::string>(block->transactions()->size()));
            // TODO: judge setRow result
            tb->setRow(SYS_KEY_TOTAL_TRANSACTION_COUNT, insertEntry);
        }
        auto receipts = block->receipts();
        int64_t failedTransactions = 0;
        for (auto& receipt : *receipts)
        {
            // TODO: check receipt status
            if (receipt->status() != 0)
            {
                ++failedTransactions;
            }
        }
        auto getFailedEntry = tb->getRow(SYS_KEY_TOTAL_FAILED_TRANSACTION);
        if (getFailedEntry != nullptr)
        {
            auto currentCount = boost::lexical_cast<int64_t>(getFailedEntry->getField(SYS_VALUE));
            currentCount += failedTransactions;
            auto updateEntry = tb->newEntry();
            updateEntry->setField(SYS_VALUE, boost::lexical_cast<std::string>(currentCount));
            tb->setRow(SYS_KEY_TOTAL_FAILED_TRANSACTION, updateEntry);
        }
        else
        {
            auto newFailedEntry = tb->newEntry();
            newFailedEntry->setField(SYS_VALUE, boost::lexical_cast<std::string>(failedTransactions));
            tb->setRow(SYS_KEY_TOTAL_FAILED_TRANSACTION, newFailedEntry);
        }
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CURRENT_STATE));
    }
}
void Ledger::writeNumber2Transactions(
    const Block::Ptr& _block, const BlockNumber& _number, TableFactoryInterface::Ptr _tableFactory)
{
    TableInterface::Ptr table = _tableFactory->openTable(SYS_NUMBER_2_TXS);
    if (table)
    {
        auto entry = table->newEntry();
        writeBlockToField(_block, entry);
        table->setRow(boost::lexical_cast<std::string>(_number), entry);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_NUMBER_2_TXS));
    }
}
void Ledger::writeNumber2Receipts(const bcos::protocol::Block::Ptr& _block,
    const BlockNumber& _number, bcos::storage::TableFactoryInterface::Ptr _tableFactory)
{
    TableInterface::Ptr table = _tableFactory->openTable(SYS_NUMBER_2_RECEIPTS);
    if (table)
    {
        auto entry = table->newEntry();
        writeBlockToField(_block, entry);
        table->setRow(boost::lexical_cast<std::string>(_number), entry);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_NUMBER_2_RECEIPTS));
    }
}