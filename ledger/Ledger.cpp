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

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;
using namespace bcos::storage;

void Ledger::asyncCommitBlock(bcos::protocol::BlockNumber _blockNumber,
                              bcos::protocol::SignatureListPtr _signList, std::function<void(Error::Ptr)> _onCommitBlock)
{}
void Ledger::asyncGetTxByHash(const bcos::crypto::HashType& _txHash,
    std::function<void(Error::Ptr, bcos::protocol::Transaction::ConstPtr)> _onGetTx)
{}
void Ledger::asyncGetTransactionReceiptByHash(const bcos::crypto::HashType& _txHash,
    std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr)> _onGetTx)
{}
void Ledger::asyncPreStoreTxs(
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
    UpgradableGuard ul(m_blockNumberMutex);
    if (m_blockNumber == -1)
    {
        bcos::protocol::BlockNumber num = getLatestBlockNumber();
        UpgradeGuard l(ul);
        m_blockNumber = num;
    }
    _onGetBlock(nullptr, m_blockNumber);
    //TODO: Error
}
void Ledger::asyncGetBlockHashByNumber(bcos::protocol::BlockNumber number,
    std::function<void(Error::Ptr, std::shared_ptr<const bcos::crypto::HashType>)> _onGetBlock)
{}
void Ledger::asyncGetBlockByHash(
    const bcos::crypto::HashType& _blockHash, std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
{}
void Ledger::asyncGetBlockByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
{}
void Ledger::asyncGetBlockEncodedByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, std::shared_ptr<const bytes>)> _onGetBlock)
{}
void Ledger::asyncGetBlockHeaderByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, std::shared_ptr<const std::pair<bcos::protocol::BlockHeader::Ptr,
                                       bcos::protocol::SignatureListPtr>>)>
        _onGetBlock)
{}
void Ledger::asyncGetBlockHeaderByHash(const bcos::crypto::HashType& _blockHash,
    std::function<void(Error::Ptr, std::shared_ptr<const std::pair<bcos::protocol::BlockHeader::Ptr,
                                       bcos::protocol::SignatureListPtr>>)>
        _onGetBlock)
{}
void Ledger::asyncGetCode(const std::string& _tableID, bcos::Address _codeAddress,
    std::function<void(Error::Ptr, std::shared_ptr<const bytes>)> _onGetCode)
{}
void Ledger::asyncGetSystemConfigByKey(const std::string& _key,
    std::function<void(
        Error::Ptr, std::shared_ptr<const std::pair<std::string, bcos::protocol::BlockNumber>>)>
        _onGetConfig)
{}
void Ledger::asyncGetNonceList(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, bcos::protocol::NonceListPtr)> _onGetList)
{}

Block::Ptr Ledger::getBlock(BlockNumber const& _blockNumber)
{
    if (_blockNumber > getLatestBlockNumber())
    {
        return nullptr;
    }
    auto blockHash = getBlockHashByNumber(_blockNumber);
    if (*blockHash != bcos::crypto::HashType(""))
    {
        return getBlock(*blockHash, _blockNumber);
    }
    LEDGER_LOG(WARNING) << LOG_DESC("[getBlock]Can't find block")
                            << LOG_KV("number", _blockNumber);
    return nullptr;
}
Block::Ptr Ledger::getBlock(const bcos::crypto::HashType& _blockHash, bcos::protocol::BlockNumber const& _blockNumber)
{
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockHash);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.first))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Cache hit, read from cache")
                              << LOG_KV("blockNumber", _blockNumber)
                              << LOG_KV("hash", _blockHash.abridged());
        return cachedBlock.first;
    }    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Cache missed, read from storage")
                              << LOG_KV("blockNumber", _blockNumber);
        TableInterface::Ptr tb = getMemoryTableFactory(_blockNumber)->openTable(SYS_HASH_2_BLOCK);
        auto openTable_time_cost = utcTime() - record_time;
        record_time = utcTime();
        if (tb)
        {
            auto entries = tb->select(_blockHash.hex(), tb->newCondition());
            auto select_time_cost = utcTime() - record_time;
            record_time = utcTime();
            if (entries->size() > 0)
            {
                auto entry = entries->get(0);

                record_time = utcTime();
                // use binary block since v2.2.0
                auto block = decodeBlock(entry);

                auto constructBlock_time_cost = utcTime() - record_time;
                record_time = utcTime();

                BLOCKCHAIN_LOG(TRACE) << LOG_DESC("[#getBlock]Write to cache");
                auto blockPtr = m_blockCache.add(block);
                auto addCache_time_cost = utcTime() - record_time;
                BLOCKCHAIN_LOG(DEBUG) << LOG_DESC("Get block from db")
                                      << LOG_KV("getCacheTimeCost", getCache_time_cost)
                                      << LOG_KV("openTableTimeCost", openTable_time_cost)
                                      << LOG_KV("selectTimeCost", select_time_cost)
                                      << LOG_KV("constructBlockTimeCost", constructBlock_time_cost)
                                      << LOG_KV("addCacheTimeCost", addCache_time_cost)
                                      << LOG_KV("totalTimeCost", utcTime() - start_time);
                return blockPtr;
            }
        }

        BLOCKCHAIN_LOG(TRACE) << LOG_DESC("[#getBlock]Can't find the block")
                              << LOG_KV("blockNumber", _blockNumber)
                              << LOG_KV("blockHash", _blockHash);
        return nullptr;
    }

    return bcos::protocol::Block::Ptr();
}

std::shared_ptr<bcos::crypto::HashType> Ledger::getBlockHashByNumber(
    bcos::protocol::BlockNumber _number) const
{
    return std::shared_ptr<bcos::crypto::HashType>();
}

bcos::protocol::BlockNumber Ledger::getBlockNumberByHash(bcos::crypto::HashType const& _hash)
{
    return 0;
}
BlockNumber Ledger::getLatestBlockNumber()
{
    int64_t num = 0;
    auto tb = m_tableFactory->openTable(SYS_CURRENT_STATE, false);
    if (tb)
    {
        auto entry = tb->getRow(SYS_KEY_CURRENT_NUMBER);
        std::string currentNumber = entry->getField(SYS_VALUE);
        num = boost::lexical_cast<BlockNumber>(currentNumber);
    }
    return num;
}
BlockHeader::Ptr Ledger::getBlockHeaderFromBlock(Block::Ptr _block)
{
    return bcos::protocol::BlockHeader::Ptr();
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
    const std::map<std::string, std::vector<std::string>>& parent2ChildList,
    const Child2ParentMap& child2Parent,
    std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>& merkleProof)
{}
void Ledger::writeBytesToField(
    std::shared_ptr<bytes> _data, bcos::storage::EntryInterface::Ptr _entry, const std::string& _fieldName)
{}
void Ledger::writeBlockToField(const Block& _block, bcos::storage::EntryInterface::Ptr _entry) {}
void Ledger::writeNumber(
    const Block& block, bcos::storage::TableFactory::Ptr _tableFactory)
{}
void Ledger::writeTxToBlock(const Block& block, bcos::storage::TableFactory::Ptr _tableFactory) {}
void Ledger::writeNumber2Hash(const Block& block, bcos::storage::TableFactory::Ptr _tableFactory) {}
void Ledger::writeHash2Block(Block& block, bcos::storage::TableFactory::Ptr _tableFactory) {}
void Ledger::writeHash2BlockHeader(Block& _block, bcos::storage::TableFactory::Ptr _tableFactory) {}
void Ledger::writeTotalTransactionCount(
    const Block& block, bcos::storage::TableFactory::Ptr _tableFactory)
{}
