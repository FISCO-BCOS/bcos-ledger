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
{}
void Ledger::asyncGetTransactionReceiptByHash(const bcos::crypto::HashType& _txHash,
    std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr)> _onGetTx)
{}
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
    UpgradableGuard ul(m_blockNumberMutex);
    if (m_blockNumber == -1)
    {
        bcos::protocol::BlockNumber num = getLatestBlockNumber();
        UpgradeGuard l(ul);
        m_blockNumber = num;
        if(num == -1){
            // TODO: to add errorCode and message
            auto error = std::make_shared<Error>(-1, "");
            _onGetBlock(error, -1);
        }
    }
    _onGetBlock(nullptr, m_blockNumber);
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

Block::Ptr Ledger::getBlock(BlockNumber const& _blockNumber, bcos::crypto::HashType const& _blockHash)
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
                auto blockPtr = m_blockCache.add(block);
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
                          << LOG_KV("blockNumber", _blockNumber)
                          << LOG_KV("blockHash", _blockHash);
        return nullptr;
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
            return getBlock(blockNumber, _blockHash);
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
}
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
            record_time = utcTime();
            if (entry != nullptr)
            {
                record_time = utcTime();
                auto encodedBlock = std::make_shared<bytes>(asBytes(entry->getField(SYS_VALUE)));

                auto encode_time_cost = utcTime() - record_time;

                LEDGER_LOG(DEBUG) << LOG_DESC("Get block RLP from db")
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
    auto tb = m_tableFactory->openTable(SYS_HASH_2_NUMBER, false);
    if (tb)
    {
        auto entry = tb->getRow(boost::lexical_cast<std::string>(_hash));
        number = boost::lexical_cast<BlockNumber>(entry->getField(SYS_VALUE));
    }
    return number;
}

BlockNumber Ledger::getLatestBlockNumber()
{
    BlockNumber num = -1;
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

bcos::protocol::Block::Ptr Ledger::decodeBlock(bcos::storage::EntryInterface::ConstPtr _entry)
{
    Block::Ptr block = nullptr;
    auto _blockStr = _entry->getFieldConst(SYS_VALUE);
    block = m_blockFactory->createBlock(bytes((unsigned char*)_blockStr.data(),
                                            (unsigned char*)(_blockStr.data() + _blockStr.size())) ,
        false, false);
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

void Ledger::writeBlockToField(const Block::Ptr& _block, bcos::storage::EntryInterface::Ptr _entry)
{
    auto encodeBlock = std::make_shared<bytes>();
    _block->encode(*encodeBlock);
    writeBytesToField(_entry, SYS_VALUE, bytesConstRef(encodeBlock->data(), encodeBlock->size()));
}

void Ledger::writeBytesToField(bcos::storage::EntryInterface::Ptr _entry, const std::string& _key, bytesConstRef _bytesValue)
{
    //TODO: check bytesConstRef::toString
    _entry->setField(_key, _bytesValue.toString());
}

void Ledger::writeNumber(
    const Block::Ptr& block, bcos::storage::TableFactory::Ptr _tableFactory)
{
    auto tb = _tableFactory->openTable(SYS_CURRENT_STATE, false);
    if (tb)
    {
        auto entry = tb->getRow(SYS_KEY_CURRENT_NUMBER); //FIXME: use new entry instance
        entry->setField(SYS_VALUE, boost::lexical_cast<std::string>(block->blockHeader()->number()));
        // TODO: judge setRow result
        tb->setRow(SYS_KEY_CURRENT_NUMBER, entry);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CURRENT_STATE));
    }
}
void Ledger::writeTxToBlock(const Block::Ptr& block, bcos::storage::TableFactory::Ptr _tableFactory)
{
    auto start_time = utcTime();
    auto record_time = utcTime();
    TableInterface::Ptr tb = _tableFactory->openTable(SYS_TX_HASH_2_BLOCK_NUMBER, false);
    TableInterface::Ptr tb_nonces = _tableFactory->openTable(SYS_BLOCK_NUMBER_2_NONCES, false);
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
                                      //Entry::Ptr entry = std::make_shared<Entry>();
                                      auto entry = tb->getRow(SYS_VALUE); // FIXME: use new entry instance

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
              // TODO: encode nonce list to bytes
              std::shared_ptr<bytes> nonceData = std::make_shared<bytes>();
              // Entry::Ptr entry_tb2nonces = std::make_shared<Entry>();
              auto entry_tb2nonces = tb_nonces->getRow(SYS_VALUE); // FIXME: use new entry instance

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
void Ledger::writeHash2Number(const Block::Ptr& block, bcos::storage::TableFactory::Ptr _tableFactory)
{
    TableInterface::Ptr tb = _tableFactory->openTable(SYS_HASH_2_NUMBER, false);
    if (tb)
    {
        // Entry::Ptr entry = std::make_shared<Entry>();
        auto entry = tb->getRow(SYS_VALUE); // FIXME: use new entry instance
        entry->setField(SYS_VALUE, boost::lexical_cast<std::string>(block->blockHeader()->number()));
        tb->setRow(block->blockHeader()->hash().hex(), entry);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_HASH_2_NUMBER));
    }
}
void Ledger::writeNumber2Block(const Block::Ptr& block, bcos::storage::TableFactory::Ptr _tableFactory)
{
    TableInterface::Ptr tb = _tableFactory->openTable(SYS_NUMBER_2_BLOCK, false);
    if (tb)
    {
        // Entry::Ptr entry = std::make_shared<Entry>();
        auto entry = tb->getRow(SYS_NUMBER_2_BLOCK); // FIXME: use new entry instance
        writeBlockToField(block, entry);
        tb->setRow(boost::lexical_cast<std::string>(block->blockHeader()->number()), entry);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_NUMBER_2_BLOCK));
    }
}
void Ledger::writeNumber2BlockHeader(const Block::Ptr& _block, bcos::storage::TableFactory::Ptr _tableFactory)
{
    TableInterface::Ptr table = _tableFactory->openTable(SYS_NUMBER_2_BLOCK_HEADER, false);

    if(table)
    {
        //Entry::Ptr entry = std::make_shared<Entry>();
        auto entry = table->getRow(SYS_VALUE); // FIXME: use new entry instance
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
    const Block::Ptr& block, bcos::storage::TableFactory::Ptr _tableFactory)
{
    auto tb = _tableFactory->openTable(SYS_CURRENT_STATE, false);
    if (tb)
    {
        auto entry = tb->getRow(SYS_KEY_TOTAL_TRANSACTION_COUNT);
        if (entry != nullptr)
        {
            auto currentCount = boost::lexical_cast<int64_t>(entry->getField(SYS_VALUE));
            currentCount += block->transactions()->size();

            // auto updateEntry = tb->newEntry();
            auto updateEntry = entry; // FIXME: use new entry instance
            updateEntry->setField(SYS_VALUE, boost::lexical_cast<std::string>(currentCount));
            // TODO: judge setRow result
            tb->setRow(SYS_KEY_TOTAL_TRANSACTION_COUNT, updateEntry);
        }
        else
        {
            //auto insertEntry = tb->newEntry();
            auto insertEntry = entry; // FIXME: use new entry instance
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
            //auto updateEntry = tb->newEntry();
            auto updateEntry = getFailedEntry; // FIXME: use new entry instance
            updateEntry->setField(SYS_VALUE, boost::lexical_cast<std::string>(currentCount));
            tb->setRow(SYS_KEY_TOTAL_FAILED_TRANSACTION, updateEntry);
        }
        else
        {

            //auto newFailedEntry = tb->newEntry();
            auto newFailedEntry = getFailedEntry; // FIXME: use new entry instance
            newFailedEntry->setField(SYS_VALUE, boost::lexical_cast<std::string>(failedTransactions));
            tb->setRow(SYS_KEY_TOTAL_FAILED_TRANSACTION, newFailedEntry);
        }
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CURRENT_STATE));
    }
}