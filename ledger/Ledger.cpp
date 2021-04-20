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

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

void Ledger::asyncCommitBlock(
    bcos::protocol::BlockNumber _blockNumber, std::function<void(Error::Ptr)> _onCommitBlock)
{}
void Ledger::asyncGetTxByHash(const h256& _txHash,
    std::function<void(Error::Ptr, bcos::protocol::Transaction::ConstPtr)> _onGetTx)
{}
void Ledger::asyncGetTransactionReceiptByHash(const h256& _txHash,
    std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr)> _onGetTx)
{}
void Ledger::asyncGetTotalTransactionCount(std::function<void(
        Error::Ptr, std::shared_ptr<std::pair<int64_t, bcos::protocol::BlockNumber>>)>
        _callback)
{}
void Ledger::asyncGetTotalFailedTransactionCount(std::function<void(
        Error::Ptr, std::shared_ptr<const std::pair<int64_t, bcos::protocol::BlockNumber>>)>
        _callback)
{}
void Ledger::asyncGetTransactionReceiptProof(const bcos::protocol::Block::Ptr _block,
    const uint64_t& _index, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{}
void Ledger::getTransactionProof(const bcos::protocol::Block::Ptr _block, const uint64_t& _index,
    std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{}
void Ledger::asyncGetTransactionProofByHash(
    const h256& _txHash, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{}
void Ledger::asyncGetBlockNumber(
    std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock)
{}
void Ledger::asyncGetBlockHashByNumber(bcos::protocol::BlockNumber number,
    std::function<void(Error::Ptr, std::shared_ptr<const h256>)> _onGetBlock)
{}
void Ledger::asyncGetBlockByHash(
    const h256& _blockHash, std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
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
void Ledger::asyncGetBlockHeaderByHash(const h256& _blockHash,
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

Block::Ptr Ledger::getBlock(BlockNumber _number)
{
    return bcos::protocol::Block::Ptr();
}
Block::Ptr Ledger::getBlock(const h256& _blockHash)
{
    return bcos::protocol::Block::Ptr();
}
BlockNumber Ledger::getLatestBlockNumber()
{
    return 0;
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
    std::shared_ptr<bytes> _data, bcos::storage::Entry::Ptr _entry, const std::string& _fieldName)
{}
void Ledger::writeBlockToField(const Block& _block, bcos::storage::Entry::Ptr _entry) {}
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
