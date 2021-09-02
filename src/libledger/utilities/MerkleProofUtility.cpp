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
 * @file MerkleProofUtility.cpp
 * @author: kyonRay
 * @date 2021-05-24
 */

#include "MerkleProofUtility.h"

using namespace bcos;
using namespace bcos::ledger;

namespace bcos
{
namespace ledger
{
std::shared_ptr<Parent2ChildListMap> MerkleProofUtility::getTransactionProof(
    crypto::CryptoSuite::Ptr _crypto, protocol::TransactionsPtr _txs)
{
    auto merklePath = std::make_shared<Parent2ChildListMap>();
    std::vector<bytes> transactionList;
    encodeToCalculateMerkle(transactionList, _txs);
    protocol::calculateMerkleProof(_crypto, transactionList, merklePath);
    return merklePath;
}

std::shared_ptr<Parent2ChildListMap> MerkleProofUtility::getReceiptProof(
    crypto::CryptoSuite::Ptr _crypto, protocol::ReceiptsPtr _receipts)
{
    auto merklePath = std::make_shared<Parent2ChildListMap>();
    std::vector<bytes> receiptList;
    encodeToCalculateMerkle(receiptList, _receipts);
    protocol::calculateMerkleProof(_crypto, receiptList, merklePath);
    return merklePath;
}

void MerkleProofUtility::getMerkleProof(const crypto::HashType& _txHash,
    const Parent2ChildListMap& parent2ChildList, const Child2ParentMap& child2Parent,
    MerkleProof& merkleProof)
{
    std::string merkleNode = _txHash.hex();
    // get child=>parent info
    auto itChild2Parent = child2Parent.find(merkleNode);
    while (itChild2Parent != child2Parent.end())
    {
        // find parent=>childrenList info
        auto itParent2ChildList = parent2ChildList.find(itChild2Parent->second);
        if (itParent2ChildList == parent2ChildList.end())
        {
            break;
        }
        // get index from itParent2ChildList->second by merkleNode
        auto itChildList = std::find(
            itParent2ChildList->second.begin(), itParent2ChildList->second.end(), merkleNode);
        if (itChildList == itParent2ChildList->second.end())
        {
            break;
        }
        // leftPath = [childrenList.begin, index)
        std::vector<std::string> leftPath{};
        // rightPath = (index, childrenList.end]
        std::vector<std::string> rightPath{};
        leftPath.insert(leftPath.end(), itParent2ChildList->second.begin(), itChildList);
        rightPath.insert(rightPath.end(), std::next(itChildList), itParent2ChildList->second.end());

        auto singleTree = std::make_pair(std::move(leftPath), std::move(rightPath));
        merkleProof.emplace_back(singleTree);

        // node=parent
        merkleNode = itChild2Parent->second;
        itChild2Parent = child2Parent.find(merkleNode);
    }
}

void MerkleProofUtility::parseMerkleMap(
    std::shared_ptr<Parent2ChildListMap> parent2ChildList, Child2ParentMap& child2Parent)
{
    // trans parent2ChildList into child2Parent concurrently
    tbb::parallel_for_each(parent2ChildList->begin(), parent2ChildList->end(),
        [&](std::pair<const std::string, std::vector<std::string>>& _childListIterator) {
            tbb::parallel_for(tbb::blocked_range<size_t>(0, _childListIterator.second.size()),
                [&](const tbb::blocked_range<size_t>& range) {
                    for (size_t i = range.begin(); i < range.end(); i++)
                    {
                        std::string child = _childListIterator.second[i];
                        if (!child.empty())
                        {
                            child2Parent[child] = _childListIterator.first;
                        }
                    }
                });
        });
}


std::shared_ptr<Child2ParentMap> MerkleProofUtility::getChild2ParentCacheByReceipt(
    std::shared_ptr<Parent2ChildListMap> _parent2ChildList, protocol::BlockNumber _blockNumber)
{
    return getChild2ParentCache(
        x_receiptChild2ParentCache, m_receiptChild2ParentCache, _parent2ChildList, _blockNumber);
}

std::shared_ptr<Child2ParentMap> MerkleProofUtility::getChild2ParentCacheByTransaction(
    std::shared_ptr<Parent2ChildListMap> _parent2Child, protocol::BlockNumber _blockNumber)
{
    return getChild2ParentCache(
        x_txsChild2ParentCache, m_txsChild2ParentCache, _parent2Child, _blockNumber);
}

std::shared_ptr<Child2ParentMap> MerkleProofUtility::getChild2ParentCache(SharedMutex& _mutex,
    std::pair<protocol::BlockNumber, std::shared_ptr<Child2ParentMap>>& _cache,
    std::shared_ptr<Parent2ChildListMap> _parent2Child, protocol::BlockNumber _blockNumber)
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
    auto child2Parent = std::make_shared<Child2ParentMap>();
    parseMerkleMap(_parent2Child, *child2Parent);
    _cache = std::make_pair(_blockNumber, child2Parent);
    return child2Parent;
}

std::shared_ptr<Parent2ChildListMap> MerkleProofUtility::getParent2ChildListByReceiptProofCache(
    protocol::BlockNumber _blockNumber, protocol::ReceiptsPtr _receipts,
    crypto::CryptoSuite::Ptr _crypto)
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

    auto parent2ChildList = getReceiptProof(_crypto, _receipts);
    m_receiptWithProof = std::make_pair(_blockNumber, parent2ChildList);
    return parent2ChildList;
}

std::shared_ptr<Parent2ChildListMap> MerkleProofUtility::getParent2ChildListByTxsProofCache(
    protocol::BlockNumber _blockNumber, protocol::TransactionsPtr _txs,
    crypto::CryptoSuite::Ptr _crypto)
{
    UpgradableGuard l(m_transactionWithProofMutex);
    // cache for the block parent2ChildList
    if (m_transactionWithProof.second && m_transactionWithProof.first == _blockNumber)
    {
        return m_transactionWithProof.second;
    }
    UpgradeGuard ul(l);
    // After preempting the write lock, judge again whether m_transactionWithProof has been
    // updated to prevent lock competition
    if (m_transactionWithProof.second && m_transactionWithProof.first == _blockNumber)
    {
        return m_transactionWithProof.second;
    }
    auto parent2ChildList = getTransactionProof(_crypto, _txs);
    m_transactionWithProof = std::make_pair(_blockNumber, parent2ChildList);
    return parent2ChildList;
}

}  // namespace ledger
}  // namespace bcos
