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
 * @file MerkleProofUtility.h
 * @author: kyonRay
 * @date 2021-04-29
 */

#pragma once

#include "Common.h"
#include <bcos-framework/interfaces/ledger/LedgerTypeDef.h>
#include <bcos-framework/libcodec/scale/Scale.h>
#include <bcos-framework/libprotocol/ParallelMerkleProof.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_for_each.h>

namespace bcos::ledger
{
class MerkleProofUtility
{
public:
    using Ptr = std::shared_ptr<MerkleProofUtility>;
    template <typename T>
    void encodeToCalculateMerkle(std::vector<bytes>& _encodedList, T _protocolDataList)
    {
        auto protocolDataSize = _protocolDataList->size();
        _encodedList.resize(protocolDataSize);
        tbb::parallel_for(tbb::blocked_range<size_t>(0, protocolDataSize),
            [&](const tbb::blocked_range<size_t>& _r) {
                for (uint32_t i = _r.begin(); i < _r.end(); ++i)
                {
                    bytes encodedData;
                    auto hash = ((*_protocolDataList)[i])->hash();
                    encodedData.insert(encodedData.end(), hash.begin(), hash.end());
                    _encodedList[i] = std::move(encodedData);
                }
            });
    }

    std::shared_ptr<Parent2ChildListMap> getTransactionProof(
        crypto::CryptoSuite::Ptr _crypto, protocol::TransactionsPtr _txs);

    std::shared_ptr<Parent2ChildListMap> getReceiptProof(
        crypto::CryptoSuite::Ptr _crypto, protocol::ReceiptsPtr _receipts);

    void getMerkleProof(const crypto::HashType& _txHash,
        const Parent2ChildListMap& parent2ChildList, const Child2ParentMap& child2Parent,
        MerkleProof& merkleProof);

    void parseMerkleMap(const std::shared_ptr<Parent2ChildListMap>& parent2ChildList,
        Child2ParentMap& child2Parent);

    std::shared_ptr<Child2ParentMap> getChild2ParentCacheByReceipt(
        std::shared_ptr<Parent2ChildListMap> _parent2ChildList, protocol::BlockNumber _blockNumber);
    std::shared_ptr<Child2ParentMap> getChild2ParentCacheByTransaction(
        std::shared_ptr<Parent2ChildListMap> _parent2Child, protocol::BlockNumber _blockNumber);

    std::shared_ptr<Child2ParentMap> getChild2ParentCache(SharedMutex& _mutex,
        std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>>& _cache,
        std::shared_ptr<Parent2ChildListMap> _parent2Child, protocol::BlockNumber _blockNumber);

    std::shared_ptr<Parent2ChildListMap> getParent2ChildListByReceiptProofCache(
        protocol::BlockNumber _blockNumber, protocol::ReceiptsPtr _receipts,
        crypto::CryptoSuite::Ptr _crypto);

    std::shared_ptr<Parent2ChildListMap> getParent2ChildListByTxsProofCache(
        protocol::BlockNumber _blockNumber, protocol::TransactionsPtr _txs,
        crypto::CryptoSuite::Ptr _crypto);

private:
    std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Parent2ChildListMap>>
        m_transactionWithProof = std::make_pair(0, nullptr);
    mutable SharedMutex m_transactionWithProofMutex;

    std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Parent2ChildListMap>>
        m_receiptWithProof = std::make_pair(0, nullptr);
    mutable SharedMutex m_receiptWithProofMutex;

    std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>>
        m_receiptChild2ParentCache;
    mutable SharedMutex x_receiptChild2ParentCache;

    std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>> m_txsChild2ParentCache;
    mutable SharedMutex x_txsChild2ParentCache;
};


}  // namespace bcos::ledger