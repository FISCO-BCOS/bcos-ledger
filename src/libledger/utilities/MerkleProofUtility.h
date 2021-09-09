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
#include <tbb/concurrent_vector.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_for_each.h>

namespace bcos::ledger
{
class MerkleProofUtility
{
public:
    using Ptr = std::shared_ptr<MerkleProofUtility>;

    template <typename T>
    std::shared_ptr<Parent2ChildListMap> getParent2ChildList(
        crypto::CryptoSuite::Ptr _crypto, T _ts)
    {
        auto merklePath = std::make_shared<Parent2ChildListMap>();
        tbb::concurrent_vector<bytes> tsVector;
        tsVector.resize(_ts->size());
        tbb::parallel_for(
            tbb::blocked_range<size_t>(0, _ts->size()), [&](const tbb::blocked_range<size_t>& _r) {
                for (uint32_t i = _r.begin(); i < _r.end(); ++i)
                {
                    crypto::HashType hash = ((*_ts)[i])->hash();
                    tsVector[i] = hash.asBytes();
                }
            });
        auto tsList = std::vector<bytes>(tsVector.begin(), tsVector.end());
        protocol::calculateMerkleProof(_crypto, tsList, merklePath);
        return merklePath;
    }

    void getMerkleProof(const crypto::HashType& _txHash,
        const Parent2ChildListMap& parent2ChildList, const Child2ParentMap& child2Parent,
        MerkleProof& merkleProof);

    std::shared_ptr<Child2ParentMap> getChild2Parent(
        std::shared_ptr<Parent2ChildListMap> _parent2Child);
};


}  // namespace bcos::ledger