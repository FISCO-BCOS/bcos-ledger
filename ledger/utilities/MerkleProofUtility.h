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

#include <bcos-framework/libcodec/scale/Scale.h>
#include <bcos-framework/libprotocol/ParallelMerkleProof.h>
#include <tbb/parallel_for.h>

namespace bcos::ledger
{
template <typename T>
void encodeToCalculateMerkle(std::vector<bytes>& _encodedList, T _protocolDataList)
{
    auto protocolDataSize = _protocolDataList->size();
    _encodedList.resize(protocolDataSize);
    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, protocolDataSize), [&](const tbb::blocked_range<size_t>& _r) {
            for (auto i = _r.begin(); i < _r.end(); ++i)
            {
                bcos::codec::scale::ScaleEncoderStream stream;
                stream << i;
                bytes encodedData = stream.data();
                auto hash = ((*_protocolDataList)[i])->hash();
                encodedData.insert(encodedData.end(), hash.begin(), hash.end());
                _encodedList[i] = std::move(encodedData);
            }
        });
}

std::shared_ptr<Parent2ChildListMap> getTransactionProof(crypto::CryptoSuite::Ptr _crypto, protocol::TransactionsPtr _txs)
{
    auto merklePath = std::make_shared<Parent2ChildListMap>();
    std::vector<bytes> transactionList;
    encodeToCalculateMerkle(transactionList, _txs);
    protocol::calculateMerkleProof(_crypto,transactionList,merklePath);
    return merklePath;
}

std::shared_ptr<Parent2ChildListMap> getReceiptProof(crypto::CryptoSuite::Ptr _crypto, protocol::ReceiptsPtr _receipts){
    auto merklePath = std::make_shared<Parent2ChildListMap>();
    std::vector<bytes> receiptList;
    encodeToCalculateMerkle(receiptList, _receipts);
    protocol::calculateMerkleProof(_crypto,receiptList,merklePath);
    return merklePath;
}

void getMerkleProof(const crypto::HashType& _txHash,
                            const Parent2ChildListMap & parent2ChildList,
                            const Child2ParentMap& child2Parent,
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

void parseMerkleMap(
    const std::shared_ptr<Parent2ChildListMap>& parent2ChildList,
    Child2ParentMap& child2Parent)
{
    // trans parent2ChildList into child2Parent concurrently
    tbb::parallel_for_each(parent2ChildList->begin(), parent2ChildList->end(),
        [&](std::pair<const std::string, std::vector<std::string>>& _childListIterator) {
            auto childList = _childListIterator.second;
            auto parent = _childListIterator.first;
            tbb::parallel_for(tbb::blocked_range<size_t>(0, childList.size()),
                [&](const tbb::blocked_range<size_t>& range) {
                    for (size_t i = range.begin(); i < range.end(); i++)
                    {
                        std::string child = childList[i];
                        if (!child.empty())
                        {
                            child2Parent[child] = parent;
                        }
                    }
                });
        });
}

} // namespace bcos::ledger