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
 * @file FakeBlock.h
 * @author: kyonRay
 * @date 2021-04-14
 */

#pragma once
#include "unittests/common/MockBlockFactory.h"
#include "unittests/ledger/FakeBlockHeader.h"
#include <bcos-framework/interfaces/protocol/BlockFactory.h>
#include <unittests/common/MockBlockHeaderFactory.h>

namespace bcos::test
{

inline bcos::protocol::BlockFactory::Ptr createBlockFactory()
{
    auto blockHeaderFactory = std::make_shared<MockBlockHeaderFactory>();
    return std::make_shared<MockBlockFactory>(blockHeaderFactory);
}

inline bcos::protocol::Block::Ptr fakeBlock(bcos::protocol::BlockFactory::Ptr _blockFactory, size_t _txsHashNum, size_t _receiptsHashNum)
{
    auto block = _blockFactory->createBlock();
    auto blockHeader = getBlockHeader();
    block->setBlockHeader(blockHeader);
    // fake txsHash
    for (size_t i = 0; i < _txsHashNum; i++)
    {
        auto hash = h256(1234);
        block->appendTransactionHash(hash);
    }
    // fake receiptsHash
    for (size_t i = 0; i < _receiptsHashNum; i++)
    {
        auto hash = h256(324);
        block->appendReceiptHash(hash);
    }
    return block;
}



} // namespace bcos
