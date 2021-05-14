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
 * @file FIFOCacheTest.cpp
 * @author: kyonRay
 * @date 2021-04-14
 */

#include "../ledger/utilities/FIFOCache.h"
#include "unittests/ledger/common/FakeBlock.h"
#include <bcos-framework/testutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

namespace bcos::test
{
BOOST_FIXTURE_TEST_SUITE(FIFOCacheTest, TestPromptFixture)

BOOST_AUTO_TEST_CASE(testBlockCacheAdd)
{
    FIFOCache<Block::Ptr, Block> _blockFIFOCache;
    auto blockDestructorThread = std::make_shared<ThreadPool>("blockCache", 1);
    _blockFIFOCache.setDestructorThread(blockDestructorThread);

    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto block1 = fakeBlock(crypto, blockFactory, 10, 10);
    _blockFIFOCache.add(block1->blockHeader()->number(), block1);
    auto block1_get = _blockFIFOCache.get(block1->blockHeader()->number());
    BOOST_CHECK_EQUAL(block1_get.first, block1->blockHeader()->number());
    BOOST_CHECK_EQUAL(block1_get.second->transactionsSize(), 10);
}

BOOST_AUTO_TEST_CASE(testBlockCacheAddMax)
{
    FIFOCache<Block::Ptr, Block> _blockFIFOCache;
    auto blockDestructorThread = std::make_shared<ThreadPool>("blockCache", 1);
    _blockFIFOCache.setDestructorThread(blockDestructorThread);

    auto crypto = createCryptoSuite();
    auto blockFactory = createBlockFactory(crypto);
    auto blocks = fakeBlocks(crypto, blockFactory, 1, 1, 11);
    for (auto & block : *blocks)
    {
        _blockFIFOCache.add(block->blockHeader()->number(), block);
    }
    auto block1_get = _blockFIFOCache.get(blocks->at(0)->blockHeader()->number());

    BOOST_CHECK_EQUAL(block1_get.first, -1);
    BOOST_CHECK(block1_get.second == nullptr);
    auto block2_get = _blockFIFOCache.get(blocks->at(1)->blockHeader()->number());
    BOOST_CHECK_EQUAL(block2_get.second->transactionsSize(), 2);
}

BOOST_AUTO_TEST_CASE(testGetEmpty)
{
    FIFOCache<Block::Ptr, Block> _blockFIFOCache;
    auto blockDestructorThread = std::make_shared<ThreadPool>("blockCache", 1);
    _blockFIFOCache.setDestructorThread(blockDestructorThread);

    auto block1_get = _blockFIFOCache.get(0);
    BOOST_CHECK_EQUAL(block1_get.first, -1);
    BOOST_CHECK(block1_get.second == nullptr);
}

BOOST_AUTO_TEST_SUITE_END()
}  // namespace bcos