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
 * @file BlockCacheTest.cpp
 * @author: kyonRay
 * @date 2021-04-14
 */

#include "bcos-ledger/ledger/utilities/FIFOCache.h"
#include "unittests/ledger/FakeBlock.h"
#include <bcos-test/libutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

namespace bcos
{
namespace test
{
BOOST_FIXTURE_TEST_SUITE(FIFOCacheTest, TestPromptFixture)

BOOST_AUTO_TEST_CASE(testBlockCacheAdd)
{
    FIFOCache<Block::Ptr, Block> _blockFIFOCache;
    auto blockFactory = createBlockFactory();
    auto block1 = fakeBlock(blockFactory, 10, 10);
    auto block2 = fakeBlock(blockFactory, 10, 10);
    auto block3 = fakeBlock(blockFactory, 10, 10);
    _blockFIFOCache.add(block1->blockHeader()->number(), block1);
    auto block1_get = _blockFIFOCache.get(block1->blockHeader()->number());
    BOOST_CHECK_EQUAL(block1_get.first, block1->blockHeader()->number());
    BOOST_CHECK_EQUAL(block1_get.second->transactionsHashSize(), 10);
}
}
}  // namespace test
}  // namespace bcos