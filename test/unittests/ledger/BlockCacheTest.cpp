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

#include "unittests/common/Common.h"
#include "unittests/ledger/FakeBlock.h"
#include <bcos-ledger/ledger/BlockCache.h>
#include <bcos-test/libutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

namespace bcos
{
namespace test
{

BOOST_FIXTURE_TEST_SUITE(BlockCacheTest, TestPromptFixture)

BOOST_AUTO_TEST_CASE(testBlockCacheAdd)
{
    BlockCache _blockCache;
    auto blockFactory = createBlockFactory();
    auto block1 = fakeBlock(blockFactory, 10,10);
    auto block2 = fakeBlock(blockFactory, 10,10);
    auto block3 = fakeBlock(blockFactory, 10,10);
    _blockCache.add(block1);
    auto block1_get = _blockCache.get(block1->blockHeader()->hash());
    BOOST_CHECK_EQUAL(block1_get.second,block1->blockHeader()->hash());
    BOOST_CHECK_EQUAL(block1_get.first->transactionsHashSize(),10);
}
}
} // namespace test
} // namespace bcos