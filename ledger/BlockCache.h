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
 * @file BlockCache.h
 * @author: kyonRay
 * @date 2021-04-14
 */

#pragma once
#include <bcos-framework/interfaces/ledger/ledgerInterface.h>
#include <bcos-framework/libutilities/Common.h>
#include <bcos-framework/libutilities/ThreadPool.h>

#include <utility>

#define LEDGER_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos
{
namespace ledger
{
using namespace bcos;
using namespace bcos::protocol;
class BlockCache{
public:
    BlockCache() = default;
    Block::Ptr add(Block::Ptr _block);
    std::pair<Block::Ptr, h256>get(h256 const& _hash);
    void setDestructorThread(ThreadPool::Ptr _destructorThread)
    {
        m_destructorThread = std::move(_destructorThread);
    }

private:
    mutable boost::shared_mutex m_sharedMutex;
    mutable std::map<h256, Block::Ptr> m_blockCacheMap;
    mutable std::deque<h256> m_blockCacheFIFO;
    const unsigned c_blockCacheMaxSize = 10;
    ThreadPool::Ptr m_destructorThread;
};
} // namespace ledger
} // namespace bcos




