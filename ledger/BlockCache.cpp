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
 * @file BlockCache.cpp
 * @author: kyonRay
 * @date 2021-04-14
 */

#include "BlockCache.h"

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::ledger;
using namespace std;

Block::Ptr BlockCache::add(Block::Ptr _block)
{
    {
        WriteGuard guard(m_sharedMutex);
        if(m_blockCacheMap.size() > c_blockCacheMaxSize)
        {
            LEDGER_LOG(TRACE) << LOG_DESC("[add]Block cache full, start to remove old item...");
            auto firstHash = m_blockCacheFIFO.front();
            m_blockCacheFIFO.pop_front();
            auto removedBlock = m_blockCacheMap[firstHash];

            m_blockCacheMap.erase(firstHash);
            // Destruct the block in m_destructorThread
            HolderForDestructor<Block> holder(std::move(removedBlock));
            m_destructorThread->enqueue(std::move(holder));

            // in case something unexcept error
            if (m_blockCacheMap.size() > c_blockCacheMaxSize)
            {
                // meet error, cache and cacheFIFO not sync, clear the cache
                m_blockCacheMap.clear();
                m_blockCacheFIFO.clear();
            }
        }
        auto blockHash = _block->blockHeader()->hash();
        auto block = _block;
        m_blockCacheMap.insert(std::make_pair(blockHash, block));
        // add hash index to the blockCache queue, use to remove first element when the cache is full
        m_blockCacheFIFO.push_back(blockHash);

        return block;

    }
}
std::pair<Block::Ptr, h256> BlockCache::get(const h256& _hash)
{
    {
        ReadGuard guard(m_sharedMutex);

        auto it = m_blockCacheMap.find(_hash);
        if (it == m_blockCacheMap.end())
        {
            return std::make_pair(nullptr, h256(0));
        }

        return std::make_pair(it->second, _hash);
    }
}
