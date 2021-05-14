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
 * @file FIFOCache.h
 * @author: kyonRay
 * @date 2021-04-23
 */

#pragma once
#include "bcos-framework/libutilities/ThreadPool.h"
#include "./Common.h"

namespace bcos::ledger
{
template <typename T, typename T2>
class FIFOCache
{
public:
    FIFOCache() = default;
    T add(const bcos::protocol::BlockNumber& _number, const T& _item)
    {
        {
            WriteGuard guard(m_sharedMutex);
            if (m_CacheMap.size() + 1 > c_CacheMaxSize)
            {
                LEDGER_LOG(TRACE) << LOG_DESC("[add]Cache full, start to remove old item...");
                auto firstNumber = m_CacheQueue.front();
                m_CacheQueue.pop_front();
                auto removedItem = m_CacheMap[firstNumber];

                m_CacheMap.erase(firstNumber);
                // Destruct the block in m_destructorThread
                HolderForDestructor<T2> holder(std::move(removedItem));
                m_destructorThread->enqueue(std::move(holder));
            }
            auto blockNumber = _number;
            auto item = _item;
            m_CacheMap.insert(std::make_pair(blockNumber, item));
            // add hash index to the blockCache queue, use to remove first element when the cache is
            // full
            m_CacheQueue.push_back(blockNumber);

            return item;
        }
    }

    std::pair<bcos::protocol::BlockNumber, T> get(const bcos::protocol::BlockNumber& _number)
    {
        {
            ReadGuard guard(m_sharedMutex);

            auto it = m_CacheMap.find(_number);
            if (it == m_CacheMap.end())
            {
                return std::make_pair(-1, nullptr);
            }

            return std::make_pair(_number, it->second);
        }
    }
    void setDestructorThread(ThreadPool::Ptr _destructorThread)
    {
        m_destructorThread = std::move(_destructorThread);
    }

private:
    FIFOCache& operator=(const FIFOCache&) = default;
    mutable boost::shared_mutex m_sharedMutex;
    mutable std::map<bcos::protocol::BlockNumber, T> m_CacheMap;
    mutable std::deque<bcos::protocol::BlockNumber> m_CacheQueue;
    const unsigned c_CacheMaxSize = 10;
    ThreadPool::Ptr m_destructorThread;
};
}  // namespace bcos::ledger
