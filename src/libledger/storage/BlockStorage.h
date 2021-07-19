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
 * @file BlockStorage.h
 * @author: kyonRay
 * @date 2021-07-09
 */

#pragma once
#include "bcos-framework/interfaces/consensus/ConsensusNode.h"
#include "bcos-framework/interfaces/protocol/Block.h"
#include "bcos-framework/interfaces/protocol/BlockFactory.h"
#include "bcos-framework/interfaces/protocol/BlockHeader.h"
#include "bcos-framework/interfaces/protocol/BlockHeaderFactory.h"
#include "bcos-framework/interfaces/protocol/Transaction.h"
#include "bcos-framework/interfaces/protocol/TransactionReceipt.h"
#include "bcos-framework/interfaces/storage/TableInterface.h"
#include "bcos-ledger/libledger/utilities/FIFOCache.h"
#include <bcos-framework/interfaces/ledger/LedgerTypeDef.h>
#include <bcos-framework/interfaces/storage/Common.h>

namespace bcos::ledger
{
class BlockStorage
{
public:
    using Ptr = std::shared_ptr<BlockStorage>;
    explicit BlockStorage(bcos::protocol::BlockFactory::Ptr _blockFactory)
      : m_blockFactory(_blockFactory)
    {
        assert(m_blockFactory);

        auto headerDestructorThread = std::make_shared<ThreadPool>("headerCache", 1);
        m_blockHeaderCache.setDestructorThread(headerDestructorThread);
    }

    inline protocol::BlockHeaderFactory::Ptr getBlockHeaderFactory()
    {
        return m_blockFactory->blockHeaderFactory();
    }

    void stop() { m_blockHeaderCache.stop(); }
    inline void headerCacheAdd(
        const protocol::BlockNumber& _num, const protocol::BlockHeader::Ptr& _header)
    {
        m_blockHeaderCache.add(_num, _header);
    }

    void getBlockHeader(const protocol::BlockNumber& _blockNumber,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, protocol::BlockHeader::Ptr)> _onGetHeader);

    void getNoncesBatchFromStorage(protocol::BlockNumber _startNumber,
        protocol::BlockNumber _endNumber, const storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(
            Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
            _onGetData);

    /**
     * @brief get block number by blockHash
     * @param _tableFactory
     * @param _tableName the table name of the table to get data
     * @param _hash hash string, it can be blockHash
     * @return return string data of block number
     */
    void getBlockNumberByHash(std::string _hash,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetString);

    /**
     * @brief get block hash by number
     * @param _num
     * @param _tableFactory
     */
    void getBlockHashByNumber(protocol::BlockNumber _num,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetString);

public:
    void setNumberToNonces(const bcos::protocol::Block::Ptr& block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void setHashToNumber(const crypto::HashType& _hash, const protocol::BlockNumber& _number,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void setNumberToHash(const protocol::BlockNumber& _number, const crypto::HashType& _hash,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void setNumberToHeader(const bcos::protocol::BlockHeader::Ptr& _header,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);

private:
    FIFOCache<protocol::BlockHeader::Ptr, protocol::BlockHeader> m_blockHeaderCache;
    bcos::protocol::BlockFactory::Ptr m_blockFactory;
};
}  // namespace bcos::ledger
