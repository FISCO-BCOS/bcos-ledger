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
 * @file BlockStorage.cpp
 * @author: kyonRay
 * @date 2021-07-09
 */

#include "BlockStorage.h"
#include "bcos-ledger/libledger/storage/StorageUtilities.h"
#include "bcos-ledger/libledger/utilities/BlockUtilities.h"
#include "bcos-ledger/libledger/utilities/Common.h"
#include <bcos-framework/interfaces/protocol/CommonError.h>

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::consensus;

namespace bcos::ledger
{
void BlockStorage::getBlockHeader(const BlockNumber& _blockNumber,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, protocol::BlockHeader::Ptr)> _onGetHeader)
{
    auto cachedHeader = m_blockHeaderCache.get(_blockNumber);

    if (cachedHeader.second)
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getBlockHeader")
                          << LOG_DESC("CacheHeader hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        _onGetHeader(nullptr, cachedHeader.second);
        return;
    }
    LEDGER_LOG(TRACE) << LOG_BADGE("getBlockHeader") << LOG_DESC("Cache missed, read from storage")
                      << LOG_KV("blockNumber", _blockNumber);

    auto table = _tableFactory->openTable(SYS_NUMBER_2_BLOCK_HEADER);
    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open table error from db")
                          << LOG_KV("openTable", SYS_NUMBER_2_BLOCK_HEADER);
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetHeader(error, nullptr);
        return;
    }
    table->asyncGetRow(boost::lexical_cast<std::string>(_blockNumber),
        [_onGetHeader, _blockNumber, this](const Error::Ptr& _error, Entry::Ptr _headerEntry) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "asyncGetRow callback error" + _error->errorMessage());
                _onGetHeader(error, nullptr);
                return;
            }
            if (!_headerEntry)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getBlockHeader")
                                  << LOG_DESC("Get header from storage callback null entry")
                                  << LOG_KV("blockNumber", _blockNumber);
                _onGetHeader(nullptr, nullptr);
                return;
            }
            auto headerPtr =
                decodeBlockHeader(getBlockHeaderFactory(), _headerEntry->getField(SYS_VALUE));
            LEDGER_LOG(TRACE) << LOG_BADGE("getBlockHeader") << LOG_DESC("Get header from storage")
                              << LOG_KV("blockNumber", _blockNumber);
            if (headerPtr)
            {
                LEDGER_LOG(TRACE) << LOG_BADGE("getBlockHeader") << LOG_DESC("Write to cache");
                m_blockHeaderCache.add(_blockNumber, headerPtr);
            }
            _onGetHeader(nullptr, headerPtr);
        });
}

void BlockStorage::getNoncesBatchFromStorage(bcos::protocol::BlockNumber _startNumber,
    protocol::BlockNumber _endNumber, const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(
        Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
        _onGetData)
{
    auto table = _tableFactory->openTable(SYS_BLOCK_NUMBER_2_NONCES);

    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_BLOCK_NUMBER_2_NONCES table error from db");
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetData(error, nullptr);
        return;
    }
    auto numberList = std::make_shared<std::vector<std::string>>();
    for (BlockNumber i = _startNumber; i <= _endNumber; ++i)
    {
        numberList->emplace_back(boost::lexical_cast<std::string>(i));
    }

    table->asyncGetRows(numberList, [=](const Error::Ptr& _error,
                                        const std::map<std::string, Entry::Ptr>& numberEntryMap) {
        if (_error && _error->errorCode() != CommonError::SUCCESS)
        {
            // TODO: add error code and msg
            auto error = std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
            _onGetData(error, nullptr);
            return;
        }
        auto retMap = std::make_shared<std::map<protocol::BlockNumber, protocol::NonceListPtr>>();
        if (numberEntryMap.empty())
        {
            LEDGER_LOG(WARNING) << LOG_BADGE("getNoncesBatchFromStorage")
                                << LOG_DESC("getRows callback empty result")
                                << LOG_KV("startNumber", _startNumber)
                                << LOG_KV("endNumber", _endNumber);
            _onGetData(nullptr, retMap);
            return;
        }
        for (const auto& number : *numberList)
        {
            try
            {
                auto entry = numberEntryMap.at(number);
                if (!entry)
                {
                    continue;
                }
                auto block = decodeBlock(m_blockFactory, entry->getField(SYS_VALUE));
                if (!block)
                    continue;
                auto nonceList = std::make_shared<protocol::NonceList>(block->nonceList());
                retMap->emplace(
                    std::make_pair(boost::lexical_cast<BlockNumber>(number), nonceList));
            }
            catch (std::out_of_range const& e)
            {
                continue;
            }
        }
        LEDGER_LOG(DEBUG) << LOG_DESC("Get Nonce list from db")
                          << LOG_KV("retMapSize", retMap->size());
        _onGetData(nullptr, retMap);
    });
}

void BlockStorage::getBlockNumberByHash(std::string _hash,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetNum)
{
    StorageUtilities::asyncTableGetter(_tableFactory, SYS_HASH_2_NUMBER, _hash, _onGetNum);
}

void BlockStorage::getBlockHashByNumber(protocol::BlockNumber _num,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetHash)
{
    StorageUtilities::asyncTableGetter(
        _tableFactory, SYS_NUMBER_2_HASH, boost::lexical_cast<std::string>(_num), _onGetHash);
}


void BlockStorage::setNumberToNonces(
    const Block::Ptr& block, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto blockNumberStr = boost::lexical_cast<std::string>(block->blockHeader()->number());
    auto emptyBlock = m_blockFactory->createBlock();
    emptyBlock->setNonceList(block->nonceList());

    std::shared_ptr<bytes> nonceData = std::make_shared<bytes>();
    emptyBlock->encode(*nonceData);
    auto nonceStr = asString(*nonceData);

    StorageUtilities::syncTableSetter(
        _tableFactory, SYS_BLOCK_NUMBER_2_NONCES, blockNumberStr, SYS_VALUE, nonceStr);
}

void BlockStorage::setHashToNumber(const crypto::HashType& _hash,
    const protocol::BlockNumber& _number,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    StorageUtilities::syncTableSetter(_tableFactory, SYS_HASH_2_NUMBER, _hash.hex(), SYS_VALUE,
        boost::lexical_cast<std::string>(_number));
}

void BlockStorage::setNumberToHash(const protocol::BlockNumber& _number,
    const crypto::HashType& _hash, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    StorageUtilities::syncTableSetter(_tableFactory, SYS_NUMBER_2_HASH,
        boost::lexical_cast<std::string>(_number), SYS_VALUE, _hash.hex());
}

void BlockStorage::setNumberToHeader(
    const BlockHeader::Ptr& _header, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto encodedBlockHeader = std::make_shared<bytes>();
    auto emptyBlock = m_blockFactory->createBlock();
    emptyBlock->setBlockHeader(_header);
    emptyBlock->blockHeader()->encode(*encodedBlockHeader);

    StorageUtilities::syncTableSetter(_tableFactory, SYS_NUMBER_2_BLOCK_HEADER,
        boost::lexical_cast<std::string>(_header->number()), SYS_VALUE,
        asString(*encodedBlockHeader));
}
}  // namespace bcos::ledger
