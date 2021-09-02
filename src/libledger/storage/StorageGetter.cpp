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
 * @file StorageGetter.cpp
 * @author: kyonRay
 * @date 2021-04-23
 */

#include "StorageGetter.h"
#include "bcos-ledger/libledger/utilities/BlockUtilities.h"
#include "bcos-ledger/libledger/utilities/Common.h"
#include <bcos-framework/interfaces/protocol/CommonError.h>

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::consensus;

namespace bcos::ledger
{
bool StorageGetter::checkTableExist(
    const std::string& _tableName, const bcos::storage::TableStorage::Ptr& _tableFactory)
{
    auto table = _tableFactory->openTable(_tableName);
    return table != nullptr;
}

void StorageGetter::getTxsFromStorage(BlockNumber _blockNumber,
    const TableStorage::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_NUMBER_2_TXS,
        boost::lexical_cast<std::string>(_blockNumber), _onGetEntry);
}

void StorageGetter::getBlockHeaderFromStorage(bcos::protocol::BlockNumber _blockNumber,
    const bcos::storage::TableStorage::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_NUMBER_2_BLOCK_HEADER,
        boost::lexical_cast<std::string>(_blockNumber), _onGetEntry);
}

void StorageGetter::getNoncesFromStorage(BlockNumber _blockNumber,
    const TableStorage::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_BLOCK_NUMBER_2_NONCES,
        boost::lexical_cast<std::string>(_blockNumber), _onGetEntry);
}

void StorageGetter::getNoncesBatchFromStorage(bcos::protocol::BlockNumber _startNumber,
    protocol::BlockNumber _endNumber, const bcos::storage::TableStorage::Ptr& _tableFactory,
    const bcos::protocol::BlockFactory::Ptr& _blockFactory,
    std::function<void(
        Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
        _onGetData)
{
    auto table = _tableFactory->openTable(SYS_BLOCK_NUMBER_2_NONCES);

    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open table error from db")
                          << LOG_KV("tableName", SYS_BLOCK_NUMBER_2_NONCES);
        auto error = std::make_shared<Error>(
            LedgerError::OpenTableFailed, "Open" + SYS_BLOCK_NUMBER_2_NONCES + " table error");
        _onGetData(error, nullptr);
        return;
    }
    auto numberList = std::make_shared<std::vector<std::string>>();
    for (BlockNumber i = _startNumber; i <= _endNumber; ++i)
    {
        numberList->emplace_back(boost::lexical_cast<std::string>(i));
    }

    table->asyncGetRows(*numberList, [numberList, _onGetData, _startNumber, _endNumber,
                                         _blockFactory](Error::Ptr&& _error,
                                         std::vector<Entry::Ptr>&& numberEntries) {
        if (_error && _error->errorCode() != CommonError::SUCCESS)
        {
            _onGetData(_error, nullptr);
            return;
        }
        auto retMap = std::make_shared<std::map<protocol::BlockNumber, protocol::NonceListPtr>>();
        if (numberEntries.empty())
        {
            LEDGER_LOG(WARNING) << LOG_BADGE("getNoncesBatchFromStorage")
                                << LOG_DESC("getRows callback empty result")
                                << LOG_KV("startNumber", _startNumber)
                                << LOG_KV("endNumber", _endNumber);
            _onGetData(nullptr, retMap);
            return;
        }

        for (size_t i = 0; i < numberList->size(); ++i)
        {
            {
                try
                {
                    auto& entry = numberEntries[i];
                    if (!entry)
                    {
                        continue;
                    }
                    auto block =
                        decodeBlock(_blockFactory, std::string(entry->getField(SYS_VALUE)));
                    if (!block)
                        continue;
                    auto nonceList = std::make_shared<protocol::NonceList>(block->nonceList());
                    // retMap->emplace(
                    //     std::make_pair(boost::lexical_cast<BlockNumber>(number), nonceList));
                }
                catch (std::exception const& e)
                {
                    continue;
                }
            }
            LEDGER_LOG(DEBUG) << LOG_BADGE("getNoncesBatchFromStorage")
                              << LOG_DESC("Get Nonce list from db")
                              << LOG_KV("retMapSize", retMap->size());
            _onGetData(nullptr, retMap);
        }
    });
}

void StorageGetter::getBlockNumberByHash(std::string _hash,
    const bcos::storage::TableStorage::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_HASH_2_NUMBER, _hash, _onGetEntry);
}

void StorageGetter::getBlockHashByNumber(BlockNumber _num,
    const bcos::storage::TableStorage::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(
        _tableFactory, SYS_NUMBER_2_HASH, boost::lexical_cast<std::string>(_num), _onGetEntry);
}

void StorageGetter::getCurrentState(std::string _row,
    const storage::TableStorage::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    return asyncTableGetter(_tableFactory, SYS_CURRENT_STATE, _row, _onGetEntry);
}

void StorageGetter::getSysConfig(std::string _key, const TableStorage::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetConfig)
{
    asyncTableGetter(_tableFactory, SYS_CONFIG, _key, _onGetConfig);
}

void StorageGetter::asyncGetSystemConfigList(const std::vector<std::string>& _keys,
    const storage::TableStorage::Ptr& _tableFactory, bool _allowEmpty,
    std::function<void(Error::Ptr&&, std::vector<Entry::Ptr>&&)> _onGetConfig)
{
    // open table
    auto table = _tableFactory->openTable(SYS_CONFIG);
    if (!table)
    {
        _onGetConfig(
            BCOS_ERROR_PTR(LedgerError::OpenTableFailed, "open table " + SYS_CONFIG + " failed."),
            std::vector<Entry::Ptr>());
        return;
    }
    table->asyncGetRows(gsl::span(const_cast<std::string*>(_keys.data()), _keys.size()),
        [_allowEmpty, _onGetConfig](Error::Ptr&& _error, std::vector<Entry::Ptr>&& _entries) {
            if (_error)
            {
                LEDGER_LOG(ERROR) << LOG_DESC("asyncGetSystemConfigList failed")
                                  << LOG_KV("code", _error->errorCode())
                                  << LOG_KV("msg", _error->errorMessage());
                _onGetConfig(BCOS_ERROR_WITH_PREV_PTR(-1, "", _error), std::vector<Entry::Ptr>());
                return;
            }
            if (_allowEmpty)
            {
                _onGetConfig(nullptr, std::move(_entries));
                return;
            }
            // check the result
            for (auto const& entry : _entries)
            {
                // Note: must make sure all the configs are not empty
                if (!entry)
                {
                    auto errorMsg = "asyncGetSystemConfigList failed for get empty config for key";
                    LEDGER_LOG(ERROR) << LOG_DESC(errorMsg);
                    _onGetConfig(std::make_shared<Error>(LedgerError::CallbackError, errorMsg),
                        std::vector<Entry::Ptr>());
                    return;
                }
            }
            _onGetConfig(nullptr, std::move(_entries));
        });
}

void StorageGetter::asyncGetConsensusConfig(std::string const& _nodeType, BlockNumber _number,
    const storage::TableStorage::Ptr& _tableFactory, crypto::KeyFactory::Ptr _keyFactory,
    std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig)
{
    std::vector<std::string> nodeTypeList;
    nodeTypeList.emplace_back(_nodeType);
    asyncGetConsensusConfigList(nodeTypeList, _number, _tableFactory, _keyFactory,
        [_nodeType, _onGetConfig](
            Error::Ptr _error, std::map<std::string, consensus::ConsensusNodeListPtr> _nodeMap) {
            if (_error)
            {
                _onGetConfig(_error, nullptr);
                return;
            }
            if (_nodeMap.count(_nodeType))
            {
                _onGetConfig(_error, _nodeMap[_nodeType]);
                return;
            }
            _onGetConfig(_error, nullptr);
        });
}

void StorageGetter::asyncGetConsensusConfigList(std::vector<std::string> const& _nodeTypeList,
    protocol::BlockNumber _number, const TableStorage::Ptr& _tableFactory,
    crypto::KeyFactory::Ptr _keyFactory,
    std::function<void(Error::Ptr, std::map<std::string, consensus::ConsensusNodeListPtr>)>
        _onGetConfig)
{
    auto table = _tableFactory->openTable(SYS_CONSENSUS);
    std::map<std::string, consensus::ConsensusNodeListPtr> emptyMap;
    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_BADGE("asyncGetConsensusConfigList")
                          << LOG_DESC("Open table error from db")
                          << LOG_KV("tableName", SYS_CONSENSUS);
        auto error =
            std::make_shared<Error>(LedgerError::OpenTableFailed, "open SYS_CONSENSUS table error");
        _onGetConfig(error, emptyMap);
        return;
    }
    table->asyncGetPrimaryKeys(
        nullptr, [_onGetConfig, table, _nodeTypeList, _keyFactory, _number, emptyMap](
                     const Error::Ptr& _error, std::vector<std::string> _keys) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                _onGetConfig(_error, emptyMap);
                return;
            }
            auto keys = std::make_shared<std::vector<std::string>>(_keys);
            table->asyncGetRows(*keys, [keys, _nodeTypeList, _keyFactory, _onGetConfig, _number,
                                           emptyMap](Error::Ptr&& _error,
                                           std::vector<Entry::Ptr>&& _entries) {
                if (_error && _error->errorCode() != CommonError::SUCCESS)
                {
                    _onGetConfig(_error, emptyMap);
                    return;
                }
                std::map<std::string, consensus::ConsensusNodeListPtr> nodeMap;

                for (auto const& type : _nodeTypeList)
                {
                    auto node = std::make_shared<ConsensusNodeList>();
                    for (size_t i = 0; i < _entries.size(); ++i)
                    {
                        auto& nodePair = _entries[i];

                        if (!nodePair)
                        {
                            continue;
                        }
                        try
                        {
                            auto nodeType = nodePair->getField(NODE_TYPE);
                            auto enableNum = boost::lexical_cast<BlockNumber>(
                                nodePair->getField(NODE_ENABLE_NUMBER));
                            auto weight =
                                boost::lexical_cast<uint64_t>(nodePair->getField(NODE_WEIGHT));
                            if ((nodeType == type) && enableNum <= _number)
                            {
                                crypto::NodeIDPtr nodeID =
                                    _keyFactory->createKey(*fromHexString((*keys)[i]));
                                // Note: use try-catch to handle the exception case
                                node->emplace_back(std::make_shared<ConsensusNode>(nodeID, weight));
                            }
                        }
                        catch (...)
                        {
                            continue;
                        }
                    }
                    nodeMap[type] = node;
                }
                _onGetConfig(nullptr, nodeMap);
            });
        });
}

void StorageGetter::asyncTableGetter(const bcos::storage::TableStorage::Ptr& _tableFactory,
    const std::string& _tableName, std::string _row,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    auto table = _tableFactory->openTable(_tableName);
    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_BADGE("asyncTableGetter") << LOG_DESC("Open table error from db")
                          << LOG_KV("openTable", _tableName);
        auto error = std::make_shared<Error>(LedgerError::OpenTableFailed, "");
        _onGetEntry(error, nullptr);
        return;
    }

    LEDGER_LOG(TRACE) << LOG_BADGE("asyncTableGetter") << LOG_DESC("Get string from db")
                      << LOG_KV("openTable", _tableName) << LOG_KV("row", _row);
    table->asyncGetRow(_row, [_onGetEntry](const Error::Ptr& _error, Entry::Ptr _entry) {
        if (_error && _error->errorCode() != CommonError::SUCCESS)
        {
            _onGetEntry(_error, nullptr);
            return;
        }
        // do not handle if entry is nullptr, just send it out
        _onGetEntry(nullptr, _entry);
    });
}

void StorageGetter::getBatchTxByHashList(std::shared_ptr<std::vector<std::string>> _hashList,
    const bcos::storage::TableStorage::Ptr& _tableFactory,
    const bcos::protocol::TransactionFactory::Ptr& _txFactory,
    std::function<void(Error::Ptr, protocol::TransactionsPtr)> _onGetTx)
{
    auto table = _tableFactory->openTable(SYS_HASH_2_TX);

    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open table error from db")
                          << LOG_KV("tableName", SYS_HASH_2_TX);
        auto error =
            std::make_shared<Error>(LedgerError::OpenTableFailed, "open table SYS_HASH_2_TX error");
        _onGetTx(error, nullptr);
        return;
    }
    table->asyncGetRows(*_hashList, [_hashList, _txFactory, _onGetTx](Error::Ptr&& _error,
                                        std::vector<Entry::Ptr>&& _hashEntries) {
        if (_error && _error->errorCode() != CommonError::SUCCESS)
        {
            _onGetTx(_error, nullptr);
            return;
        }
        auto txList = std::make_shared<Transactions>();
        if (_hashEntries.empty())
        {
            _onGetTx(nullptr, txList);
            return;
        }
        // get tx list in hashList sequence
        for (size_t i = 0; i < _hashList->size(); ++i)
        {
            try
            {
                auto entry = _hashEntries[i];
                if (!entry)
                {
                    continue;
                }
                auto tx = decodeTransaction(_txFactory, std::string(entry->getField(SYS_VALUE)));
                if (tx)
                    txList->emplace_back(tx);
            }
            catch (std::out_of_range const& e)
            {
                continue;
            }
        }
        _onGetTx(nullptr, txList);
    });
}

void StorageGetter::getReceiptByTxHash(std::string _txHash,
    const bcos::storage::TableStorage::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_HASH_2_RECEIPT, _txHash, _onGetEntry);
}

void StorageGetter::getBatchReceiptsByHashList(std::shared_ptr<std::vector<std::string>> _hashList,
    const bcos::storage::TableStorage::Ptr& _tableFactory,
    const bcos::protocol::TransactionReceiptFactory::Ptr& _receiptFactory,
    std::function<void(Error::Ptr, protocol::ReceiptsPtr)> _onGetReceipt)
{
    auto table = _tableFactory->openTable(SYS_HASH_2_RECEIPT);

    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_HASH_2_RECEIPT table error from db")
                          << LOG_KV("tableName", SYS_HASH_2_RECEIPT);
        auto error = std::make_shared<Error>(
            LedgerError::OpenTableFailed, "open table SYS_HASH_2_RECEIPT error");
        _onGetReceipt(error, nullptr);
        return;
    }
    table->asyncGetRows(*_hashList, [_onGetReceipt, _receiptFactory, _hashList](Error::Ptr&& _error,
                                        std::vector<Entry::Ptr>&& _hashEntries) {
        if (_error && _error->errorCode() != CommonError::SUCCESS)
        {
            _onGetReceipt(_error, nullptr);
            return;
        }
        auto receiptList = std::make_shared<Receipts>();
        if (_hashEntries.empty())
        {
            _onGetReceipt(nullptr, receiptList);
            return;
        }

        for (auto& entry : _hashEntries)
        {
            try
            {
                if (!entry)
                {
                    continue;
                }
                auto receipt =
                    decodeReceipt(_receiptFactory, std::string(entry->getField(SYS_VALUE)));
                if (receipt)
                    receiptList->emplace_back(receipt);
            }
            catch (std::out_of_range const& e)
            {
                continue;
            }
        }
        _onGetReceipt(nullptr, receiptList);
    });
}

}  // namespace bcos::ledger