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
    const std::string& _tableName, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto table = _tableFactory->openTable(_tableName);
    return table != nullptr;
}

void StorageGetter::getTxsFromStorage(BlockNumber _blockNumber,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_NUMBER_2_TXS,
        boost::lexical_cast<std::string>(_blockNumber), _onGetEntry);
}

void StorageGetter::getBlockHeaderFromStorage(bcos::protocol::BlockNumber _blockNumber,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_NUMBER_2_BLOCK_HEADER,
        boost::lexical_cast<std::string>(_blockNumber), _onGetEntry);
}

void StorageGetter::getNoncesFromStorage(BlockNumber _blockNumber,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_BLOCK_NUMBER_2_NONCES,
        boost::lexical_cast<std::string>(_blockNumber), _onGetEntry);
}

void StorageGetter::getNoncesBatchFromStorage(bcos::protocol::BlockNumber _startNumber,
    protocol::BlockNumber _endNumber,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
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

    table->asyncGetRows(numberList, [=](const Error::Ptr& _error,
                                        const std::map<std::string, Entry::Ptr>& numberEntryMap) {
        if (_error && _error->errorCode() != CommonError::SUCCESS)
        {
            _onGetData(_error, nullptr);
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
                auto block = decodeBlock(_blockFactory, entry->getField(SYS_VALUE));
                if (!block)
                    continue;
                auto nonceList = std::make_shared<protocol::NonceList>(block->nonceList());
                retMap->emplace(
                    std::make_pair(boost::lexical_cast<BlockNumber>(number), nonceList));
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
    });
}

void StorageGetter::getBlockNumberByHash(std::string _hash,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_HASH_2_NUMBER, _hash, _onGetEntry);
}

void StorageGetter::getBlockHashByNumber(BlockNumber _num,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(
        _tableFactory, SYS_NUMBER_2_HASH, boost::lexical_cast<std::string>(_num), _onGetEntry);
}

void StorageGetter::getCurrentState(std::string _row,
    const storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    return asyncTableGetter(_tableFactory, SYS_CURRENT_STATE, _row, _onGetEntry);
}

void StorageGetter::getSysConfig(std::string _key, const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetConfig)
{
    asyncTableGetter(_tableFactory, SYS_CONFIG, _key, _onGetConfig);
}

void StorageGetter::asyncGetSystemConfigList(const std::shared_ptr<std::vector<std::string>>& _keys,
    const storage::TableFactoryInterface::Ptr& _tableFactory, bool _allowEmpty,
    std::function<void(const Error::Ptr&, std::map<std::string, Entry::Ptr> const&)> _onGetConfig)
{
    // open table
    auto table = _tableFactory->openTable(SYS_CONFIG);
    if (!table)
    {
        auto error = std::make_shared<Error>(
            LedgerError::OpenTableFailed, "open table " + SYS_CONFIG + " failed.");
        std::map<std::string, bcos::storage::Entry::Ptr> emptyEntries;
        _onGetConfig(error, emptyEntries);
        return;
    }
    table->asyncGetRows(_keys, [_keys, _allowEmpty, _onGetConfig](const Error::Ptr& _error,
                                   std::map<std::string, Entry::Ptr> const& _entries) {
        std::map<std::string, Entry::Ptr> emptyEntryMap;
        if (_error)
        {
            LEDGER_LOG(ERROR) << LOG_DESC("asyncGetSystemConfigList failed")
                              << LOG_KV("code", _error->errorCode())
                              << LOG_KV("msg", _error->errorMessage());
            _onGetConfig(_error, emptyEntryMap);
            return;
        }
        if (_allowEmpty)
        {
            _onGetConfig(_error, _entries);
            return;
        }
        // check the result
        for (auto const& key : *_keys)
        {
            // Note: must make sure all the configs are not empty
            if (!_entries.count(key))
            {
                auto entry = _entries.at(key);
                if (entry)
                {
                    continue;
                }
                auto errorMsg =
                    "asyncGetSystemConfigList failed for get empty config for key: " + key;
                LEDGER_LOG(ERROR) << LOG_DESC(errorMsg) << LOG_KV("key", key);
                _onGetConfig(
                    std::make_shared<Error>(LedgerError::CallbackError, errorMsg), emptyEntryMap);
                return;
            }
        }
        _onGetConfig(_error, _entries);
    });
}

void StorageGetter::asyncGetConsensusConfig(std::string const& _nodeType, BlockNumber _number,
    const storage::TableFactoryInterface::Ptr& _tableFactory, crypto::KeyFactory::Ptr _keyFactory,
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
    protocol::BlockNumber _number, const TableFactoryInterface::Ptr& _tableFactory,
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
            table->asyncGetRows(keys, [_nodeTypeList, _keyFactory, _onGetConfig, _number, emptyMap](
                                          const Error::Ptr& _error,
                                          const std::map<std::string, Entry::Ptr>& _entryMap) {
                if (_error && _error->errorCode() != CommonError::SUCCESS)
                {
                    _onGetConfig(_error, emptyMap);
                    return;
                }
                std::map<std::string, consensus::ConsensusNodeListPtr> nodeMap;
                for (auto const& type : _nodeTypeList)
                {
                    auto node = std::make_shared<ConsensusNodeList>();
                    for (const auto& nodePair : _entryMap)
                    {
                        if (!nodePair.second)
                        {
                            continue;
                        }
                        try
                        {
                            auto nodeType = nodePair.second->getField(NODE_TYPE);
                            auto enableNum = boost::lexical_cast<BlockNumber>(
                                nodePair.second->getField(NODE_ENABLE_NUMBER));
                            auto weight = boost::lexical_cast<uint64_t>(
                                nodePair.second->getField(NODE_WEIGHT));
                            if ((nodeType == type) && enableNum <= _number)
                            {
                                crypto::NodeIDPtr nodeID =
                                    _keyFactory->createKey(*fromHexString(nodePair.first));
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

void StorageGetter::asyncTableGetter(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
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
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
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
    table->asyncGetRows(_hashList, [_txFactory, _onGetTx, _hashList](const Error::Ptr& _error,
                                       const std::map<std::string, Entry::Ptr>& _hashEntryMap) {
        if (_error && _error->errorCode() != CommonError::SUCCESS)
        {
            _onGetTx(_error, nullptr);
            return;
        }
        auto txList = std::make_shared<Transactions>();
        if (_hashEntryMap.empty())
        {
            _onGetTx(nullptr, txList);
            return;
        }
        // get tx list in hashList sequence
        for (const auto& hash : *_hashList)
        {
            try
            {
                auto entry = _hashEntryMap.at(hash);
                if (!entry)
                {
                    continue;
                }
                auto tx = decodeTransaction(_txFactory, entry->getField(SYS_VALUE));
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
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    asyncTableGetter(_tableFactory, SYS_HASH_2_RECEIPT, _txHash, _onGetEntry);
}

void StorageGetter::getBatchReceiptsByHashList(std::shared_ptr<std::vector<std::string>> _hashList,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
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
    table->asyncGetRows(
        _hashList, [_onGetReceipt, _receiptFactory, _hashList](const Error::Ptr& _error,
                       const std::map<std::string, Entry::Ptr>& _hashEntryMap) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                _onGetReceipt(_error, nullptr);
                return;
            }
            auto receiptList = std::make_shared<Receipts>();
            if (_hashEntryMap.empty())
            {
                _onGetReceipt(nullptr, receiptList);
                return;
            }
            for (const auto& hash : *_hashList)
            {
                try
                {
                    auto entry = _hashEntryMap.at(hash);
                    if (!entry)
                    {
                        continue;
                    }
                    auto receipt = decodeReceipt(_receiptFactory, entry->getField(SYS_VALUE));
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