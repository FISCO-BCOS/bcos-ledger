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
 * @brief storage getter interfaces call layer
 * @file StorageGetter.h
 * @author: kyonRay
 * @date 2021-04-23
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
#include <bcos-framework/interfaces/ledger/LedgerTypeDef.h>
#include <bcos-framework/interfaces/storage/Common.h>

namespace bcos::ledger
{
using stringsPair = std::pair<std::string, std::string>;
class StorageGetter final
{
public:
    using Ptr = std::shared_ptr<StorageGetter>;
    StorageGetter() = default;

    inline static StorageGetter::Ptr storageGetterFactory()
    {
        return std::make_shared<StorageGetter>();
    }

    bool checkTableExist(const std::string& _tableName,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    /**
     * @brief get transactions in SYS_NUMBER_2_TXS table
     * @param _blockNumber the number of block
     * @param _tableFactory
     */
    void getTxsFromStorage(bcos::protocol::BlockNumber _blockNumber,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetString);

    /**
     * @brief get block header in SYS_NUMBER_2_BLOCK_HEADER table
     * @param _blockNumber the number of block
     * @param _tableFactory
     */
    void getBlockHeaderFromStorage(bcos::protocol::BlockNumber _blockNumber,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetString);

    /**
     * @brief get nonce list in SYS_BLOCK_NUMBER_2_NONCES table
     * @param _blockNumber the number of block
     * @param _tableFactory
     */
    void getNoncesFromStorage(bcos::protocol::BlockNumber _blockNumber,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetString);

    void getNoncesBatchFromStorage(bcos::protocol::BlockNumber _startNumber,
        protocol::BlockNumber _endNumber,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const bcos::protocol::BlockFactory::Ptr& _blockFactory,
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

    /**
     * @brief get current state in row
     * @param _tableFactory
     * @param _row
     */
    void getCurrentState(std::string _row, const storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetString);

    /**
     * @brief get sys config in table SYS_CONFIG
     * @param _tableFactory
     * @param _key row key in table
     */
    void getSysConfig(std::string _key, const storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetConfig);

    /**
     * @brief get consensus node list in table SYS_CONSENSUS
     * @param _nodeType
     * @param _tableFactory
     * @param _keyFactory key factory to generate nodeID
     */
    void getConsensusConfig(const std::string& _nodeType,
        const storage::TableFactoryInterface::Ptr& _tableFactory,
        crypto::KeyFactory::Ptr _keyFactory,
        std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig);

    void getBatchTxByHashList(std::shared_ptr<std::vector<std::string>> _hashList,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const bcos::protocol::TransactionFactory::Ptr& _txFactory,
        std::function<void(Error::Ptr, protocol::TransactionsPtr)> _onGetTx);

    void getReceiptByTxHash(std::string _txHash,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetString);

    void getBatchReceiptsByHashList(std::shared_ptr<std::vector<std::string>> _hashList,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const bcos::protocol::TransactionReceiptFactory::Ptr& _receiptFactory,
        std::function<void(Error::Ptr, protocol::ReceiptsPtr)> _onGetReceipt);

    /**
     * @brief select field from tableName where row=_row
     * @param _tableFactory
     * @param _tableName
     * @param _row
     * @param _field
     * @param _onGetEntry callback when get entry in db
     */
    void asyncTableGetter(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _tableName, std::string _row,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry);
};
}  // namespace bcos::ledger