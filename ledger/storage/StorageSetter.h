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
 * @file StorageSetter.h
 * @author: kyonRay
 * @date 2021-04-23
 */

#pragma once
#include "bcos-framework/interfaces/protocol/Block.h"
#include "bcos-framework/interfaces/protocol/BlockFactory.h"
#include "bcos-framework/interfaces/protocol/Transaction.h"
#include "bcos-framework/interfaces/protocol/TransactionReceipt.h"
#include "bcos-framework/interfaces/protocol/BlockHeader.h"
#include "bcos-framework/interfaces/protocol/BlockHeaderFactory.h"
#include "bcos-framework/interfaces/storage/TableInterface.h"
#include "bcos-framework/interfaces/consensus/ConsensusNode.h"

namespace bcos::ledger
{
DERIVE_BCOS_EXCEPTION(OpenSysTableFailed);
using stringsPair = std::pair<std::string, std::string>;
class StorageSetter final {
public:
    using Ptr = std::shared_ptr<StorageSetter>;
    inline static StorageSetter::Ptr storageSetterFactory(){
        return std::make_shared<StorageSetter>();
    }

    void createTables(const storage::TableFactoryInterface::Ptr& _tableFactory);

    /**
     * @brief update tableName set fieldName=fieldValue where row=_row
     * @param _tableFactory
     * @param _tableName
     * @param _row
     * @param _fieldName
     * @param _fieldValue
     * @return return update result
     */
    bool tableSetterByRowAndField(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _tableName, const std::string& _row, const std::string& _fieldName,
        const std::string& _fieldValue);

    /**
     * @brief update SYS_NUMBER_2_BLOCK set SYS_VALUE=_blockValue where row=_row
     * @param _tableFactory
     * @param _row
     * @param _blockValue encoded block string value
     * @return return update result
     */
    bool setNumber2Block(const bcos::storage::TableFactoryInterface::Ptr & _tableFactory,
        const std::string& _row, const std::string& _blockValue);

    /**
     * @brief update SYS_CURRENT_STATE set SYS_VALUE=_stateValue where row=_row
     * @param _tableFactory
     * @param _row
     * @param _stateValue string value
     * @return return update result
     */
    bool setCurrentState(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _row, const std::string& _stateValue);

    /**
    * @brief update SYS_NUMBER_2_BLOCK_HEADER set SYS_VALUE=_headerValue where row=_row
    * @param _tableFactory
    * @param _row
    * @param _headerValue encoded block header string value
    * @return return update result
    */
    bool setNumber2Header(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _row, const std::string& _headerValue);

    /**
    * @brief update SYS_NUMBER_2_TXS set SYS_VALUE=_txsValue where row=_row
    * @param _tableFactory
    * @param _row
    * @param _txsValue encoded block string value, which txs contain in
    * @return return update result
    */
    bool setNumber2Txs(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _row, const std::string& _txsValue);

    /**
    * @brief update SYS_NUMBER_2_RECEIPTS set SYS_VALUE=_receiptsValue where row=_row
    * @param _tableFactory
    * @param _row
    * @param _receiptsValue encoded block string value, which receipts contain in
    * @return return update result
    */
    bool setNumber2Receipts(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _row, const std::string& _receiptsValue);

    /**
    * @brief update SYS_HASH_2_NUMBER set SYS_VALUE=_numberValue where row=_row
    * @param _tableFactory
    * @param _row
    * @param _numberValue block number string value
    * @return return update result
    */
    bool setHash2Number(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _row, const std::string& _numberValue);

    bool setNumber2Hash(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _row, const std::string& _hashValue);

    /**
    * @brief update SYS_BLOCK_NUMBER_2_NONCES set SYS_VALUE=_noncesValue where row=_row
    * @param _tableFactory
    * @param _row
    * @param _noncesValue encoded nonces string value
    * @return return update result
    */
    bool setNumber2Nonces(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _row, const std::string& _noncesValue);

    /**
     * @brief update SYS_CONFIG set SYS_VALUE=_value,SYSTEM_CONFIG_ENABLE_NUM=_enableBlock where row=_key
     * @param _tableFactory
     * @param _key
     * @param _value
     * @param _enableBlock
     * @return
     */
    bool setSysConfig(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _key, const std::string& _value, const std::string& _enableBlock);

    /**
     * @brief update SYS_CONSENSUS set NODE_TYPE=type,NODE_WEIGHT=weight,NODE_ENABLE_NUMBER=_enableBlock where row=nodeID
     * @param _tableFactory
     * @param _type node type, only support CONSENSUS_SEALER CONSENSUS_OBSERVER
     * @param _nodeList
     * @param _enableBlock
     * @return
     */
    bool setConsensusConfig(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _type, const consensus::ConsensusNodeList& _nodeList,
        const std::string& _enableBlock);

    /**
     * @brief tbb:parallel_for update SYS_TX_HASH_2_BLOCK_NUMBER set SYS_VALUE=blockNumber, index=txIndex where row=_row
     * @param _block
     * @param _tableFactory
     */
    void writeTxToBlock(const protocol::Block::Ptr& _block,
        const storage::TableFactoryInterface::Ptr& _tableFactory);

    bool setHashToTx(const bcos::storage::TableFactoryInterface::Ptr _tableFactory,
        const std::string& _txHash, const std::string& _encodeTx);

};
}