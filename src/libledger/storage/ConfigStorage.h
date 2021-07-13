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
 * @file ConfigStorage.h
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
#include "bcos-ledger/libledger/storage/StorageUtilities.h"
#include <bcos-framework/interfaces/ledger/LedgerConfig.h>
#include <bcos-framework/interfaces/ledger/LedgerTypeDef.h>
#include <bcos-framework/interfaces/storage/Common.h>

namespace bcos::ledger
{
class ConfigStorage
{
public:
    using Ptr = std::shared_ptr<ConfigStorage>;
    ConfigStorage(crypto::KeyFactory::Ptr _keyFactory) : m_keyFactory(_keyFactory){};
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

    void getSystemConfigList(const std::shared_ptr<std::vector<std::string>>& _keys,
        const storage::TableFactoryInterface::Ptr& _tableFactory, bool _allowEmpty,
        std::function<void(
            const Error::Ptr&, std::map<std::string, bcos::storage::Entry::Ptr> const&)>
            _onGetConfig);
    /**
     * @brief get consensus node list in table SYS_CONSENSUS
     * @param _nodeType
     * @param _tableFactory
     * @param _keyFactory key factory to generate nodeID
     */
    void getConsensusConfig(const std::string& _nodeType,
        const storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig);

    void getConsensusConfigList(std::vector<std::string> const& _nodeTypeList,
        const storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, std::map<std::string, consensus::ConsensusNodeListPtr>)>
            _onGetConfig);

    /**
     * @brief update SYS_CURRENT_STATE set SYS_VALUE=_stateValue where row=_row
     * @param _tableFactory
     * @param _row
     * @param _stateValue string value
     * @return return update result
     */
    bool setCurrentState(const std::string& _row, const std::string& _stateValue,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);

    /**
     * @brief update SYS_CONFIG set SYS_VALUE=_value,SYSTEM_CONFIG_ENABLE_NUM=_enableBlock where
     * row=_key
     * @param _tableFactory
     * @param _key
     * @param _value
     * @param _enableBlock
     * @return
     */
    bool setSysConfig(const std::string& _key, const std::string& _value,
        const std::string& _enableBlock,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);

    /**
     * @brief update SYS_CONSENSUS set
     * NODE_TYPE=type,NODE_WEIGHT=weight,NODE_ENABLE_NUMBER=_enableBlock where row=nodeID
     * @param _tableFactory
     * @param _type node type, only support CONSENSUS_SEALER CONSENSUS_OBSERVER
     * @param _nodeList
     * @param _enableBlock
     * @return
     */
    bool setConsensusConfig(const std::string& _type, const consensus::ConsensusNodeList& _nodeList,
        const std::string& _enableBlock,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);

    void calTotalTransactionCount(const bcos::protocol::Block::Ptr& block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);

    void setLedgerConfig(const LedgerConfig::Ptr& _ledgerConfig,
        const storage::TableFactoryInterface::Ptr& _tableFactory);

    LedgerConfig::Ptr getLedgerConfig(protocol::BlockNumber _number, const crypto::HashType& _hash,
        const storage::TableFactoryInterface::Ptr& _tableFactory);

private:
    crypto::KeyFactory::Ptr m_keyFactory;
};
}  // namespace bcos::ledger