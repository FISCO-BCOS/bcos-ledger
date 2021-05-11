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
 * @file LedgerTest.cpp
 * @author: kyonRay
 * @date 2021-05-07
 */
#include "common/FakeTable.h"
#include "common/FakeBlock.h"
#include "../ledger/Ledger.h"
#include "../ledger/storage/StorageGetter.h"
#include "../ledger/storage/StorageSetter.h"
#include <bcos-test/libutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::crypto;

namespace bcos::test
{

class LedgerFixture : public TestPromptFixture{
public:
    LedgerFixture() :TestPromptFixture(){
        auto hashImpl = std::make_shared<Keccak256Hash>();
        m_storage = std::make_shared<MockStorage>();
        BOOST_TEST(m_storage != nullptr);
        m_storageGetter =  StorageGetter::storageGetterFactory();
        m_storageSetter = StorageSetter::storageSetterFactory();
        m_blockFactory = createBlockFactory(createCryptoSuite());
        m_headerFactory = std::make_shared<PBBlockHeaderFactory>(createCryptoSuite());
        BOOST_CHECK(m_blockFactory != nullptr);
        BOOST_CHECK(m_headerFactory != nullptr);
        m_ledger = std::make_shared<Ledger>(m_blockFactory, m_headerFactory, m_storage);
        BOOST_CHECK(m_ledger != nullptr);

        auto param = std::make_shared<LedgerConfig>();
        param->setBlockNumber(0);
        param->setHash(HashType(""));
        param->setBlockTxCountLimit(1000);
        param->setConsensusTimeout(1000);

        auto signImpl = std::make_shared<Secp256k1SignatureImpl>();
        consensus::ConsensusNodeList consensusNodeList;
        consensus::ConsensusNodeList observerNodeList;
        for (int i = 0; i < 4; ++i)
        {
            auto node = std::make_shared<consensus::ConsensusNode>(signImpl->generateKeyPair()->publicKey(), 10 + i);
            consensusNodeList.emplace_back(node);
        }
        auto observer_node = std::make_shared<consensus::ConsensusNode>(signImpl->generateKeyPair()->publicKey(), -1);
        observerNodeList.emplace_back(observer_node);

        param->setConsensusNodeList(consensusNodeList);
        param->setObserverNodeList(observerNodeList);

        m_storageSetter->createTables(getTableFactory(0));

        auto result = m_ledger->buildGenesisBlock(param);
        BOOST_CHECK(result);
    }
    ~LedgerFixture(){}

    inline TableFactoryInterface::Ptr getTableFactory(const BlockNumber& _number)
    {
        auto hashImpl = std::make_shared<Keccak256Hash>();
        auto table = std::make_shared<TableFactory>(m_storage, hashImpl, _number);
        BOOST_CHECK(table != nullptr);
        return table;
    }

    storage::StorageInterface::Ptr m_storage = nullptr;
    StorageSetter::Ptr m_storageSetter = nullptr;
    StorageGetter::Ptr m_storageGetter = nullptr;
    BlockFactory::Ptr m_blockFactory = nullptr;
    BlockHeaderFactory::Ptr m_headerFactory = nullptr;
    std::shared_ptr<Ledger> m_ledger = nullptr;
};

BOOST_FIXTURE_TEST_SUITE(LedgerTest, LedgerFixture)

BOOST_AUTO_TEST_CASE(testLedger){}

BOOST_AUTO_TEST_SUITE_END()
}
