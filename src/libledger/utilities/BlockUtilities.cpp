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
 * @file BlockUtilities.cpp
 * @author: kyonRay
 * @date 2021-05-07
 */

#include "BlockUtilities.h"
#include <bcos-framework/interfaces/protocol/Block.h>

namespace bcos::ledger
{
protocol::TransactionsPtr blockTransactionListGetter(const protocol::Block::Ptr& _block)
{
    auto txs = std::make_shared<protocol::Transactions>();
    if (_block == nullptr)
    {
        return nullptr;
    }
    auto txSize = _block->transactionsSize();
    if (txSize == 0)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Block transactions size is 0, return empty");
        return txs;
    }
    for (size_t i = 0; i < txSize; ++i)
    {
        auto tx = std::const_pointer_cast<protocol::Transaction>(_block->transaction(i));
        txs->emplace_back(tx);
    }
    return txs;
}

std::shared_ptr<std::vector<std::string>> blockTxHashListGetter(const protocol::Block::Ptr& _block)
{
    auto txHashList = std::make_shared<std::vector<std::string>>();
    if (_block == nullptr)
    {
        return nullptr;
    }
    auto txHashSize = _block->transactionsHashSize();
    if (txHashSize == 0)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Block transactions size is 0, return empty");
        return txHashList;
    }
    for (size_t i = 0; i < txHashSize; ++i)
    {
        txHashList->emplace_back(_block->transactionHash(i).hex());
    }
    return txHashList;
}


int blockTransactionListSetter(
    const protocol::Block::Ptr& _block, const protocol::TransactionsPtr& _txs)
{
    if (_block == nullptr || _txs == nullptr)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("blockTransactionListSetter nullptr args");
        return -1;
    }
    for (const auto& tx : *_txs)
    {
        _block->appendTransaction(tx);
        _block->appendTransactionHash(tx->hash());
    }
    return _block->transactionsSize();
}

protocol::ReceiptsPtr blockReceiptListGetter(const protocol::Block::Ptr& _block)
{
    auto receipts = std::make_shared<protocol::Receipts>();
    if (_block == nullptr)
    {
        return nullptr;
    }
    auto receiptSize = _block->receiptsSize();
    if (receiptSize == 0)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Block receipts size is 0, return empty");
        return receipts;
    }
    for (size_t i = 0; i < receiptSize; ++i)
    {
        auto receipt = std::const_pointer_cast<protocol::TransactionReceipt>(_block->receipt(i));
        receipts->emplace_back(receipt);
    }
    return receipts;
}

int blockReceiptListSetter(
    const protocol::Block::Ptr& _block, const protocol::ReceiptsPtr& _receipts)
{
    if (_block == nullptr || _receipts == nullptr)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("blockReceiptListSetter nullptr args");
        return -1;
    }
    for (const auto& rcpt : *_receipts)
    {
        _block->appendReceipt(rcpt);
    }
    return _block->receiptsSize();
}

bcos::protocol::Block::Ptr decodeBlock(
    const protocol::BlockFactory::Ptr& _blockFactory, const std::string_view& _blockStr)
{
    protocol::Block::Ptr block = nullptr;
    try
    {
        block = _blockFactory->createBlock(
            bytesConstRef((bcos::byte*)_blockStr.data(), _blockStr.size()), false, false);
    }
    catch (std::exception const& e)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("decodeBlock") << LOG_DESC("decode error, return nullptr")
                          << LOG_KV("blockStr", _blockStr)
                          << LOG_KV("error", boost::diagnostic_information(e));
    }
    return block;
}

bcos::protocol::BlockHeader::Ptr decodeBlockHeader(
    const protocol::BlockHeaderFactory::Ptr& _headerFactory, const std::string_view& _headerStr)
{
    protocol::BlockHeader::Ptr header = nullptr;
    try
    {
        header = _headerFactory->createBlockHeader(
            bytesConstRef((bcos::byte*)_headerStr.data(), _headerStr.size()));
    }
    catch (std::exception const& e)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("decodeBlockHeader")
                          << LOG_DESC("decode error, return nullptr")
                          << LOG_KV("headerStr", _headerStr)
                          << LOG_KV("error", boost::diagnostic_information(e));
    }
    return header;
}

bcos::protocol::Transaction::Ptr decodeTransaction(
    const protocol::TransactionFactory::Ptr& _txFactory, const std::string& _txStr)
{
    protocol::Transaction::Ptr tx = nullptr;
    try
    {
        tx = _txFactory->createTransaction(asBytes(_txStr), false);
    }
    catch (std::exception const& e)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("decodeTransaction")
                          << LOG_DESC("decode error, return nullptr") << LOG_KV("txStr", _txStr)
                          << LOG_KV("error", boost::diagnostic_information(e));
    }
    return tx;
}

bcos::protocol::TransactionReceipt::Ptr decodeReceipt(
    const protocol::TransactionReceiptFactory::Ptr& _receiptFactory, const std::string& _receiptStr)
{
    protocol::TransactionReceipt::Ptr receipt = nullptr;
    try
    {
        receipt = _receiptFactory->createReceipt(asBytes(_receiptStr));
    }
    catch (std::exception const& e)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("decodeReceipt") << LOG_DESC("decode error, return nullptr")
                          << LOG_KV("receiptStr", _receiptStr)
                          << LOG_KV("error", boost::diagnostic_information(e));
    }
    return receipt;
}
}  // namespace bcos::ledger
