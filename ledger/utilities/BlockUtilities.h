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
 * @file BlockUtilities.h
 * @author: kyonRay
 * @date 2021-05-06
 */

#pragma once
#include "Common.h"

namespace bcos::ledger
{
protocol::TransactionsPtr blockTransactionListGetter(const protocol::Block::Ptr& _block);

protocol::HashListPtr blockTxHashListGetter(const protocol::Block::Ptr& _block);

size_t blockTransactionListSetter(
    const protocol::Block::Ptr& _block, const protocol::TransactionsPtr& _txs);

protocol::ReceiptsPtr blockReceiptListGetter(const protocol::Block::Ptr& _block);

size_t blockReceiptListSetter(
    const protocol::Block::Ptr& _block, const protocol::ReceiptsPtr& _receipts);
}
