//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    auto *catalog = exec_ctx_->GetCatalog();
    auto *tableInfo = catalog->GetTable(plan_->GetTableOid());
    auto &tableHeap = tableInfo->table_;
    itr_ = std::make_unique<TableIterator>(tableHeap->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (itr_->IsEnd()) return false;
    while(1) {
        auto res = itr_->GetTuple();
        ++(*itr_);
        if (res.first.is_deleted_) continue;
        *tuple = res.second;
        *rid = res.second.GetRid();
        break;
    }
    return true;
}

}  // namespace bustub
