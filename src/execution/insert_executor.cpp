//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_plan_(std::move(child_executor)) {
        auto *catalog = exec_ctx_->GetCatalog();
        auto *tableInfo = catalog->GetTable(plan_->TableOid());
        indexes_ = catalog->GetTableIndexes(tableInfo->name_);
    }

void InsertExecutor::Init() {
    child_plan_->Init();
    done_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    if (done_) return false;
    auto *catalog = exec_ctx_->GetCatalog();
    auto *tableInfo = catalog->GetTable(plan_->TableOid());
    auto &tableHeap = tableInfo->table_;
    auto n = 0;
    for(;child_plan_->Next(tuple, rid);n++) {
        tableHeap->InsertTuple(TupleMeta{}, *tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), tableInfo->oid_);
        for (auto itr = indexes_.begin(); itr != indexes_.end(); itr++) {
            auto indexInfo = *itr;
            auto key = tuple->KeyFromTuple(tableInfo->schema_, indexInfo->key_schema_, indexInfo->index_->GetKeyAttrs());
            indexInfo->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
        }
    }
    *tuple = Tuple({Value(TypeId::INTEGER, n)}, &plan_->OutputSchema());
    done_ = true;
    return true;
}

}  // namespace bustub
