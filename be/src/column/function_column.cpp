// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#include "column/column_helper.h"
#include "column/chunk.h"
#include "exprs/expr_context.h"
#include "function_column.h"

namespace starrocks::vectorized {
ColumnPtr FunctionColumn::evaluate(starrocks::ExprContext* context, vectorized::Chunk* ptr) {
    return  EVALUATE_NULL_IF_ERROR(context, expr, ptr);
}
}