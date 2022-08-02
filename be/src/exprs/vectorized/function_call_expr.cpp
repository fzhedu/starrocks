// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/function_call_expr.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/builtin_functions.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/user_function_cache.h"

namespace starrocks::vectorized {

VectorizedFunctionCallExpr::VectorizedFunctionCallExpr(const TExprNode& node) : Expr(node), _fn_desc(nullptr) {}

Status VectorizedFunctionCallExpr::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));

    if (!_fn.__isset.fid) {
        return Status::InternalError("Vectorized engine doesn't implement function " + _fn.name.function_name);
    }

    _fn_desc = BuiltinFunctions::find_builtin_function(_fn.fid);

    if (_fn_desc == nullptr || _fn_desc->scalar_function == nullptr) {
        return Status::InternalError("Vectorized engine doesn't implement function " + _fn.name.function_name);
    }

    if (_fn_desc->args_nums > _children.size()) {
        return Status::InternalError(strings::Substitute("Vectorized function $0 requires $1 arguments but given $2",
                                                         _fn.name.function_name, _fn_desc->args_nums,
                                                         _children.size()));
    }

    FunctionContext::TypeDesc return_type = AnyValUtil::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> args_types;

    for (Expr* child : _children) {
        args_types.push_back(AnyValUtil::column_type_to_type_desc(child->type()));
    }

    // todo: varargs use for allocate slice memory, need compute buffer size
    //  for varargs in vectorized engine?
    _fn_context_index = context->register_func(state, return_type, args_types, 0);

    _is_returning_random_value = _fn.fid == 10300 /* rand */ || _fn.fid == 10301 /* random */ ||
                                 _fn.fid == 10302 /* rand */ || _fn.fid == 10303 /* random */ ||
                                 _fn.fid == 100015 /* uuid */ || _fn.fid == 100016 /* uniq_id */;

    return Status::OK();
}

Status VectorizedFunctionCallExpr::open(starrocks::RuntimeState* state, starrocks::ExprContext* context,
                                        FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));

    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);

    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        std::vector<ColumnPtr> const_columns;
        const_columns.reserve(_children.size());
        for (const auto& child : _children) {
            ASSIGN_OR_RETURN(auto&& child_col, child->evaluate_const(context))
            const_columns.emplace_back(std::move(child_col));
        }
        fn_ctx->impl()->set_constant_columns(std::move(const_columns));
    }

    if (_fn_desc->prepare_function != nullptr) {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            RETURN_IF_ERROR(_fn_desc->prepare_function(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
        }

        RETURN_IF_ERROR(_fn_desc->prepare_function(fn_ctx, FunctionContext::THREAD_LOCAL));
    }

    // Todo: We will use output_scale in the result_writer to format the
    //  output in row engine, but we need set output scale in vectorized engine?
    if (_fn.name.function_name == "round" && _type.type == TYPE_DOUBLE) {
        if (_children[1]->is_constant()) {
            ColumnPtr ptr = _children[1]->evaluate(context, nullptr);
            _output_scale =
                    std::static_pointer_cast<Int32Column>(std::static_pointer_cast<ConstColumn>(ptr)->data_column())
                            ->get_data()[0];
        }
    }

    return Status::OK();
}

void VectorizedFunctionCallExpr::close(starrocks::RuntimeState* state, starrocks::ExprContext* context,
                                       FunctionContext::FunctionStateScope scope) {
    if (_fn_desc != nullptr && _fn_desc->close_function != nullptr) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        _fn_desc->close_function(fn_ctx, FunctionContext::THREAD_LOCAL);

        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            _fn_desc->close_function(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
        }
    }

    Expr::close(state, context, scope);
}

bool VectorizedFunctionCallExpr::is_constant() const {
    if (_is_returning_random_value) {
        return false;
    }

    return Expr::is_constant();
}

ColumnPtr VectorizedFunctionCallExpr::evaluate(starrocks::ExprContext* context, vectorized::Chunk* ptr) {
    if (_fn_desc->name == "transform") {
        // run the first arg
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(context, _children[0], ptr);
        // take the result column (elements of array) to construct a chunk
        auto _cur_chunk = std::make_shared<vectorized::Chunk>();
        auto array_col = std::dynamic_pointer_cast<ArrayColumn>(column);
        DCHECK(array_col != nullptr);
        auto size = array_col->elements_column()->size();
        DCHECK(size > 0);
        auto str = column->debug_string();
        DCHECK(str.size() > 0);
        auto nullable_col = std::dynamic_pointer_cast<NullableColumn>(array_col->elements_column());
        DCHECK(nullable_col != nullptr);
        _cur_chunk->append_column(nullable_col, 111); // column ref
#if 0
        // use the chunk to invoke the second arg, i.e., lambda function
        column = EVALUATE_NULL_IF_ERROR(context, _children[1], _cur_chunk.get());
#else
        column = EVALUATE_NULL_IF_ERROR(context, _children[1], _cur_chunk.get());
        auto function_col = std::dynamic_pointer_cast<FunctionColumn>(column);
        column = function_col->evaluate(context,_cur_chunk.get());
#endif
        auto str1 = column->debug_string() + " name: " + column->get_name();
        DCHECK(str1.size() > 0);
        nullable_col = std::make_shared<vectorized::NullableColumn>(column, nullable_col->null_column());
        str = nullable_col->debug_string() + " name: " + nullable_col->get_name();
        DCHECK(str.size() > 0);

        // updated elements with offsets to construct the result.
        return std::make_shared<ArrayColumn>(nullable_col, array_col->offsets_column());
    } else if (_fn_desc->name == "lambda") {
        return std::make_shared<FunctionColumn>(_children[0]);
    } else {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);

        Columns args;
        args.reserve(_children.size());
        for (Expr* child : _children) {
            ColumnPtr column = EVALUATE_NULL_IF_ERROR(context, child, ptr);
            args.emplace_back(column);
        }

        if (_is_returning_random_value) {
            if (ptr != nullptr) {
                args.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(ptr->num_rows(), ptr->num_rows()));
            } else {
                args.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(1, 1));
            }
        }

    #ifndef NDEBUG
        if (ptr != nullptr) {
            size_t size = ptr->num_rows();
            // Ensure all columns have the same size
            for (const ColumnPtr& c : args) {
                CHECK_EQ(size, c->size());
            }
        }
    #endif

        ColumnPtr result;
        if (_fn_desc->exception_safe) {
            result = _fn_desc->scalar_function(fn_ctx, args);
        } else {
            SCOPED_SET_CATCHED(false);
            result = _fn_desc->scalar_function(fn_ctx, args);
        }

        // For no args function call (pi, e)
        if (result->is_constant() && ptr != nullptr) {
            result->resize(ptr->num_rows());
        }
        return result;
    }
}

} // namespace starrocks::vectorized
