// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#pragma once
#include "column/bytes.h"
#include "column/column.h"
#include "column/datum.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "util/slice.h"

namespace starrocks::vectorized {

class FunctionColumn final : public ColumnFactory<Column, FunctionColumn> {
    friend class ColumnFactory<Column, FunctionColumn>;
public:
    FunctionColumn(Expr* a): expr(a) {}

    ~FunctionColumn() override = default;

    const uint8_t* raw_data() const override {return nullptr;}

    uint8_t* mutable_raw_data() override {return nullptr;}

    size_t size() const override { return 0;}

    size_t capacity() const override {return 0;}

    size_t type_size() const override { return 0; }

    size_t byte_size() const override { return 0; }
    size_t byte_size(size_t from, size_t size) const override {return 0;}

    size_t byte_size(size_t idx) const override {return 0;}

    void reserve(size_t n) override {}

    bool is_function() {return true;}

    int * get_data() {return nullptr;}

    void resize(size_t n) override {}

    void assign(size_t n, size_t idx) override{}

    void append_datum(const Datum& datum) override {}

    void append(const Column& src, size_t offset, size_t count) override {}

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override {}

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override {}

    bool append_nulls(size_t count) override {return true;}

    bool append_strings(const Buffer<Slice>& strs) override { return false; }

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override {}

    void append_default() override {}

    void append_default(size_t count) override {}

    void fill_default(const Filter& filter) override {}

    Status update_rows(const Column& src, const uint32_t* indexes) override { throw std::runtime_error(get_name()); }

    void remove_first_n_values(size_t count) override {}

    uint32_t max_one_element_serialize_size() const override {return 0;}

    uint32_t serialize(size_t idx, uint8_t* pos) override {return 0;}

    uint32_t serialize_default(uint8_t* pos) override {return 0;}

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override {}

    const uint8_t* deserialize_and_append(const uint8_t* pos) override {return nullptr;}

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override {}

    uint32_t serialize_size(size_t idx) const override {return 0;}

    MutableColumnPtr clone_empty() const override { throw std::runtime_error(get_name()); }

    size_t filter_range(const Filter& filter, size_t from, size_t to) override{return 0;}

    int compare_at(size_t left, size_t right, const Column& right_column, int nan_direction_hint) const override{return 0;}

    void crc32_hash_at(uint32_t* seed, int32_t idx) const override{}
    void fnv_hash_at(uint32_t* seed, int32_t idx) const override {}
    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override {}

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override {}
    int64_t xor_checksum(uint32_t from, uint32_t to) const override {return 0;}

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override {}

    std::string get_name() const override { return "function"; }

    Datum get(size_t idx) const override{ throw std::runtime_error(get_name()); }

    size_t get_element_size(size_t idx) const{return 0;}

    bool set_null(size_t idx) override {return true;}

    size_t memory_usage() const override { return 0; }

    size_t container_memory_usage() const override {
        return 0;
    }

    size_t element_memory_usage(size_t from, size_t size) const override {return 0;}

    void swap_column(Column& rhs) override {}

    void reset_column() override{}

    bool is_nullable() const override { return false; }

    std::string debug_item(uint32_t idx) const override {return "function";}

    std::string debug_string() const override {return "function";}

    bool capacity_limit_reached(std::string* msg = nullptr) const override {
        return false;
    }

    StatusOr<ColumnPtr> upgrade_if_overflow() override{ throw std::runtime_error(get_name()); }

    StatusOr<ColumnPtr> downgrade() override { throw std::runtime_error(get_name()); }

    bool has_large_column() const override { return false; }
    void check_or_die() const override {}

    ColumnPtr evaluate(starrocks::ExprContext* context, vectorized::Chunk* ptr);
private:
    Expr* expr;
};
}
