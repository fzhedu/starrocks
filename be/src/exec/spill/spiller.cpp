// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/spill/spiller.h"

#include <butil/iobuf.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

#include "column/chunk.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/sort_exec_exprs.h"
#include "exec/spill/common.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "exec/spill/spiller.hpp"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks::spill {

SpillProcessMetrics::SpillProcessMetrics(RuntimeProfile* profile, std::atomic_int64_t* total_spill_bytes_) {
    DCHECK(profile != nullptr);
    total_spill_bytes = total_spill_bytes_;

    std::string parent = "SpillStatistics";
    ADD_COUNTER(profile, parent, TUnit::NONE);

    append_data_timer = ADD_CHILD_TIMER(profile, "AppendDataTime", parent);
    spill_rows = ADD_CHILD_COUNTER(profile, "RowsSpilled", TUnit::UNIT, parent);
    flush_timer = ADD_CHILD_TIMER(profile, "FlushTime", parent);

    write_io_timer = ADD_CHILD_TIMER(profile, "WriteIOTime", parent);
    local_write_io_timer = ADD_CHILD_TIMER(profile, "LocalWriteIOTime", "WriteIOTime");
    remote_write_io_timer = ADD_CHILD_TIMER(profile, "RemoteWriteIOTime", "WriteIOTime");

    restore_rows = ADD_CHILD_COUNTER(profile, "RowsRestored", TUnit::UNIT, parent);
    restore_from_buffer_timer = ADD_CHILD_TIMER(profile, "RestoreTime", parent);

    read_io_timer = ADD_CHILD_TIMER(profile, "ReadIOTime", parent);
    local_read_io_timer = ADD_CHILD_TIMER(profile, "LocalReadIOTime", "ReadIOTime");
    remote_read_io_timer = ADD_CHILD_TIMER(profile, "RemoteReadIOTime", "ReadIOTime");

    flush_bytes = ADD_CHILD_COUNTER(profile, "BytesFlush", TUnit::BYTES, parent);
    local_flush_bytes = ADD_CHILD_COUNTER(profile, "BytesFlushToLocalDisk", TUnit::BYTES, "BytesFlush");
    remote_flush_bytes = ADD_CHILD_COUNTER(profile, "BytesFlushToRemoteStorage", TUnit::BYTES, "BytesFlush");

    restore_bytes = ADD_CHILD_COUNTER(profile, "BytesRestore", TUnit::BYTES, parent);
    local_restore_bytes = ADD_CHILD_COUNTER(profile, "BytesRestoreFromLocalDisk", TUnit::BYTES, "BytesRestore");
    remote_restore_bytes = ADD_CHILD_COUNTER(profile, "BytesRestoreFromRemoteStorage", TUnit::BYTES, "BytesRestore");

    serialize_timer = ADD_CHILD_TIMER(profile, "SerializeTime", parent);
    deserialize_timer = ADD_CHILD_TIMER(profile, "DeserializeTime", parent);
    mem_table_peak_memory_usage = profile->AddHighWaterMarkCounter(
            "MemTablePeakMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES), parent);
    input_stream_peak_memory_usage = profile->AddHighWaterMarkCounter(
            "InputStreamPeakMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES), parent);

    sort_chunk_timer = ADD_CHILD_TIMER(profile, "SortChunkTime", parent);
    materialize_chunk_timer = ADD_CHILD_TIMER(profile, "MaterializeChunkTime", parent);
    shuffle_timer = ADD_CHILD_TIMER(profile, "ShuffleTime", parent);
    split_partition_timer = ADD_CHILD_TIMER(profile, "SplitPartitionTime", parent);
    restore_from_mem_table_rows = ADD_CHILD_COUNTER(profile, "RowsRestoreFromMemTable", TUnit::UNIT, parent);
    restore_from_mem_table_bytes = ADD_CHILD_COUNTER(profile, "BytesRestoreFromMemTable", TUnit::UNIT, parent);
    partition_writer_peak_memory_usage =
            profile->AddHighWaterMarkCounter("PartitionWriterPeakMemoryBytes", TUnit::BYTES,
                                             RuntimeProfile::Counter::create_strategy(TUnit::BYTES), parent);

    block_count = ADD_CHILD_COUNTER(profile, "BlockCount", TUnit::NONE, parent);
    local_block_count = ADD_CHILD_COUNTER(profile, "LocalBlockCount", TUnit::UNIT, "BlockCount");
    remote_block_count = ADD_CHILD_COUNTER(profile, "RemoteBlockCount", TUnit::UNIT, "BlockCount");

    read_io_count = ADD_CHILD_COUNTER(profile, "ReadIOCount", TUnit::UNIT, parent);
    local_read_io_count = ADD_CHILD_COUNTER(profile, "LocalReadIOCount", TUnit::UNIT, "ReadIOCount");
    remote_read_io_count = ADD_CHILD_COUNTER(profile, "RemoteReadIOCount", TUnit::UNIT, "ReadIOCount");

    compact_count = ADD_CHILD_COUNTER(profile, "CompactCount", TUnit::UNIT, parent);
    compact_block_count = ADD_CHILD_COUNTER(profile, "CompactBlockCount", TUnit::UNIT, parent);

    flush_io_task_count = ADD_CHILD_COUNTER(profile, "FlushIOTaskCount", TUnit::UNIT, parent);
    peak_flush_io_task_count = profile->AddHighWaterMarkCounter(
            "PeakFlushIOTaskCount", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT), parent);
    restore_io_task_count = ADD_CHILD_COUNTER(profile, "RestoreIOTaskCount", TUnit::UNIT, parent);
    peak_restore_io_task_count = profile->AddHighWaterMarkCounter(
            "PeakRestoreIOTaskCount", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT), parent);

    mem_table_finalize_timer = ADD_CHILD_TIMER(profile, "MemTableFinalizeTime", parent);
    flush_task_yield_times = ADD_CHILD_COUNTER(profile, "FlushIOTaskYieldCount", TUnit::UNIT, parent);
    restore_task_yield_times = ADD_CHILD_COUNTER(profile, "RestoreIOTaskYieldCount", TUnit::UNIT, parent);

    skew_mem_table_count = ADD_CHILD_COUNTER(profile, "SkewMemTableCount", TUnit::UNIT, parent);
    skew_mem_table_skew_ratio = profile->AddLowWaterMarkCounter(
            "SkewMemTableSkewRatio", TUnit::DOUBLE_VALUE,
            RuntimeProfile::Counter::create_strategy(TCounterAggregateType::AVG), parent);
    skew_mem_table_merge_timer = ADD_CHILD_TIMER(profile, "SkewMemTableMergeTime", parent);
    skew_mem_table_input_bytes = ADD_CHILD_COUNTER(profile, "SkewMemTableInputBytes", TUnit::BYTES, parent);
    skew_mem_table_output_bytes = ADD_CHILD_COUNTER(profile, "SkewMemTableOutputBytes", TUnit::BYTES, parent);
    skew_mem_table_input_rows = ADD_CHILD_COUNTER(profile, "SkewMemTableInputRows", TUnit::UNIT, parent);
    skew_mem_table_output_rows = ADD_CHILD_COUNTER(profile, "SkewMemTableOutputRows", TUnit::UNIT, parent);
}

Status Spiller::prepare(RuntimeState* state) {
    _chunk_builder.chunk_schema() = std::make_shared<SpilledChunkBuildSchema>();
#ifndef BE_TEST
    DCHECK(_opts.wg != nullptr) << "workgroup must be set";
#endif

    ASSIGN_OR_RETURN(_serde, Serde::create_serde(this));

    if (_opts.init_partition_nums > 0) {
        _writer = std::make_unique<PartitionedSpillerWriter>(this, state);
    } else {
        _writer = std::make_unique<RawSpillerWriter>(this, state);
    }

    _writer->prepare(state);

    _reader = std::make_unique<SpillerReader>(this);

    if (!_opts.is_unordered) {
        DCHECK(_opts.init_partition_nums == -1);
    }

    _block_manager = _opts.block_manager;

    return Status::OK();
}

void Spiller::set_partition(const std::vector<const SpillPartitionInfo*>& parititons) {
    DCHECK_GT(_opts.init_partition_nums, 0);
    down_cast<PartitionedSpillerWriter*>(_writer.get())->reset_partition(parititons);
}

void Spiller::set_partition(RuntimeState* state, size_t num_partitions) {
    down_cast<PartitionedSpillerWriter*>(_writer.get())->reset_partition(state, num_partitions);
}

void Spiller::update_spilled_task_status(Status&& st) {
    std::lock_guard guard(_mutex);
    if (_spilled_task_status.ok() && !st.ok()) {
        _spilled_task_status = std::move(st);
    }
}

Status Spiller::reset_state(RuntimeState* state) {
    _spilled_append_rows = 0;
    _restore_read_rows = 0;
    RETURN_IF_ERROR(prepare(state));
    return Status::OK();
}

std::vector<std::shared_ptr<SpillerReader> > Spiller::get_partition_spill_readers(
        const std::vector<const SpillPartitionInfo*>& partitions) {
    std::vector<std::shared_ptr<SpillerReader> > res;

    for (auto partition : partitions) {
        res.emplace_back(std::make_unique<SpillerReader>(this));
        std::shared_ptr<SpillInputStream> stream;
        // TODO check return status
        CHECK(_writer->acquire_stream(partition, &stream).ok());
        res.back()->set_stream(std::move(stream));
    }

    return res;
}

Status Spiller::_acquire_input_stream(RuntimeState* state) {
    std::shared_ptr<SpillInputStream> input_stream;

    RETURN_IF_ERROR(_writer->acquire_stream(&input_stream));
    _reader->set_stream(std::move(input_stream));

    return Status::OK();
}

void Spiller::_init_max_block_nums() {
    size_t chunk_avg_mem_size = _chunk_builder.chunk_schema()->chunk_avg_mem_size();
    chunk_avg_mem_size = std::max<size_t>(1, chunk_avg_mem_size);
    _max_sorted_block_cnt = std::max<size_t>(16, _opts.spill_mem_table_bytes_size / chunk_avg_mem_size);
    TRACE_SPILL_LOG << "spill max block cnt:" << _max_sorted_block_cnt << ",avg size " << chunk_avg_mem_size;
}

} // namespace starrocks::spill
