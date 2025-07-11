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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/bit_stream_utils_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

// Must come before gtest.h.
#include <gtest/gtest.h>

#include <boost/utility/binary.hpp>

#include "util/bit_packing_default.h"
#include "util/bit_stream_utils.h"
#include "util/bit_stream_utils.inline.h"
#include "util/bit_util.h"
#include "util/faststring.h"

using std::string;
using std::vector;

namespace starrocks {

const int kMaxWidth = 64;
class TestBitStreamUtil : public testing::Test {};

TEST(TestBitStreamUtil, TestBool) {
    const int len_bytes = 2;
    faststring buffer(len_bytes);

    BitWriter writer(&buffer);

    // Write alternating 0's and 1's
    for (int i = 0; i < 8; ++i) {
        writer.PutValue(i % 2, 1);
    }
    writer.Flush();
    EXPECT_EQ(buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));

    // Write 00110011
    for (int i = 0; i < 8; ++i) {
        switch (i) {
        case 0:
        case 1:
        case 4:
        case 5:
            writer.PutValue(0, 1);
            break;
        default:
            writer.PutValue(1, 1);
            break;
        }
    }
    writer.Flush();

    // Validate the exact bit value
    EXPECT_EQ(buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));
    EXPECT_EQ(buffer[1], BOOST_BINARY(1 1 0 0 1 1 0 0));

    // Use the reader and validate
    BitReader reader(buffer.data(), buffer.size());
    for (int i = 0; i < 8; ++i) {
        bool val = false;
        bool result = reader.GetValue(1, &val);
        EXPECT_TRUE(result);
        EXPECT_EQ(val, i % 2);
    }

    for (int i = 0; i < 8; ++i) {
        bool val = false;
        bool result = reader.GetValue(1, &val);
        EXPECT_TRUE(result);
        switch (i) {
        case 0:
        case 1:
        case 4:
        case 5:
            EXPECT_EQ(val, false);
            break;
        default:
            EXPECT_EQ(val, true);
            break;
        }
    }
}

// Writes 'num_vals' values with width 'bit_width' and reads them back.
void TestBitArrayValues(int bit_width, int num_vals) {
    const int kTestLen = BitUtil::Ceil(bit_width * num_vals, 8);
    const uint64_t mod = bit_width == 64 ? 1 : 1LL << bit_width;

    faststring buffer(kTestLen);
    BitWriter writer(&buffer);
    for (int i = 0; i < num_vals; ++i) {
        writer.PutValue(i % mod, bit_width);
    }
    writer.Flush();
    EXPECT_EQ(writer.bytes_written(), kTestLen);

    BitReader reader(buffer.data(), kTestLen);
    for (int i = 0; i < num_vals; ++i) {
        int64_t val = 0;
        bool result = reader.GetValue(bit_width, &val);
        EXPECT_TRUE(result);
        EXPECT_EQ(val, i % mod);
    }
    EXPECT_EQ(reader.bytes_left(), 0);
}

TEST(TestBitStreamUtil, TestValues) {
    for (int width = 1; width <= kMaxWidth; ++width) {
        TestBitArrayValues(width, 1);
        TestBitArrayValues(width, 2);
        // Don't write too many values
        TestBitArrayValues(width, (width < 12) ? (1 << width) : 4096);
        TestBitArrayValues(width, 1024);
    }
}

// Test some mixed values
TEST(TestBitStreamUtil, TestMixed) {
    const int kTestLenBits = 1024;
    faststring buffer(kTestLenBits / 8);
    bool parity = true;

    BitWriter writer(&buffer);
    for (int i = 0; i < kTestLenBits; ++i) {
        if (i % 2 == 0) {
            writer.PutValue(parity, 1);
            parity = !parity;
        } else {
            writer.PutValue(i, 10);
        }
    }
    writer.Flush();

    parity = true;
    BitReader reader(buffer.data(), buffer.size());
    for (int i = 0; i < kTestLenBits; ++i) {
        bool result;
        if (i % 2 == 0) {
            bool val = false;
            result = reader.GetValue(1, &val);
            EXPECT_EQ(val, parity);
            parity = !parity;
        } else {
            int val = 0;
            result = reader.GetValue(10, &val);
            EXPECT_EQ(val, i);
        }
        EXPECT_TRUE(result);
    }
}

TEST(TestBitStreamUtil, TestSeekToBit) {
    faststring buffer(1);

    BitWriter writer(&buffer);
    writer.PutValue(2019, 32);
    writer.PutValue(2020, 32);
    writer.PutValue(2021, 32);
    writer.Flush();

    BitReader reader(buffer.data(), buffer.size());
    reader.SeekToBit(buffer.size() * 8 - 8 * 8);
    uint32_t second_value;
    reader.GetValue(32, &second_value);
    ASSERT_EQ(second_value, 2020);

    uint32_t third_value;
    reader.GetValue(32, &third_value);
    ASSERT_EQ(third_value, 2021);

    reader.SeekToBit(0);
    uint32_t first_value;
    reader.GetValue(32, &first_value);
    ASSERT_EQ(first_value, 2019);
}

TEST(TestBitStreamUtil, TestUint64) {
    faststring buffer(1);
    BitWriter writer(&buffer);
    writer.PutValue(18446744073709551614U, 64);
    writer.PutValue(18446744073709551613U, 64);
    writer.PutValue(128, 32);
    writer.PutValue(126, 16);
    writer.Flush();

    BitReader reader(buffer.data(), buffer.size());

    uint64_t v1;
    reader.GetValue(64, &v1);
    ASSERT_EQ(v1, 18446744073709551614U);

    uint64_t v2;
    reader.GetValue(64, &v2);
    ASSERT_EQ(v2, 18446744073709551613U);

    uint64_t v3;
    reader.GetValue(32, &v3);
    ASSERT_EQ(v3, 128);

    uint64_t v4;
    reader.GetValue(16, &v4);
    ASSERT_EQ(v4, 126);
}

TEST(TestBitStreamUtil, TestVLQ) {
    {
        std::vector<int32_t> values = {0, std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::min()};
        for (int i = 0; i < 30; i++) {
            values.push_back((1 << i) - 1);
            values.push_back(-((1 << i) - 1));
            values.push_back(-((1 << i) + 1));
        }
        faststring buffer(1);
        BitWriter writer(&buffer);
        for (auto v : values) {
            writer.PutZigZagVlqInt(v);
        }

        BitReader reader(buffer.data(), buffer.size());
        for (auto v : values) {
            int32_t val = 0;
            bool result = reader.GetZigZagVlqInt(&val);
            EXPECT_TRUE(result);
            EXPECT_EQ(val, v);
        }
    }
    {
        std::vector<int64_t> values = {0, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::min()};
        for (int i = 0; i < 62; i++) {
            values.push_back((1 << i) - 1);
            values.push_back(-((1 << i) - 1));
            values.push_back(-((1 << i) + 1));
        }
        faststring buffer(1);
        BitWriter writer(&buffer);
        for (auto v : values) {
            writer.PutZigZagVlqInt(v);
        }

        BitReader reader(buffer.data(), buffer.size());
        for (auto v : values) {
            int64_t val = 0;
            bool result = reader.GetZigZagVlqInt(&val);
            EXPECT_TRUE(result);
            EXPECT_EQ(val, v);
        }
    }
}

TEST(TestBitStreamUtil, TestGetBatch) {
    auto f = [](int BASE, int N, int BW) {
        faststring buffer(1);
        BitWriter writer(&buffer);
        ASSERT_TRUE((1 << BW) >= (BASE + 2 * N)) << "BW: " << BW << " BASE: " << BASE << " N: " << N;
        for (int i = 0; i < 2 * N; i++) {
            writer.PutValue(BASE + i, BW);
        }
        writer.Flush();

        BitReader reader(buffer.data(), buffer.size());
        std::vector<int> data(N);
        EXPECT_TRUE(reader.GetBatch(BW, data.data(), N));
        for (int i = 0; i < N; i++) {
            EXPECT_EQ(data[i], i + BASE);
        }
        EXPECT_TRUE(reader.GetBatch(BW, data.data(), N));
        for (int i = 0; i < N; i++) {
            EXPECT_EQ(data[i], i + BASE + N);
        }
    };

    for (int bw = 9; bw < 20; bw++) {
        for (int bwi = 0; bwi < 10; bwi++) {
            for (int ni = 0; ni < 10; ni++) {
                f(1 << (bw - 7), 64 - ni, bw + bwi);
            }
        }
    }
}

TEST(TestBitStreamUtil, BatchedBitReaderGetBytes) {
    uint8_t data[4] = {0x8, 0x1, 0x0, 0x0};

    BatchedBitReader reader;
    reader.reset(data, 4);

    uint32_t value;
    ASSERT_TRUE(reader.get_bytes(4, &value));
    ASSERT_EQ(264, value);
}

TEST(TestBitStreamUtil, BatchedBitReaderUnpackBatch) {
    uint8_t data[starrocks::util::bitpacking_default::MAX_BITWIDTH * 48 / 8];
    for (unsigned char& i : data) {
        i = 0x8;
    }

    BatchedBitReader reader;
    reader.reset(data, 24);
    uint64_t result[48];
    int64_t num = reader.unpack_batch<uint64_t>(4, 48, result);
    ASSERT_EQ(num, 48);

    for (size_t i = 0; i < 48; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result[i], 8);
        } else {
            ASSERT_EQ(result[i], 0);
        }
    }
}
} // namespace starrocks
