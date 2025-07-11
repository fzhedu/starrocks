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

package com.starrocks.sql.optimizer;

import com.starrocks.alter.AlterJobV2;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;

import java.util.Map;
import java.util.Optional;

public class MVTestUtils {
    private static final Logger LOG = LogManager.getLogger(MVTestUtils.class);

    public static void waitingRollupJobV2Finish() {
        // waiting alterJobV2 finish
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        //Assert.assertEquals(1, alterJobs.size());

        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            if (alterJobV2.getType() != AlterJobV2.JobType.ROLLUP) {
                continue;
            }
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "rollup job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                ThreadUtil.sleepAtLeastIgnoreInterrupts(1000L);
            }
        }
    }

    public static void waitForSchemaChangeAlterJobFinish() {
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                if (alterJobV2.getType() != AlterJobV2.JobType.SCHEMA_CHANGE) {
                    continue;
                }
                LOG.info(
                        "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                ThreadUtil.sleepAtLeastIgnoreInterrupts(100);
            }
            System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
    }

    public static Optional<AlterJobV2> findAlterJobV2(long dbId,
                                                      long tableId) {
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        return alterJobs.values().stream()
                .filter(job -> job.getDbId() == dbId && job.getTableId() == tableId)
                .findFirst();
    }

    public static boolean waitForSchemaChangeAlterJobFinish(AlterJobV2 alterJobV2) {
        while (!alterJobV2.getJobState().isFinalState()) {
            if (alterJobV2.getType() != AlterJobV2.JobType.SCHEMA_CHANGE) {
                return false;
            }
            LOG.info(
                    "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
            ThreadUtil.sleepAtLeastIgnoreInterrupts(100);
        }
        System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        return true;
    }
}
