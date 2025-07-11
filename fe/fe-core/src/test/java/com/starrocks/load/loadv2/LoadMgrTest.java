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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/LoadManagerTest.java

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

package com.starrocks.load.loadv2;

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadMgrTest {
    private LoadMgr loadManager;
    private final String fieldName = "idToLoadJob";

    @AfterEach
    public void tearDown() throws Exception {
        File file = new File("./loadManagerTest");
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testSerializationNormal(@Mocked GlobalStateMgr globalStateMgr,
                                        @Injectable Database database,
                                        @Injectable Table table) throws Exception {
        loadManager = new LoadMgr(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, System.currentTimeMillis(), "", "", null);
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);

        UtFrameUtils.PseudoImage file = serializeToFile(loadManager);

        LoadMgr newLoadManager = deserializeFromFile(file);

        Map<Long, LoadJob> loadJobs = Deencapsulation.getField(loadManager, fieldName);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);
        Assertions.assertEquals(loadJobs, newLoadJobs);
    }

    @Test
    public void testSerializationWithJobRemoved(@Mocked GlobalStateMgr globalStateMgr,
                                                @Injectable Database database,
                                                @Injectable Table table) throws Exception {
        loadManager = new LoadMgr(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, System.currentTimeMillis(), "", "", null);
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);

        //make job1 don't serialize
        Config.label_keep_max_second = 1;
        Thread.sleep(2000);

        UtFrameUtils.PseudoImage  file = serializeToFile(loadManager);

        LoadMgr newLoadManager = deserializeFromFile(file);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);

        Assertions.assertEquals(0, newLoadJobs.size());
    }

    @Test
    public void testDeserializationWithJobRemoved(@Mocked GlobalStateMgr globalStateMgr,
                                                @Injectable Database database,
                                                @Injectable Table table) throws Exception {
        Config.label_keep_max_second = 10;

        // 1. serialize 1 job to file
        loadManager = new LoadMgr(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, System.currentTimeMillis(), "", "", null);
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);
        UtFrameUtils.PseudoImage  file = serializeToFile(loadManager);

        // 2. read it directly, expect 1 job
        LoadMgr newLoadManager = deserializeFromFile(file);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);
        Assertions.assertEquals(1, newLoadJobs.size());

        // 3. set max keep second to 1, then read it again
        // the job expired, expect read 0 job
        Config.label_keep_max_second = 1;
        Thread.sleep(2000);
        newLoadManager = deserializeFromFile(file);
        newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);
        Assertions.assertEquals(0, newLoadJobs.size());
    }

    private UtFrameUtils.PseudoImage serializeToFile(LoadMgr loadManager) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        loadManager.saveLoadJobsV2JsonFormat(image.getImageWriter());
        return image;
    }

    private LoadMgr deserializeFromFile(UtFrameUtils.PseudoImage image) throws Exception {
        LoadMgr loadManager = new LoadMgr(new LoadJobScheduler());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        loadManager.loadLoadJobsV2JsonFormat(reader);
        return loadManager;
    }

    @Test
    public void testRemoveOldLoadJob(@Mocked GlobalStateMgr globalStateMgr,
                                     @Injectable Database db) throws Exception {
        loadManager = new LoadMgr(new LoadJobScheduler());
        int origLabelKeepMaxSecond = Config.label_keep_max_second;
        int origLabelKeepMaxNum = Config.label_keep_max_num;
        Map<Long, LoadJob> idToLoadJob = Deencapsulation.getField(loadManager, "idToLoadJob");
        Map<Long, Map<String, List<LoadJob>>> dbIdToLabelToLoadJobs = Deencapsulation.getField(
                loadManager, "dbIdToLabelToLoadJobs");
        long currentTimeMs = System.currentTimeMillis();

        // finished insert job
        LoadJob job0 = new InsertLoadJob("job0", 0L, 1L, currentTimeMs - 101000, "", "", null);
        job0.id = 10;
        job0.finishTimestamp = currentTimeMs - 101000;
        Deencapsulation.invoke(loadManager, "addLoadJob", job0);

        // broker load job
        // loading
        LoadJob job1 = new BrokerLoadJob(1L, "job1", null, null, null);
        job1.state = JobState.LOADING;
        job1.id = 11;
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);
        // cancelled
        LoadJob job2 = new BrokerLoadJob(1L, "job2", null, null, null);
        job2.finishTimestamp = currentTimeMs - 3000;
        job2.state = JobState.CANCELLED;
        job2.id = 16;
        Deencapsulation.invoke(loadManager, "addLoadJob", job2);
        // finished
        LoadJob job22 = new BrokerLoadJob(1L, "job2", null, null, null);
        job22.finishTimestamp = currentTimeMs - 1000;
        job22.state = JobState.FINISHED;
        job22.id = 12;
        Deencapsulation.invoke(loadManager, "addLoadJob", job22);

        // spark load job
        // etl
        LoadJob job3 = new SparkLoadJob(2L, "job3", null, null);
        job3.state = JobState.ETL;
        job3.id = 13;
        Deencapsulation.invoke(loadManager, "addLoadJob", job3);
        // cancelled
        LoadJob job4 = new SparkLoadJob(2L, "job4", null, null);
        job4.finishTimestamp = currentTimeMs - 51000;
        job4.state = JobState.CANCELLED;
        job4.id = 14;
        Deencapsulation.invoke(loadManager, "addLoadJob", job4);
        // finished
        LoadJob job42 = new SparkLoadJob(2L, "job4", null, null);
        job42.finishTimestamp = currentTimeMs - 2000;
        job42.state = JobState.FINISHED;
        job42.id = 15;
        Deencapsulation.invoke(loadManager, "addLoadJob", job42);

        Assertions.assertEquals(7, idToLoadJob.size());
        Assertions.assertEquals(3, dbIdToLabelToLoadJobs.size());

        // test remove jobs by label_keep_max_second
        // remove db 0, job0
        Config.label_keep_max_second = 100;
        Config.label_keep_max_num = 10;
        loadManager.removeOldLoadJob();
        System.out.println(idToLoadJob);
        Assertions.assertEquals(6, idToLoadJob.size());
        Assertions.assertFalse(idToLoadJob.containsKey(10L));
        Assertions.assertEquals(2, dbIdToLabelToLoadJobs.size());
        Assertions.assertFalse(dbIdToLabelToLoadJobs.containsKey(0L));

        // remove cancelled job4
        Config.label_keep_max_second = 50;
        Config.label_keep_max_num = 10;
        loadManager.removeOldLoadJob();
        System.out.println(idToLoadJob);
        Assertions.assertEquals(5, idToLoadJob.size());
        Assertions.assertFalse(idToLoadJob.containsKey(14L));
        Assertions.assertEquals(2, dbIdToLabelToLoadJobs.size());
        Assertions.assertEquals(1, dbIdToLabelToLoadJobs.get(2L).get("job4").size());

        // test remove jobs by label_keep_max_num
        // remove cancelled job2, finished job4
        Config.label_keep_max_second = 50;
        Config.label_keep_max_num = 3;
        loadManager.removeOldLoadJob();
        System.out.println(idToLoadJob);
        Assertions.assertEquals(3, idToLoadJob.size());
        Assertions.assertFalse(idToLoadJob.containsKey(15L));
        Assertions.assertFalse(idToLoadJob.containsKey(16L));
        Assertions.assertEquals(2, dbIdToLabelToLoadJobs.size());
        Assertions.assertEquals(1, dbIdToLabelToLoadJobs.get(1L).get("job2").size());
        Assertions.assertFalse(dbIdToLabelToLoadJobs.get(2L).containsKey("job4"));

        // remove finished job2
        Config.label_keep_max_second = 50;
        Config.label_keep_max_num = 1;
        loadManager.removeOldLoadJob();
        System.out.println(idToLoadJob);
        Assertions.assertEquals(2, idToLoadJob.size());
        Assertions.assertFalse(idToLoadJob.containsKey(12L));
        Assertions.assertEquals(2, dbIdToLabelToLoadJobs.size());
        Assertions.assertFalse(dbIdToLabelToLoadJobs.get(1L).containsKey("job2"));

        // recover config
        Config.label_keep_max_second = origLabelKeepMaxSecond;
        Config.label_keep_max_num = origLabelKeepMaxNum;
    }

    @Test
    public void testLoadJsonImage(@Mocked GlobalStateMgr globalStateMgr,
                                  @Injectable Database db) throws Exception {
        LoadMgr loadManager = new LoadMgr(new LoadJobScheduler());
        LoadJob loadJob1 = new InsertLoadJob("job0", 0L, 1L, System.currentTimeMillis(), "", "", null);
        loadJob1.id = 1L;
        loadManager.replayCreateLoadJob(loadJob1);

        LoadJob loadJob2 = new BrokerLoadJob(1L, "job1", new BrokerDesc("DUMMY", new HashMap<>()), null, null);
        loadJob2.id = 2L;
        loadManager.replayCreateLoadJob(loadJob2);

        LoadJob loadJob3 = new SparkLoadJob(2L, "job3", null, null);
        loadJob3.id = 3L;
        loadManager.replayCreateLoadJob(loadJob3);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();

        loadManager.saveLoadJobsV2JsonFormat(image.getImageWriter());

        LoadMgr loadManager2 = new LoadMgr(new LoadJobScheduler());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        loadManager2.loadLoadJobsV2JsonFormat(reader);
        reader.close();

        Map<Long, LoadJob> idToLoadJob = Deencapsulation.getField(loadManager2, "idToLoadJob");

        Assertions.assertEquals(3, idToLoadJob.size());
    }

    @Test
    public void testGetLoadJobsByDb(@Mocked GlobalStateMgr globalStateMgr) throws MetaNotFoundException {
        LoadMgr loadMgr = new LoadMgr(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, System.currentTimeMillis(), "", "", null);
        Deencapsulation.invoke(loadMgr, "addLoadJob", job1);
        Assertions.assertTrue(loadMgr.getLoadJobsByDb(2L, "job1", true).isEmpty());
        Assertions.assertEquals(1, loadMgr.getLoadJobsByDb(1L, "job1", true).size());
    }

    @Test
    public void testGetRunningLoadCount() {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.setId(1);
        brokerLoadJob.setLabel("label1");
        brokerLoadJob.setState(JobState.LOADING);
        brokerLoadJob.setWarehouseId(1);

        InsertLoadJob insertLoadJob = new InsertLoadJob();
        insertLoadJob.setId(2);
        insertLoadJob.setLabel("label2");
        insertLoadJob.setState(JobState.LOADING);
        insertLoadJob.setWarehouseId(2);

        SparkLoadJob sparkLoadJob = new SparkLoadJob();
        sparkLoadJob.setId(3);
        sparkLoadJob.setLabel("label3");
        sparkLoadJob.setState(JobState.LOADING);
        sparkLoadJob.setWarehouseId(3);

        LoadMgr loadMgr = new LoadMgr(null);
        loadMgr.addLoadJob(brokerLoadJob);
        loadMgr.addLoadJob(insertLoadJob);
        loadMgr.addLoadJob(sparkLoadJob);

        Map<Long, Long> result = loadMgr.getRunningLoadCount();
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(Long.valueOf(1), result.get(1L));
        Assertions.assertEquals(Long.valueOf(1), result.get(3L));
    }
}
