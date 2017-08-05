package cz.zcu.kiv.Server;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

/***********************************************************************************************************************
 *
 * This file is part of the ExecutionServer project

 * ==========================================
 *
 * Copyright (C) 2017 by University of West Bohemia (http://www.zcu.cz/en/)
 *
 ***********************************************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 ***********************************************************************************************************************
 *
 * cz.zcu.kiv.Server.Initialize, 2017/07/20 10:17 dbg
 *
 **********************************************************************************************************************/
public class Initialize {

    public static void initFolder() {
        new File(System.getProperty("user.home") + "/spark_server").mkdirs();
        new File(System.getProperty("user.home") + "/spark_server/classifiers").mkdirs();
        new File(System.getProperty("user.home") + "/spark_server/scripts").mkdirs();
        new File(System.getProperty("user.home") + "/spark_server/results").mkdirs();
        new File(System.getProperty("user.home") + "/spark_server/logs").mkdirs();

    }

    @Test
    public void test() throws IOException {
        initFolder();
    }
}
