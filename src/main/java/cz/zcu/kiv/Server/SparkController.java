package cz.zcu.kiv.Server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

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
 * SparkController, 2017/07/19 11:06 dbg
 *
 **********************************************************************************************************************/
@RestController
public class SparkController {
    private static Log logger = LogFactory.getLog(SparkController.class);

    HashMap<Integer,SparkSubmitService> jobManager = new HashMap<>(100);

    @Async
    @RequestMapping(value = "/jobs/submit/{id}",method = RequestMethod.GET)
    public String submitJob(@PathVariable Integer id, @RequestParam Map<String,String> queryMap){
        logger.info("Received request to submit a job with id " + id);
        //initialize the spark submitter for the given id
        jobManager.put(id,new SparkSubmitService());
        logger.info("Entries " + jobManager.entrySet());
        // actually submit a job
        jobManager.get(id).submitJob(queryMap);
        return "OK";
    }

    @RequestMapping(value = "/jobs/check/{id}", method = RequestMethod.GET)
    public String checkStatus(@PathVariable Integer id){
        logger.info("Received a request to check job status");
        return jobManager.get(id).checkStatus();
    }

    @RequestMapping(value = "/jobs/result/{id}", method = RequestMethod.GET)
    public String getResults(@PathVariable Integer id){
        logger.info("Received a request to get the result of job");
        return jobManager.get(id).getResults();
    }
}
