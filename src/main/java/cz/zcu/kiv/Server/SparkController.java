package cz.zcu.kiv.Server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.WebRequest;

import java.io.*;
import java.util.*;

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

    LinkedHashMap<Integer,SparkSubmitService> jobManager = new LinkedHashMap<Integer,SparkSubmitService>(300);

    @Async
    @RequestMapping(value = "/jobs/submit/{id}",method = RequestMethod.GET)
    public String submitJob(@PathVariable Integer id, @RequestParam Map<String,String> queryMap) throws IOException{
        logger.info("Received request to submit a job with id " + id);
        //initialize the spark submitter for the given id
        jobManager.put(id,new SparkSubmitService());
        logger.info("Entries " + jobManager.entrySet());
        // actually submit a job
        jobManager.get(id).submitJob(queryMap,id);
        logger.info("Job manager size " + jobManager.size());
        if(jobManager.size() > 150){
            int counter = 0;
            for( Map.Entry<Integer,SparkSubmitService> entry : jobManager.entrySet() ) {
                if(entry.getValue().checkStatus().equals("FINISHED") && counter < 100)
                    jobManager.remove(entry.getKey());
                    counter++;
            }
        }
        return "OK";
    }

    @RequestMapping(value = "/jobs/check/{id}", method = RequestMethod.GET)
    public String checkStatus(@PathVariable Integer id){
        logger.info("Received a request to check job status");
        return jobManager.get(id).checkStatus();
    }

    @RequestMapping(value = "/jobs/result/{id}", method = RequestMethod.GET)
    public String getResults(@PathVariable Integer id) throws IOException {
        logger.info("Received a request to get the result of job");
        return jobManager.get(id).getResults();
    }


    @RequestMapping(value = "/classifiers/list",method = RequestMethod.GET)
    public String getSavedClassifiers(){
        logger.info("Fetching saved classifiers");
        File file = new File(System.getProperty("user.home") + "/spark_server/classifiers");
        File[] listOfFiles = file.listFiles();

        ArrayList<String> filteredFiles = new ArrayList<String>();

        for (File tFile : listOfFiles){
            if(!tFile.getName().startsWith(".") && (file.isDirectory() || file.isFile())){
                filteredFiles.add(tFile.getName());
            }
        }
        return filteredFiles.toString();
    }

    @RequestMapping(value = "/jobs/log/{id}", method = RequestMethod.GET)
    public String getJobLog(@PathVariable Integer id) throws IOException{
        logger.info("Getting the log of a job " + id);
        return jobManager.get(id).getLog(id);
    }

    @RequestMapping(value = "/jobs/configuration/{name}", method = RequestMethod.GET)
    public String getSavedClfConfig(@PathVariable String name) throws IOException {
        logger.info("Getting the configuration of a job " + name);

        StringBuilder sb = new StringBuilder(25);
        String line;
        BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.home") + "/spark_server/configurations/" + name + ".conf"));
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        return sb.toString();
    }


    @RequestMapping(value = "/jobs/cancel/{id}", method = RequestMethod.GET)
    public void cancelJob(@PathVariable Integer id){
        logger.info("Canceling the job with id " + id);
        jobManager.get(id).cancelJob();
    }


    @ExceptionHandler({Exception.class})
    public String error(Exception e) {
        // Nothing to do.  Returns the logical view name of an error page, passed
        // to the view-resolver(s) in usual way.
        // Note that the exception is NOT available to this view (it is not added
        // to the model) but see "Extending ExceptionHandlerExceptionResolver"
        // below.
        logger.error(e.getMessage());

        return e.getMessage();
    }



}
