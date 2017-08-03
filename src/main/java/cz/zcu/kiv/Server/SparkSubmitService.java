package cz.zcu.kiv.Server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.scheduling.annotation.Async;

import java.io.*;
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
 * SparkSubmitService, 2017/07/27 08:34 dbg
 *
 **********************************************************************************************************************/

public class SparkSubmitService {

    private static Log logger = LogFactory.getLog(SparkSubmitService.class);

    private Map<String, String> queryMap;
    Process process;


    @Async
    public void submitJob(Map<String,String> queryMap){
        logger.info("Submitting a job with query parameters = " + queryMap);
        this.queryMap = queryMap;
        String content = "spark-submit " +
                "--class cz.zcu.kiv.Main " +
                "--master local[*] " +
                "--conf spark.driver.host=localhost " +
                "/Users/dorianbeganovic/gsoc/Spark_EEG_Analysis/target/spark_eeg-1.2-jar-with-dependencies.jar " +
                "\"" +
                queryMap.toString().replace("{","").replace("}","").replace(", ","&") +
                "\"";
        logger.info("Content of the script" + content);

        String scriptLocation = System.getProperty("user.home") + "/spark_server/scripts/" + "spark_submit_script.sh";
        logger.info("Script location" + scriptLocation);

        PrintWriter out = null;
        try {
            out = new PrintWriter(scriptLocation);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        out.print(content);
        out.close();


        File script = new File(scriptLocation);
        script.setExecutable(true);
        String[] command = { scriptLocation};

        try {
            // this executes the script
            process = Runtime.getRuntime().exec(command);
            //process.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream()));

        logger.info("Executing the script");
        String s;
        try {
            while ((s = reader.readLine()) != null) {
                // pauses the program while executing
                logger.info("Script output: " + s); // Replace this line with the code to print the result to file
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Script is finished");
    }

    @Async
    public String checkStatus(){
        logger.info("Checking the job status");
        try {
            process.exitValue();
            return "FINISHED";
        } catch (Exception e) {
            return "RUNNING";
        }

        /*
        if (flag == false) {
            logger.info("Job is running");
            return ;
        } else {
            logger.info("Job has finsihed");
            return ;
        }
        */
    }

    @Async
    public String getResults(){
        logger.info("Reading results for job in location " + queryMap.get("result_path"));

        try(BufferedReader br = new BufferedReader(new FileReader(queryMap.get("result_path")))) {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            if(sb.toString().length()==0){
                return "Job failed";
            }
            else{
                return sb.toString();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }


}
