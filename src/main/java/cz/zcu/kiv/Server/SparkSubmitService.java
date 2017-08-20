package cz.zcu.kiv.Server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.scheduling.annotation.Async;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
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
    private boolean finished = true;
    Process process;


    @Async
    public void submitJob(Map<String,String> queryMap, int id){
        logger.info("Submitting a job with query parameters = " + queryMap);
        this.queryMap = queryMap;

        // result path
        queryMap.put("result_path", System.getProperty("user.home") + "/spark_server/results/" + id + ".txt");
        if(queryMap.containsKey("save_name")){
            queryMap.put("save_name",System.getProperty("user.home") + "/spark_server/classifiers/" + queryMap.get("save_name"));
        }

        String jarName = "remote-server-2.0.jar";
        String decodedPath = SparkSubmitService.class.getProtectionDomain().getCodeSource().getLocation().getPath().split(jarName)[0];

        logger.info("Decoded path: " + decodedPath);

        String content = "spark-submit " +
                "--class cz.zcu.kiv.Main " +
                "--master local[*] " +
                "--conf spark.driver.host=localhost " +
                "--conf spark.executor.extraJavaOptions=-Dlogfile.name=" + System.getProperty("user.home") + "/spark_server/logs/" + id + " " +
                "--conf spark.driver.extraJavaOptions=-Dlogfile.name=" + System.getProperty("user.home") + "/spark_server/logs/" + id + " " +
                decodedPath+"spark_eeg-1.2-jar-with-dependencies.jar " +
                "\"" +
                queryMap.toString().replace("{","").replace("}","").replace(", ","&") +
                "\"";
        logger.info("Content of the script" + content);

        // write the job configuration into a file
        if(queryMap.containsKey("save_clf") && queryMap.get("save_clf").equals("true")){
            String text = hashMapToText(queryMap);
            PrintWriter out;
            try {
                out = new PrintWriter(System.getProperty("user.home") + "/spark_server/configurations/" + queryMap.get("save_name") + ".conf");
                out.println(text);
                out.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        String scriptLocation = System.getProperty("user.home") + "/spark_server/scripts/" + "spark_submit_script_jobId=" + id + ".sh";


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
    }

    @Async
    public String getResults(){
        logger.info("Reading results for job in location " + queryMap.get("result_path"));

        if(finished == false){
           return "Job cancelled";
        }
        try {
            BufferedReader br = new BufferedReader(new FileReader(queryMap.get("result_path")));
            try {
                StringBuilder sb = new StringBuilder();
                String line = br.readLine();

                while (line != null) {
                    sb.append(line);
                    sb.append("\n");
                    line = br.readLine();
                }
                if (sb.toString().length() == 0) {
                    return "Job failed";
                } else {
                    return sb.toString();
                }
            } finally {
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    @Async
    public String getLog(int id){
        List<String> lines = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.home") + "/spark_server/logs/" + id + ".log"));
            try {
                lines = new LinkedList<String>();
                for (String tmp; (tmp = br.readLine()) != null; )
                    if (lines.add(tmp) && lines.size() > 1000)
                        lines.remove(0);
            } finally {
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder(10000);
        for (String s : lines)
        {
            sb.append(s);
            sb.append("\n");
        }
        return sb.toString();
    }

    @Async
    public void cancelJob(){
        finished = false;
        process.destroy();
    }



    private String hashMapToText(Map<String,String> queryParams){
        StringBuilder params = new StringBuilder(500);
        for(String key : queryParams.keySet()){
            params.append(key);
            params.append(": ");
            params.append(queryParams.get(key));
            params.append("/////");
        }
        return params.toString();
    }

}
