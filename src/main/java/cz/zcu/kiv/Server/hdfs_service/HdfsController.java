package cz.zcu.kiv.Server.hdfs_service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
 * HdfsController, 2017/07/19 09:51 dbg
 *
 **********************************************************************************************************************/
@RestController
public class HdfsController {

    private static Log logger = LogFactory.getLog(HdfsController.class);

    @RequestMapping(method = RequestMethod.GET,value = "/hdfs/{path}")
    public List<HadoopFile> getFilesInPath(@PathVariable String path) throws IOException {

        String content = "hdfs dfs -ls -R " + path.replace(",","/");
        String scriptLocation = System.getProperty("user.home") + "/spark_server/scripts/hdfs_list.sh";
        PrintWriter out = null;
        out = new PrintWriter(scriptLocation);
        out.print(content);
        out.close();

        File script = new File(scriptLocation);
        script.setExecutable(true);

        String[] command = { scriptLocation};
        Process process = null;
        process = Runtime.getRuntime().exec(command);
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream()));

        logger.info("Started with script: " + content);
        String s;
        List<HadoopFile> data = new ArrayList<HadoopFile>();

        while ((s = reader.readLine()) != null) {
            String[] fileInfoString= s.split(" +");

            System.out.println(Arrays.toString(fileInfoString));

            if(fileInfoString.length>1){
                data.add(new HadoopFile(
                        fileInfoString[7].substring(fileInfoString[7].lastIndexOf("/")+1,fileInfoString[7].length()), //file name
                        fileInfoString[7],   // file path
                        fileInfoString[2],   // file owner
                        fileInfoString[4],   // file size
                        fileInfoString[5],   // date modified
                        fileInfoString[1].equals("-")));    // is directory
            }

            System.out.println(Arrays.toString(fileInfoString)); // Replace this line with the code to print the result to file
        }

        logger.info("Done listing files");

        //save the data as json

        /*
        ObjectMapper mapper = new ObjectMapper();
        //mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        String jsonInString = mapper.writeValueAsString(data);
        //System.out.println(jsonInString);


        // parse the data back into array list of strings

        List<HadoopFile> someClassList =
                mapper.readValue(jsonInString, mapper.getTypeFactory().constructCollectionType(ArrayList.class, HadoopFile.class));

        System.out.println("First file name");
        System.out.println(someClassList.get(0).getFileName());

        */
        return data;
    }
}
