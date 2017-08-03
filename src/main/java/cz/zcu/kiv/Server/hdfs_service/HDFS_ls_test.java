package cz.zcu.kiv.Server.hdfs_service;

import org.junit.Test;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

/***********************************************************************************************************************
 *
 * This file is part of the Spark_Back-end-cz.zcu.kiv.Server project

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
 * HDFS_ls_test, 2017/07/16 16:49 dbg
 *
 **********************************************************************************************************************/
public class HDFS_ls_test {
    @Test
    public void test() throws IOException {
        String content = "hadoop fs -ls -R / ";
        String scriptLocation = "src/main/resources/hdfs_list.sh";
        PrintWriter out = new PrintWriter(scriptLocation);
        out.print(content);
        out.close();


        File script = new File(scriptLocation);
        script.setExecutable(true);

        String[] command = { scriptLocation};
        Process process = Runtime.getRuntime().exec(command);
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream()));
        System.out.println("Started with script");
        String s;
        ArrayList<HadoopFile> data = new ArrayList<HadoopFile>();

        while ((s = reader.readLine()) != null) {
            String[] fileInfoString= s.split(" +");


            /*
            String[] fileInfo = new String[4];

            fileInfo[0] = fileInfoString[2];
            fileInfo[1] = fileInfoString[4];
            fileInfo[2] = fileInfoString[5];
            fileInfo[3] = fileInfoString[7];
            */

            /*
            System.out.println("FileOwner = " + fileInfoString[2]);
            System.out.println("FileSize = " + fileInfoString[4]);
            System.out.println("DateModified = " + fileInfoString[5]);
            System.out.println("FileName = " + fileInfoString[7]);
            */

//            data.add(fileInfo);
            //System.out.println(s); // Replace this line with the code to print the result to file

        }

        /**
         * save the data as json
         */
        ObjectMapper mapper = new ObjectMapper();

        String jsonInString = mapper.writeValueAsString(data);
        System.out.println(jsonInString);


        /**
         * parse the data back into array list of strings
         */
        ArrayList<String[]> someClassList =
                mapper.readValue(jsonInString, mapper.getTypeFactory().constructCollectionType(ArrayList.class, String[].class));

        //System.out.println();
        System.out.println(Arrays.toString(someClassList.get(0)));

    }

}
