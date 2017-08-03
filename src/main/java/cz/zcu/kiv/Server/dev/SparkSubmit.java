package cz.zcu.kiv.Server.dev;


import org.junit.Test;

import java.io.*;

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
 * SparkSubmit, 2017/07/12 15:11 Dorian Beganovic
 *
 **********************************************************************************************************************/
public class SparkSubmit {
    @Test
    public void test() throws IOException {
        String content = "spark-submit " +
                "--class cz.zcu.kiv.Main " +
                "--master local[*] " +
                "/Users/dorianbeganovic/gsoc/Spark_EEG_Analysis/target/spark_eeg-1.2-jar-with-dependencies.jar " +
                "\"" +
                "info_file=/user/digitalAssistanceSystem/data/numbers/infoTrain.txt" +
                "&fe=dwt-8" +
                "&train_clf=svm"+
                "&config_step_size=1.0"+
                "&config_num_iterations=10000"+
                "&config_reg_param=0.01"+
                "&config_mini_batch_fraction=1.0" +
                "\"";

        // this should be a folder somewhere on the server
        String scriptLocation = System.getProperty("user.home") + "/spark_server/scripts/" + "spark_submit_script.sh";
        PrintWriter out = new PrintWriter(scriptLocation);
        out.print(content);
        out.close();

        File script = new File(scriptLocation);
        script.setExecutable(true);

        String[] command = { scriptLocation};
        Process process = Runtime.getRuntime().exec(command);
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream()));
        System.out.println("started with script");
        String s;
        while ((s = reader.readLine()) != null) {
            System.out.println("Script output: " + s); // Replace this line with the code to print the result to file
        }
    }
}
