package cz.zcu.kiv.Server.hdfs_service;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.IOException;
import java.io.Serializable;

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
 * HadoopFile, 2017/07/19 12:59 dbg
 *
 **********************************************************************************************************************/
@JsonIgnoreProperties(ignoreUnknown = true)
public class HadoopFile implements Serializable{

    private String path;
    private String fileName;
    private String owner;
    private String size;
    private String dateModified;
    private boolean isDirectory;


    public HadoopFile(){

    }

    public HadoopFile(String fileName, String path, String owner, String size, String dateModified, boolean isDirectory) throws IOException {
        this.fileName= fileName;
        this.path = path;
        this.owner= owner;
        this.size= Long.toString( (Long.parseLong(size)) / (1024 * 1024)) + " mb";
        this.dateModified= dateModified;
        this.isDirectory = isDirectory;
    }

    public String getFileName() { return fileName; }

    public String getPath() {
        return path;
    }

    public String getOwner() {
            return owner;
        }

    public String getSize() {
            return size;
        }

    public String getDateModified() {
            return dateModified;
        }

    public boolean isDirectory() {
        return isDirectory;
    }


}
