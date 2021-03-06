# Copyright (c) Members of the EGEE Collaboration. 2004. 
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.  
#
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
#
#     http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.

import os, os.path, stat
import shutil
import types
import ConfigParser

class Workspace:

    def __init__(self, **args):
    
        if "workspacedir" in args:
            self.workspace = args["workspacedir"]
        else:
            self.workspace = "/tmp/dynschedtest"
        self.sampleDir = self.workspace + '/sampledir'
        
        if os.path.exists(self.workspace):
            shutil.rmtree(self.workspace)
        os.mkdir(self.workspace)
        os.mkdir(self.sampleDir)         
        
        self.configFilename = self.workspace + "/scheduler.conf"
        
        if "cfgsyntaxerr" in args and args["cfgsyntaxerr"] == "true":
            pass
        else:
            #
            # Workspace creates only the main config file
            # any reference to ldif-files and scripts are pending
            # so it is possible to simulate the filenotfound
            #
            config = ConfigParser.ConfigParser()
            config.add_section("Main")
            config.add_section("Scheduler")
            config.add_section("LRMS")
            
            if "outputformat" in args:
                config.set("Main", "outputformat", args["outputformat"])
            else:
                config.set("Main", "outputformat", "both")
            
            config.set("Main", "bdii-configfile", self.workspace + "/bdii.conf")
            
            if not "noglue1static" in args or args["noglue1static"] <> "true":
                config.set("Main", "static_ldif_file", self.workspace +"/static-file-CE.ldif")
            if not "noglue2static" in args or args["noglue2static"] <> "true":
                config.set("Main", "static_glue2_ldif_file_computingshare", 
                           self.workspace +"/ComputingShare.ldif")
            
            config.set("Main", "cream_prefix", "creXX_")
            if "enableES" in args and args["enableES"] == "true":
                config.set("Main", "es_prefix", "esXX_")
            config.set("Main", "sample_dir", self.sampleDir)
            
            if not "nomaxjobcmd" in args or args["nomaxjobcmd"] <> "true":
                config.set("Scheduler", "vo_max_jobs_cmd", self.workspace +"/maxjobs-mock")
            if not "nolrmscmd" in args or args["nolrmscmd"] <> "true":
                config.set("LRMS", "lrms_backend_cmd", self.workspace +"/lrmsinfo-mock")
            
            if "vomap" in args:
                vomap = args["vomap"]
                buff = ""
                for key in vomap:
                    buff = buff + ("%s:%s\n" % (key, vomap[key]))
                config.set("Main", "vomap", buff)
                
            cfgFile = open(self.configFilename, "w")
            config.write(cfgFile)
            cfgFile.close
        
        bdiiConffile = open(self.workspace + "/bdii.conf", 'w')
        bdiiConffile.write("BDII_LDIF_DIR=%s\n" % self.workspace)
        bdiiConffile.close()
        
    def getConfigurationFile(self):
        return self.configFilename
    
    def setMaxJobCmd(self, table):
    
        if type(table) is types.DictType:
            script = """#!/bin/bash

printf 'mydict = {
"""
            for key in table:
                script = script + ('"%s": %s,\n' % (key, table[key]))
            script = script + "}'"
        else:
            script = str(table)
        
        mjcmdFile = open(self.workspace +"/maxjobs-mock", "w")
        mjcmdFile.write(script)
        mjcmdFile.close
        
        os.chmod(self.workspace +"/maxjobs-mock",
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    
    def setLRMSCmd(self, lrms_script):
        lrmscmdFile = open(self.workspace +"/lrmsinfo-mock", "w")
        lrmscmdFile.write(lrms_script)
        lrmscmdFile.close
        
        os.chmod(self.workspace +"/lrmsinfo-mock",
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    
    def setGLUE1StaticFile(self, ldif):
        ldifFile = open(self.workspace +"/static-file-CE.ldif", "w")
        ldifFile.write(ldif)
        ldifFile.close()
    
    def setGLUE2StaticFile(self, ldif):
        ldifFile = open(self.workspace +"/ComputingShare.ldif", "w")
        ldifFile.write(ldif)
        ldifFile.close()
    

if __name__ == '__main__':
    vomap = {'dteamprod': 'dteam',
             'dteamsgm': 'dteam',
             'cmssgm': 'cms',
             'cmsprd': 'cms',
             'cmssilver': 'cms',
             'cmsgold': 'cms',
             'cmsbronze': 'cms',
             'infngridsgm': 'infngrid',
             'alicesgm': 'alice',
             'aliceprd': 'alice',
             'sgmtesters': 'testers.eu-emi.eu',
             'piltesters': 'testers.eu-emi.eu',
             'sgmcreamtest': 'creamtest'}
    
    workspace = Workspace(vomap = vomap,
                          nolrmscmd = 'true')
    
    mydict = {"sgmcreamtest": 30,
              "cmssgm": 25,
              "cmsprd": 40,
              "cmssilver": 20,
              "cmsgold": 30,
              "cmsbronze": 30,
              "dteamsgm": 40,
              "dteamprd": 40,
              "infngridsgm": 100,
              "alicesgm": 120,
              "aliceprd": 120,
              "sgmtesters": 200,
              "piltesters": 200}
    
    workspace.setMaxJobCmd(mydict)

