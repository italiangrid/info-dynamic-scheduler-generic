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
        if os.path.exists(self.workspace):
            shutil.rmtree(self.workspace)
        os.mkdir(self.workspace)            
        
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
                
            if not "noglue1static" in args or args["noglue1static"] <> "true":
                config.set("Main", "static_ldif_file", self.workspace +"/static-file-CE.ldif")
            if not "noglue2static" in args or args["noglue2static"] <> "true":
                config.set("Main", "static_glue2_ldif_file_computingshare", 
                           self.workspace +"/ComputingShare.ldif")
        
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
    
    def setGLUE1StaticFile(self, ldif):
        pass
    
    def setGLUE2StaticFile(self, ldif):
        pass
    

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

