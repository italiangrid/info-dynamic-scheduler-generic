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

import sys
import unittest

from TestUtils import Workspace
from DynamicSchedulerGeneric import Utils as DynSchedUtils
from DynamicSchedulerGeneric import Analyzer
from DynamicSchedulerGeneric import GLUE2Handler

class DummyOutput:

    def __init__(self):
        self.queued = {}
        self.running = {}
        self.curr_id = None
        
        
    def write(self, line):
        if line.startswith("dn: GLUE2ShareID"):
            self.curr_id = line[4:-1]
        if line.startswith("GLUE2ComputingShareWaitingJobs"):
            self.queued[self.curr_id] = int(line[32:-1])
        if line.startswith("GLUE2ComputingShareRunningJobs"):
            self.running[self.curr_id] = int(line[32:-1])    


class GLUE1TestCase(unittest.TestCase):

    def setUp(self):
        self.vomap = {"atlasprod": "atlas",
                      "atlassgm": "atlas",
                      "dteamgold": "dteam",
                      "dteamsilver": "dteam",
                      "dteambronze": "dteam",
                      "infngridlow": "infngrid",
                      "infngridmedium": "infngrid",
                      "infngridhigh": "infngrid"}
                      
        self.headerfmt = "#!/bin/bash\n\n"
        self.headerfmt += "printf 'nactive      %d\n"
        self.headerfmt += "nfree        %d\n"
        self.headerfmt += "now          %d\n"
        self.headerfmt += "schedCycle   %d\n\n"
        
        self.footerfmt = "'\n\nexit 0"
        
        self.dictfmt = '{"group": "%s", "queue": "%s", "state": "%s", "qtime": %d, "name": "%s"}\n'
    
    def _script(self):
        jTable = [
                  ("dteamgold", 'creamtest2', 'running', 1327566866, "creXX_23081970"),
                  ("dteamgold", 'creamtest2', 'queued', 1327568866, "creXX_23081971"),
                  ("dteambronze", "creamtest2", 'queued', 1327571866, "creXX_23081972"),
                  ("dteamgold", "creamtest1", 'running', 1327567866, "creXX_23081973"),
                  ("dteamsilver", "creamtest1", 'running', 1327569866, "creXX_23081974"),
                  ("dteambronze", "creamtest1", 'queued', 1327570866, "creXX_23081975"),
                  ("infngridlow", "creamtest1", 'queued', 1327572866, "creXX_23081976"),
                  ("infngridmedium", "creamtest1", 'running', 1327573866, "creXX_23081977")
                 ]
        script = self.headerfmt % (5, 0, 1327574866, 26)
        for jItem in jTable:
            script += self.dictfmt % jItem            
        script += self.footerfmt
        return script
        
    def test_process_ok(self):

            glue2shareid = 'GLUE2ShareID=creamtest1_dteam_abc,GLUE2ServiceID=abc,GLUE2GroupID=resource,o=glue'
            ldif = """
dn: GLUE2PolicyID=creamtest1_dteam_abc_policy,%s
GLUE2PolicyUserDomainForeignKey: dteam
GLUE2MappingPolicyShareForeignKey: creamtest1_dteam_abc

dn: %s
GLUE2ShareID: creamtest1_dteam_abc
GLUE2ComputingShareMappingQueue: creamtest1
""" % (glue2shareid, glue2shareid)
            
            workspace = Workspace(vomap = self.vomap)
            workspace.setLRMSCmd(self._script())
            workspace.setGLUE2StaticFile(ldif)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            dOut = DummyOutput()
            collector = Analyzer.analyze(config, {})
            GLUE2Handler.process(config, collector, dOut)
            
            result = dOut.queued[glue2shareid] == 1
            result = result and dOut.running[glue2shareid] == 2
            self.assertTrue(result)
            

    def test_process_missing_share(self):
        try:
            ldif = """
dn: GLUE2PolicyID=creamtest1_dteam_abc_policy,GLUE2ShareId=creamtest1_dteam_abc,GLUE2ServiceID=abc,GLUE2GroupID=resource,o=glue
GLUE2PolicyUserDomainForeignKey: dteam
GLUE2MappingPolicyShareForeignKey: creamtest1_dteam_abc

"""
            workspace = Workspace(vomap = self.vomap)
            workspace.setLRMSCmd(self._script())
            workspace.setGLUE2StaticFile(ldif)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            collector = Analyzer.analyze(config, {})
            GLUE2Handler.process(config, collector, DummyOutput())            
            self.fail("No exception detected")

        except GLUE2Handler.GLUE2Exception, glue_error:
            msg = str(glue_error)
            self.assertTrue(msg.startswith("Invalid foreign key"))      


    def test_process_missing_vo_in_policy(self):
        try:
            glue2shareid = 'GLUE2ShareID=creamtest1_dteam_abc,GLUE2ServiceID=abc,GLUE2GroupID=resource,o=glue'
            ldif = """
dn: %s
GLUE2ShareID: creamtest1_dteam_abc
GLUE2ComputingShareMappingQueue: creamtest1

dn: GLUE2PolicyID=creamtest1_dteam_abc_policy,%s
GLUE2MappingPolicyShareForeignKey: creamtest1_dteam_abc

""" % (glue2shareid, glue2shareid)

            workspace = Workspace(vomap = self.vomap)
            workspace.setLRMSCmd(self._script())
            workspace.setGLUE2StaticFile(ldif)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            collector = Analyzer.analyze(config, {})
            GLUE2Handler.process(config, collector, DummyOutput())            
            self.fail("No exception detected")

        except GLUE2Handler.GLUE2Exception, glue_error:
            msg = str(glue_error)
            self.assertTrue(msg == "Missing mandatory attribute GLUE2PolicyUserDomainForeignKey")      


if __name__ == '__main__':
    unittest.main()


