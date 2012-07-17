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
from DynamicSchedulerGeneric import GLUE1Handler


class DummyOutput:

    def __init__(self):
        self.queued = {}
        self.running = {}
        self.curr_id = None
        
        
    def write(self, line):
        if line.startswith("dn: GlueCEUniqueID") or \
           line.startswith("dn: GlueVOViewLocalID"):
            self.curr_id = line[4:-1]
        if line.startswith("GlueCEStateWaitingJobs"):
            self.queued[self.curr_id] = int(line[24:-1])
        if line.startswith("GlueCEStateRunningJobs"):
            self.running[self.curr_id] = int(line[24:-1])    

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
        
        self.dictfmt = '{"group": "%s", "queue": "%s", "state": "%s", "qtime": %d}\n'
        

    def _script(self):
        jTable = [
                  ("dteamgold", 'creamtest2', 'running', 1327566866),
                  ("dteamgold", 'creamtest2', 'queued', 1327568866),
                  ("dteambronze", "creamtest2", 'queued', 1327571866),
                  ("dteamgold", "creamtest1", 'running', 1327567866),
                  ("dteamsilver", "creamtest1", 'running', 1327569866),
                  ("dteambronze", "creamtest1", 'queued', 1327570866),
                  ("infngridlow", "creamtest1", 'queued', 1327572866),
                  ("infngridmedium", "creamtest1", 'running', 1327573866)
                 ]
        script = self.headerfmt % (5, 0, 1327574866, 26)
        for jItem in jTable:
            script += self.dictfmt % jItem            
        script += self.footerfmt
        return script


    def test_process_ok(self):
        try:
            glueceuniqueid = 'GlueCEUniqueID=cream-38.pd.infn.it:8443/cream-pbs-creamtest1,mds-vo-name=resource,o=grid'
            gluevoviewid = 'GlueVOViewLocalID=dteam,' + glueceuniqueid
            ldif = """
dn: %s
GlueVOViewLocalID: dteam
GlueChunkKey: GlueCEUniqueID=cream-38.pd.infn.it:8443/cream-pbs-creamtest1
GlueCEAccessControlBaseRule: VO:dteam

dn: %s
GlueCEUniqueID: cream-38.pd.infn.it:8443/cream-pbs-creamtest1
GlueCEName: creamtest1
GlueCEAccessControlBaseRule: VO:infngrid
GlueCEAccessControlBaseRule: VO:dteam
""" % (gluevoviewid, glueceuniqueid)

            workspace = Workspace(vomap = self.vomap)            
            workspace.setLRMSCmd(self._script())
            workspace.setGLUE1StaticFile(ldif)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            dOut = DummyOutput()
            collector = Analyzer.analyze(config, {})
            GLUE1Handler.process(config, collector, dOut)
            
            result = dOut.queued[glueceuniqueid] == 2
            result = result and dOut.running[glueceuniqueid] == 3
            result = result and dOut.queued[gluevoviewid] == 1
            result = result and dOut.running[gluevoviewid] == 2
            self.assertTrue(result)
            
        except Exception, test_error:
            etype, value, traceback = sys.exc_info()
            sys.excepthook(etype, value, traceback)
            self.fail(repr(test_error))
            



    def test_process_missingce(self):
        try:
            glueceuniqueid = 'GlueCEUniqueID=cream-38.pd.infn.it:8443/cream-pbs-creamtest1,mds-vo-name=resource,o=grid'
            gluevoviewid = 'GlueVOViewLocalID=dteam,' + glueceuniqueid
            ldif = """
dn: %s
GlueVOViewLocalID: dteam
GlueChunkKey: GlueCEUniqueID=cream-38.pd.infn.it:8443/cream-pbs-creamtest1
GlueCEAccessControlBaseRule: VO:dteam
""" % gluevoviewid
            
            workspace = Workspace(vomap = self.vomap)
            workspace.setLRMSCmd(self._script())
            workspace.setGLUE1StaticFile(ldif)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            dOut = DummyOutput()
            collector = Analyzer.analyze(config, {})
            GLUE1Handler.process(config, collector, dOut)            
            self.fail("No exception detected")
        
        except GLUE1Handler.GLUE1Exception, glue_error:
            msg = str(glue_error)
            self.assertTrue(msg.startswith("Invalid foreign key"))      
        except Exception, test_error:
            etype, value, traceback = sys.exc_info()
            sys.excepthook(etype, value, traceback)
            self.fail(repr(test_error))
        

    
if __name__ == '__main__':
    unittest.main()


