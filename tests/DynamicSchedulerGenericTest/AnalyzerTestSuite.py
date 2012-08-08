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

import unittest

from TestUtils import Workspace
from DynamicSchedulerGeneric import Utils as DynSchedUtils

from DynamicSchedulerGeneric import Analyzer

class AnalyzerTestCase(unittest.TestCase):

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

    def tearDown(self):
        pass

    def test_analyze_ok(self):
        try:
        
            jTable = [
                      ("atlasprod", "creamtest1", 'running', 1327564866, "creXX_23081970"),
                      ("atlasprod", 'creamtest2', 'queued', 1327565866, "creXX_23081971"),
                      ("dteamgold", 'creamtest2', 'running', 1327566866, "creXX_23081972"),
                      ("dteamgold", "creamtest1", 'running', 1327567866, "creXX_23081973"),
                      ("dteamgold", 'creamtest2', 'queued', 1327568866, "creXX_23081974"),
                      ("infngridlow", 'creamtest1', 'running', 1327569866, "creXX_23081975"),
                      ("infngridlow", 'creamtest2', 'running', 1327570866, "creXX_23081976"),
                      ("infngridhigh", 'creamtest1', 'running', 1327571866, "creXX_23081977"),
                      ("infngridhigh", 'creamtest2', 'running', 1327572866, "creXX_23081978"),
                      ("infngridhigh", 'creamtest1', 'queued', 1327573866, "creXX_23081979")
                     ]
            workspace = Workspace(vomap = self.vomap)
            
            script = self.headerfmt % (5, 0, 1327574866, 26)
            for jItem in jTable:
                script += self.dictfmt % jItem            
            script += self.footerfmt
            
            workspace.setLRMSCmd(script)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            collector = Analyzer.analyze(config, {})
            
            result =            collector.runningCREAMJobsForVO('atlas') == 1
            result = result and collector.queuedCREAMJobsForVO('atlas') == 1
            result = result and collector.runningCREAMJobsForVO('dteam') == 2
            result = result and collector.queuedCREAMJobsForVO('dteam') == 1
            result = result and collector.runningCREAMJobsForVO('infngrid') == 4
            result = result and collector.queuedCREAMJobsForVO('infngrid') == 1
            
            self.assertTrue(result)
            
        except Exception, test_error:
            self.fail(str(test_error))


    def test_analyze_err_from_script(self):
        try:
            workspace = Workspace(vomap = self.vomap)
            script = """#!/usr/bin/python
import sys
sys.stderr.write("Dummy error message")
sys.exit(1)
"""
            workspace.setLRMSCmd(script)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            collector = Analyzer.analyze(config, {})
        
            self.fail("Exception not handled")
            
        except Analyzer.AnalyzeException, test_error:
            msg = str(test_error)
            self.assertTrue(msg.startswith("Dummy error message"))
        except Exception, test_error:
            self.fail(repr(test_error))


    def test_analyze_cream_and_es(self):
        try:
            jTable = [
                      ("atlasprod", "creamtest1", 'running', 1327564866, "creXX_23081970"),
                      ("atlasprod", 'creamtest2', 'queued', 1327565866, "creXX_23081971"),
                      ("atlasprod", "creamtest1", 'running', 1327564866, "esXX_23081970"),
                      ("atlasprod", 'creamtest2', 'queued', 1327565866, "esXX_23081971"),
                      ("atlasprod", 'creamtest2', 'queued', 1327565866, "esXX_23081972")
                     ]
            workspace = Workspace(vomap = self.vomap, enableES = 'true')
            
            script = self.headerfmt % (5, 0, 1327574866, 26)
            for jItem in jTable:
                script += self.dictfmt % jItem            
            script += self.footerfmt
            
            workspace.setLRMSCmd(script)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            collector = Analyzer.analyze(config, {})
            
            result =            collector.runningCREAMJobsForVO('atlas') == 1
            result = result and collector.queuedCREAMJobsForVO('atlas') == 1
            result = result and collector.runningESJobsForVO('atlas') == 1
            result = result and collector.queuedESJobsForVO('atlas') == 2
            self.assertTrue(result)
            
        except Exception, test_error:
            self.fail(str(test_error))
            

if __name__ == '__main__':
    unittest.main()

