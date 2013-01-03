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
import sys

from DynamicSchedulerGeneric import Utils as DynSchedUtils

from TestUtils import Workspace

class UtilsTestCase(unittest.TestCase):

    def setUp(self):
        self.vomap = {"atlasprod": "atlas",
                      "atlassgm": "atlas",
                      "dteamgold": "dteam",
                      "dteamsilver": "dteam",
                      "dteambronze": "dteam",
                      "infngridlow": "infngrid",
                      "infngridmedium": "infngrid",
                      "infngridhigh": "infngrid"}
        self.mjTable = {"atlasprod": 20,
                        "atlassgm": 30,
                        "dteamgold": 50,
                        "dteamsilver": 40,
                        "dteambronze": 60,
                        "infngridlow": 110,
                        "infngridmedium": 120,
                        "infngridhigh": 130}
        
    def tearDown(self):
        pass

    def test_getMaxJobsTable_ok(self):

            workspace = Workspace(vomap = self.vomap)
            workspace.setMaxJobCmd(self.mjTable)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            result = DynSchedUtils.getMaxJobsTable(config)
            self.assertTrue('atlas' in result and result['atlas'] == 50
                            and 'dteam' in result and result['dteam'] == 150
                            and 'infngrid' in result and result['infngrid'] == 360)
                


    def test_getMaxJobsTable_wrongexit(self):
        try:
            workspace = Workspace(vomap = self.vomap)
            script = """#!/bin/bash
exit 1
"""
            workspace.setMaxJobCmd(script)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            result = DynSchedUtils.getMaxJobsTable(config)
        
        except DynSchedUtils.UtilsException, test_error:
            msg = str(test_error)
            self.assertTrue(msg.startswith("VO max jobs backend command returned"))
        

    def test_getMaxJobsTable_nofile(self):
        try:
            workspace = Workspace(vomap = self.vomap)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            result = DynSchedUtils.getMaxJobsTable(config)
        
        except DynSchedUtils.UtilsException, test_error:
            msg = str(test_error)
            self.assertTrue(msg.startswith("Error running"))
       
if __name__ == '__main__':
    unittest.main()

