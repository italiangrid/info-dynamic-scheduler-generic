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

from DynamicSchedulerGeneric import Utils as DynSchedUtils
from TestUtils import Workspace

class UtilsTestCase(unittest.TestCase):

    def setUp(self):
        pass
        
    def tearDown(self):
        pass

    def test_getMaxJobsTable_ok(self):
        try:
            vomap = {"atlasprod": "atlas",
                     "atlassgm": "atlas",
                     "dteamgold": "dteam",
                     "dteamsilver": "dteam",
                     "dteambronze": "dteam",
                     "infngridlow": "infngrid",
                     "infngridmedium": "infngrid",
                     "infngridhigh": "infngrid"}
                     
            mjTable = {"atlasprod": 20,
                       "atlassgm": 30,
                       "dteamgold": 50,
                       "dteamsilver": 40,
                       "dteambronze": 60,
                       "infngridlow": 110,
                       "infngridmedium": 120,
                       "infngridhigh": 130}
            
            workspace = Workspace(vomap = vomap)
            workspace.setMaxJobCmd(mjTable)
            
            cfgfile = workspace.getConfigurationFile()
            config = DynSchedUtils.readConfigurationFromFile(cfgfile)
            
            result = DynSchedUtils.getMaxJobsTable(config)
            self.assertTrue('atlas' in result and result['atlas'] == 50
                            and 'dteam' in result and result['dteam'] == 150
                            and 'infngrid' in result and result['infngrid'] == 360)
                
        except Exception, test_error:
            self.fail(repr(test_error))
        
if __name__ == '__main__':
    unittest.main()

