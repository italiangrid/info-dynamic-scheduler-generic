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
import os, os.path
import shutil

from DynamicSchedulerGeneric import PersistentEstimators


class BasicEstimatorWrapper(PersistentEstimators.BasicEstimator):

    def __init__(self):
        self.sampleNumber = 1000
        self.storeDir = "/tmp/dynschedtest"
        if os.path.exists(self.storeDir):
            shutil.rmtree(self.storeDir)
        os.mkdir(self.storeDir)
        
        self.buffer = dict()
        self.localERT = dict()
        self.localWRT = dict()

    def setERT(self, qName, value):
        self.localERT[qName] = value
    
    def setWRT(self, qName, value):
        self.localWRT[qName] = value

class EstimatorsTestCase(unittest.TestCase):

    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def test_BasicEstimator_ok(self):
        try:
            estimator = BasicEstimatorWrapper()
            
            estimator.register({'queue' : 'dteam', 'qtime' : 10, 'start' : 25})
            estimator.register({'queue' : 'dteam', 'qtime' : 40, 'start' : 75})
            estimator.register({'queue' : 'dteam', 'qtime' : 110, 'start' : 130})
            estimator.register({'queue' : 'dteam', 'qtime' : 210, 'start' : 225})
            
            estimator.estimate()
            
            self.assertTrue(estimator.localERT['dteam'] == 21)
            
        except Exception, test_error:
            etype, evalue, etraceback = sys.exc_info()
            sys.excepthook(etype, evalue, etraceback)
            self.fail(str(test_error))

    def test_BasicEstimator_empty(self):
        try:
            estimator = BasicEstimatorWrapper()
            
            estimator.estimate()
            
            self.assertTrue(len(estimator.localERT) == 0)
            
        except Exception, test_error:
            self.fail(str(test_error))

    def test_BasicEstimator_missing_time(self):
        try:
            estimator = BasicEstimatorWrapper()
            
            estimator.register({'queue' : 'dteam', 'qtime' : 10, 'start' : 25})
            estimator.register({'queue' : 'dteam', 'start' : 130})
            estimator.register({'queue' : 'dteam', 'qtime' : 210})
            estimator.register({'queue' : 'dteam'})
            
            estimator.estimate()
            
            self.assertTrue(estimator.localERT['dteam'] == 15)
            
        except Exception, test_error:
            self.fail(str(test_error))


    def test_BasicEstimator_empty_for_missing_time(self):
        try:
            estimator = BasicEstimatorWrapper()
            
            estimator.register({'queue' : 'dteam', 'start' : 130})
            estimator.register({'queue' : 'dteam', 'qtime' : 210})
            estimator.register({'queue' : 'dteam'})
            
            estimator.estimate()
            
            self.assertTrue(len(estimator.localERT) == 0)
            
        except Exception, test_error:
            self.fail(str(test_error))


if __name__ == '__main__':
    unittest.main()

