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
import logging

from DynamicSchedulerGeneric import PersistentEstimators


class BasicEstimatorWrapper(PersistentEstimators.BasicEstimator):

    def __init__(self, keepfiles=False):
        self.sampleNumber = 1000
        self.storeDir = "/tmp/dynschedtest"
        if os.path.exists(self.storeDir) and not keepfiles:
            shutil.rmtree(self.storeDir)
        if not os.path.exists(self.storeDir):
            os.mkdir(self.storeDir)
        
        self.now = 1000
            
        self.buffer = dict()
        self.nqueued = dict()
        self.nrun = dict()
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

        estimator = BasicEstimatorWrapper()
            
        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_1', 'start' : 500})
        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_2', 'start' : 200})
        estimator.register({'queue' : 'dteam', 'state' : 'queued', 'jobid': 'crea_3',})
        estimator.register({'queue' : 'dteam', 'state' : 'queued', 'jobid': 'crea_4',})
            
        estimator.estimate()
            
        self.assertTrue(estimator.localERT['dteam'] == 1300)
                        
            
    def test_BasicEstimator_empty(self):

        estimator = BasicEstimatorWrapper()
            
        estimator.estimate()
            
        self.assertTrue(len(estimator.localERT) == 0)


    def test_BasicEstimator_multi_estimate(self):
        estimator = BasicEstimatorWrapper()
        estimator.now = 2000
            
        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_1', 'start' : 500})
        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_2', 'start' : 200})
        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_3', 'start' : 1200})
        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_4', 'start' : 1700})
        estimator.register({'queue' : 'dteam', 'state' : 'queued', 'jobid': 'crea_5',})
        estimator.register({'queue' : 'dteam', 'state' : 'queued', 'jobid': 'crea_6',})
            
        estimator.estimate()

        estimator = BasicEstimatorWrapper(True)
        estimator.now = 3000

        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_3', 'start' : 1200})
        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_4', 'start' : 1700})
        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_5', 'start' : 2200})
        estimator.register({'queue' : 'dteam', 'state' : 'running', 'jobid': 'crea_6', 'start' : 2400})
        estimator.register({'queue' : 'dteam', 'state' : 'queued', 'jobid': 'crea_7',})
        estimator.register({'queue' : 'dteam', 'state' : 'queued', 'jobid': 'crea_8',})
        estimator.register({'queue' : 'dteam', 'state' : 'queued', 'jobid': 'crea_9',})
        
        estimator.estimate()
        
        self.assertTrue(estimator.localERT['dteam'] == 2600)


if __name__ == '__main__':
    if os.path.exists('logging.conf'):
        import logging.config
        logging.config.fileConfig('logging.conf')
    else:
        logging.basicConfig()
    unittest.main()

