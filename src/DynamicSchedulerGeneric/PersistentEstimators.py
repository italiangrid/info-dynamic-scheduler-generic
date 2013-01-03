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
import os, os.path
import logging

from DynamicSchedulerGeneric.Analyzer import DataCollector

class BasicEstimator(DataCollector):

    logger = logging.getLogger("PersistentEstimators.BasicEstimator")
    
    DEFAULT_STORE_DIR = "/var/tmp/info-dynamic-scheduler-generic"
    DEFAULT_SAMPLE_NUM = 5000

    def __init__(self, config, mjTable):
        DataCollector.__init__(self, config, mjTable)
        
        if config.has_option('Main', 'sample_number'):
            self.sampleNumber = int(config.get('Main', 'sample_number'))
        else:
            self.sampleNumber = BasicEstimator.DEFAULT_SAMPLE_NUM
            
        if config.has_option('Main', 'sample_dir'):
            self.storeDir = config.get('Main', 'sample_dir')
        else:
            self.storeDir = BasicEstimator.DEFAULT_STORE_DIR
            
        if not os.path.isdir(self.storeDir) or not os.access(self.storeDir, os.W_OK):
            raise Exception("Cannot find or access directory %s" % self.storeDir)
        
        self.buffer = dict()
        
    def register(self, evndict):
    
        qname = evndict['queue']
        
        #
        # TODO check for free slots in queue and exit
        #
        
        if 'qtime' in evndict and 'start' in evndict:
        
            if not qname in self.buffer:
                self.buffer[qname] = list()
        
            self.buffer[qname].append((evndict['qtime'], evndict['start'] - evndict['qtime']))
        
    def estimate(self):
        
        #
        # TODO check for free slots in queue and exit
        #

        for qname in self.buffer:
        
            self.buffer[qname].sort()
            firstEventFound = self.buffer[qname][0]
            
            tmpl = list()
            qFilename = self.storeDir + '/' + qname
            qFile = None
        
            try:

                if os.path.exists(qFilename):
                    qFile = open(qFilename)
                    for line in qFile:
                        item = line.strip().split(":")
                        if len(item) == 2 and item[0] < firstEventFound[0]:
                            tmpl.append(item)
                
                    qFile.close()
                    qFile = None
                
                tmpl = tmpl + self.buffer[qname]
                if len(tmpl) > self.sampleNumber:
                    del tmpl[0:len(tmpl)-self.sampleNumber]
                
                tmpSum = 0
                tmpMax = -1
                for tmpt in tmpl:
                    tmpSum = tmpSum + tmpt[1]
                    tmpMax = max(tmpMax, tmpt[1])
                
                self.setERT(qname, int(tmpSum/len(tmpl)))           
                self.setWRT(qname, tmpMax)
                
                qFile = open(qFilename, 'w')
                for tmpt in tmpl:
                    qFile.write('%d:%d\n' % tmpt)
                qFile.close()
                qFile = None

            except:
                etype, evalue, etraceback = sys.exc_info()
                sys.excepthook(etype, evalue, etraceback)
                #BasicEstimator.logger.error("Error reading %s" % qFilename, exc_info=True)

            if qFile:
                try:
                    qFile.close()
                except:
                    BasicEstimator.logger.error("Cannot close %s" % qFilename, exc_info=True)


def getEstimatorList():
    return [ BasicEstimator ]


