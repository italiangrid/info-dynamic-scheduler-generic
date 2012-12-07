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

class BasicEstimator(DataCollector):

    logger = logging.getLogger("PersistentEstimators.BasicEstimator")

    def __init__(self, config, mjTable):
        DataCollector.__init__(self, config, mjTable)
        
        self.sampleNumber = 1000
        if "sample_number" in config:
            self.sampleNumber = int(config["sample_number"])
            
        self.storeDir = "/var/tmp/info-dynamic-scheduler-generic"
        if "sample_dir" in config:
            self.storeDir = config["sample_dir"]
            
        if not os.path.isdir(self.storeDir) or not os.access(self.storeDir, os.W_OK):
            raise Exception("Cannot find or access directory %s" % self.storeDir)
        
        self.buffer = dict()
        
    def register(self, evndict):
    
        qname = evndict['queue']
        
        #
        # TODO check for free slots in queue and exit
        #
        
        if not qname in self.buffer:
            self.buffer[qname] = list()
        
        if 'qtime' in evndict and 'start' in evndict:
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

                qFile = open(qFilename)
                for line in qFile:
                    item = line.strip().split(":")
                    if len(item) == 2 and item[0] < firstEventFound[0]:
                        tmpl.append(item)
                
                qFile.close()
                
                tmpl.append(self.buffer[qname])
                
                if len(tmpl) > self.sampleNumber:
                    del tmpl[0:len(tmpl)-self.sampleNumber]
                
                tmps = 0
                tmpm = -1
                for tmpt in tmpl:
                    tmps = tmps + tmpt[1]
                    tmpm = max(tmpm, tmpt[1])
                
                self.ert[qname] = int(tmps/len(tmpl))            
                self.wrt[qname] = tmpm
                
                qFile = open(qFilename, 'w')
                for tmpt in tmpl:
                    qFile.write('%d:%d\n' % tmpt)
                qFile.close()
                qFile = None

            except:
                BasicEstimator.logger.error("Error reading %s" % qFilename, exc_info=true)

            if qFile:
                try:
                    qFile.close()
                except:
                    BasicEstimator.logger.error("Cannot close %s" % qFilename, exc_info=true)


