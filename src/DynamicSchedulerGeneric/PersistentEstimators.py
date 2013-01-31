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
import time
import math

from DynamicSchedulerGeneric.Analyzer import DataCollector
from DynamicSchedulerGeneric.Analyzer import AnalyzeException

class BasicEstimator(DataCollector):

    logger = logging.getLogger("PersistentEstimators.BasicEstimator")
    
    DEFAULT_STORE_DIR = "/var/tmp/info-dynamic-scheduler-generic"
    DEFAULT_SAMPLE_NUM = 1000

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
            raise AnalyzeException("Cannot find or access directory %s" % self.storeDir)
        
        self.buffer = dict()
        self.now = int(time.time())
        self.nqueued = dict()
        self.nrun = dict()
        
    def register(self, evndict):
    
        qname = evndict['queue']
        
        if not qname in self.nqueued:
            self.nqueued[qname] = 0
        if not qname in self.nrun:
            self.nrun[qname] = 0
        
        if evndict['state'] == 'queued':
            self.nqueued[qname] += 1
            
        if evndict['state'] == 'running':
            self.nrun[qname] += 1
        
            if 'start' in evndict:
        
                if not qname in self.buffer:
                    self.buffer[qname] = list()
        
                BasicEstimator.logger.debug('Updating service time for ' + str(evndict))
                record = (evndict['start'], evndict['jobid'], self.now - evndict['start'])
                self.buffer[qname].append(record)


        
    # Given:
    # N      number of queued jobs
    # R      number of running jobs
    # K      number of slots
    # Savg   average service time
    # Smax   max service time
    #
    # for each iteration we have:
    #
    #       / 0                          R < K
    # ERT = | 
    #       \ ceil((N / K) + 1) * Savg   R = K
    #
    #       / 0                          R < K
    # WRT = | 
    #       \ ceil((N / K) + 1) * Smax   R = K
    #
    def estimate(self):
        
        for qname in self.buffer:
        
            if len(self.buffer[qname]) == 0:
                BasicEstimator.logger.debug('No events for %s' % qname)
                continue
            
            if self.nqueued[qname] > 0:
                nslots = self.nrun[qname]
            else:
                #undef K
                nslots = -1
            
            self.buffer[qname].sort()
            buffIdx = 0

            tmpl = list()
            qFilename = os.path.join(self.storeDir, qname)
            qFile = None
            
            try:

                if os.path.exists(qFilename):
                    qFile = open(qFilename)
                    for line in qFile:
                        tmpt = line.strip().split()
                        
                        if len(tmpt) < 2:
                            continue
                            
                        if tmpt[0] == "#nslot" and nslots < 0:
                            nslots = int(tmpt[1])
                            continue
                        
                        if len(tmpt) < 3:
                            continue

                        tmpstt = int(tmpt[0])
                        tmpjid = tmpt[1]
                        tmpsrvt = int(tmpt[2])
                        tmpt = (tmpstt, tmpjid, tmpsrvt)

                        crsr = self.buffer[qname][buffIdx]
                        
                        if tmpstt < crsr[0]:
                            tmpl.append(tmpt)
                            BasicEstimator.logger.debug('Registered %s' % str(tmpt))

                        elif crsr[1] <> tmpjid:
                            tmpl.append(tmpt)
                            BasicEstimator.logger.debug('Registered %s' % str(tmpt))
                            
                        else:
                            tmpl.append(crsr)
                            buffIdx += 1
                            BasicEstimator.logger.debug('Registered %s' % str(crsr))

                    qFile.close()
                    qFile = None

                while buffIdx < len(self.buffer[qname]):
                    crsr = self.buffer[qname][buffIdx]
                    tmpl.append(crsr)
                    buffIdx += 1
                    BasicEstimator.logger.debug('Registered %s' % str(crsr))

                if len(tmpl) > self.sampleNumber:
                    del tmpl[0:len(tmpl)-self.sampleNumber]

                # number of slot is still undefined
                # force R == K
                if nslots < 0:
                    nslots = self.nrun[qname]


                if self.nrun[qname] < nslots:
                    self.setERT(qname, 0)
                    self.setWRT(qname, 0)
                else:


                    tmpAvg = 0
                    tmpMax = -1
                    for tmpt in tmpl:
                        tmpAvg = tmpAvg + tmpt[2]
                        tmpMax = max(tmpMax, tmpt[2])
                    tmpAvg = int(tmpAvg/len(tmpl))
                    
                    tmpFact = int(math.ceil(float(self.nqueued[qname]) / float(nslots) + 1.0))

                    self.setERT(qname, tmpFact * tmpAvg)           
                    self.setWRT(qname, tmpFact * tmpMax)

                qFile = open(qFilename, 'w')
                qFile.write("#nslot %d\n" % nslots)
                for tmpt in tmpl:
                    qFile.write('%d %s %d\n' % tmpt)
                qFile.close()
                qFile = None

            except:
                BasicEstimator.logger.error("Error reading %s" % qFilename, exc_info=True)

            if qFile:
                try:
                    qFile.close()
                except:
                    BasicEstimator.logger.error("Cannot close %s" % qFilename, exc_info=True)
            
            
        oldQueues = os.listdir(self.storeDir)
        for tmpq in oldQueues:
            try:
                if not self.buffer.has_key(tmpq):
                    os.remove(os.path.join(self.storeDir, tmpq))
            except:
                BasicEstimator.logger.error("Cannot remove %s" % tmpq, exc_info=True)


def getEstimatorList():
    return [ BasicEstimator ]


