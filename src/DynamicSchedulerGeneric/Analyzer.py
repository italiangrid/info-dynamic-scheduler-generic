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
import re
import shlex
import subprocess
import logging
from threading import Thread

import Utils

class AnalyzeException(Exception):
    
    def __init__(self, msg):
        Exception.__init__(self, msg)


class DataCollector:

    def __init__(self, config, mjTable):
    
        self.config = config
        self.mjTable = mjTable
        
        self.active = -1
        self.free = -1
        self.now = -1
        self.cycle = -1
        
        self.njStateVO = {}
        self.njQueueState = {}
        self.njQueueStateVO = {}
        
        self.ert = {}
        self.wrt = {}
        
    def register(self, evndict):
        pass
    
    def estimate(self):
        pass
        
    def freeSlots(self, queue, vo):
        #Since the scheduler handles free slots and max free slots per VO
        #without considering the queue, it is necessary to aggregate
        #the value for all queues, so first parameter is ignored

        if self.queuedJobsForVO(vo) > 0:
            return 0
        
        if vo in self.mjTable:
            availSlots = max(self.mjTable[vo]-self.runningJobsForVO(vo), 0)
            return min(self.free, availSlots)
            
        return self.free

    def load(self, event):
        tmpdict = eval(event, {"__builtins__" : {}})
        
        for label in ['group', 'queue', 'state']:
            if not label in tmpdict:
                raise AnalyzeException("Missing %s in %s" % (label, event))

        vomap = self.config.get('Main','vomap')
        if tmpdict['group'] in vomap:
            tmpdict['group'] = vomap[tmpdict['group']]
        
        key1 = (tmpdict['state'], tmpdict['group'])
        key2 = (tmpdict['queue'], tmpdict['state'])
        key3 = (tmpdict['queue'], tmpdict['state'], tmpdict['group'])
        
        if key1 in self.njStateVO:
            self.njStateVO[key1] += 1
        else:
            self.njStateVO[key1] = 1
            
        if key2 in self.njQueueState:
            self.njQueueState[key2] += 1
        else:
            self.njQueueState[key2] = 1

        if key3 in self.njQueueStateVO:
            self.njQueueStateVO[key3] += 1
        else:
            self.njQueueStateVO[key3] = 1
            
        self.register(tmpdict)
        

    def getERT(self, qName):
        return self.ert[qName]
    
    def setERT(self, qName, value):
        if value < self.cycle:
            self.ert[qName] = int(self.cycle / 2)
        else:
            self.ert[qName] = int(value)
            
    def isSetERT(self, qName):
        return qName in self.ert
    
    def getWRT(self, qName):
        return self.wrt[qName]
    
    def setWRT(self, qName, value):
        if value < self.cycle:
            self.wrt[qName] = int(self.cycle)
        else:
            self.wrt[qName] = int(value)
            
    def isSetWRT(self, qName):
        return qName in self.wrt
    
    def runningJobsForVO(self, voname):
        key = ('running', voname)
        if key in self.njStateVO:
            return self.njStateVO[key]
        return 0
    
    def runningJobsOnQueue(self, qname):
        key = (qname, 'running')
        if key in self.njQueueState:
            return self.njQueueState[key]
        return 0

    def runningJobsOnQueueForVO(self, qname, voname):
        key = (qname, 'running', voname)
        if key in self.njQueueStateVO:
            return self.njQueueStateVO[key]
        return 0

    def queuedJobsForVO(self, voname):
        key = ('queued', voname)
        if key in self.njStateVO:
            return self.njStateVO[key]
        return 0
    
    def queuedJobsOnQueue(self, qname):
        key = (qname, 'queued')
        if key in self.njQueueState:
            return self.njQueueState[key]
        return 0


    def queuedJobsOnQueueForVO(self, qname, voname):
        key = (qname, 'queued', voname)
        if key in self.njQueueStateVO:
            return self.njQueueStateVO[key]
        return 0




class DataHandler(Thread):

    logger = logging.getLogger("Analyzer.DataHandler")

    def __init__(self, in_stream, collector):
        Thread.__init__(self)
        self.stream = in_stream
        self.collector = collector
        self.evn_re = re.compile("^\s*(\{[^}]+\})\s*$")
        self.prop_re = re.compile("^\s*(\w+)\s+(\d+)\s*$")
        self.internerr = None
        
    def run(self):
    
        try:
            tmpc = 0
            line = self.stream.readline();
            while line:
        
                pmatch = self.prop_re.match(line)
                if pmatch:
                    key = pmatch.group(1).lower()
                    value = pmatch.group(2)
                    if key == 'nactive':
                        self.collector.active = int(value)
                        tmpc |= 1
                    elif key == 'nfree':
                        self.collector.free = int(value)
                        tmpc |= 2
                    elif key == 'now':
                        self.collector.now = int(value)
                        tmpc |= 4
                    elif key == 'schedcycle':
                        self.collector.cycle = int(value)
                        tmpc |= 8

                ematch = self.evn_re.match(line)
                if ematch:
                
                    if tmpc < 15:
                        raise Exception("Missing attributes before job table")
                        
                    try:
                        self.collector.load(ematch.group(1))
                    except AnalyzeException, collect_error:
                        logger.error("Cannot analyze: %s (%s)" %(ematch.group(1), str(collect_error)))

                line = self.stream.readline();
        
            self.collector.estimate()
            
        except:
            etype, evalue, etraceback = sys.exc_info()
            sys.excepthook(etype, evalue, etraceback)
            self.internerr = "%s: (%s)" % (etype, evalue)


class ErrorHandler(Thread):

    def __init__(self, err_stream):
        Thread.__init__(self)
        self.stream = err_stream
        self.message = ""
    
    def run(self):
        line = self.stream.readline()
        while line:
            self.message = self.message + line
            line = self.stream.readline()
        


def analyze(config, maxjobTable):
    
    if not config.has_option('LRMS','lrms_backend_cmd'):
        raise AnalyzeException("Missing LRMS backend command in configuration")
    
    estimatorClass = Utils.loadEstimator(config)
    collector = estimatorClass(config, maxjobTable)
    
    cmd = shlex.split(config.get('LRMS','lrms_backend_cmd'))
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    stdout_thread = DataHandler(process.stdout, collector)
    stderr_thread = ErrorHandler(process.stderr)
    
    stdout_thread.start()
    stderr_thread.start()
    
    ret_code = process.wait()
    
    stdout_thread.join()
    stderr_thread.join()
    
    if ret_code > 0:
        raise AnalyzeException(stderr_thread.message)
        
    if stdout_thread.internerr:
        raise AnalyzeException(stdout_thread.internerr)
    
    return collector

