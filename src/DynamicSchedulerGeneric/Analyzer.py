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
from threading import Thread

class AnalyzeException(Exception):
    
    def __init__(self, msg):
        Exception.__init__(self, msg)


class DataCollector:

    CREAMTYPE = 0
    ESTYPE = 1
    EXTRATYPE = 2

    def __init__(self, config, mjTable):
    
        self.config = config
        self.mjTable = mjTable
        
        self.creamPrefix = config.get('Main', 'cream_prefix')
        if config.has_option('Main', 'es_prefix'):
            self.esPrefix = config.get('Main', 'es_prefix')
        else:
            self.esPrefix = None
        
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

        if self.queuedCREAMJobsForVO(vo) > 0:
            return 0
        
        if vo in self.mjTable:
            availSlots = max(self.mjTable[vo]-self.runningCREAMJobsForVO(vo), 0)
            return min(self.free, availSlots)
            
        return self.free

    def load(self, event):
        tmpdict = eval(event, {"__builtins__" : {}})
        
        for label in ['group', 'queue', 'state', 'name']:
            if not label in tmpdict:
                raise AnalyzeException("Missing %s in %s" % (label, event))

        if tmpdict['name'].startswith(self.creamPrefix):
            srvId = DataCollector.CREAMTYPE
        elif self.esPrefix <> None and tmpdict['name'].startswith(self.esPrefix):
            srvId = DataCollector.ESTYPE
        else:
            #Unknown job
            srvId = DataCollector.EXTRATYPE
        
        vomap = self.config.get('Main','vomap')
        if tmpdict['group'] in vomap:
            tmpdict['group'] = vomap[tmpdict['group']]
        
        key1 = (srvId, tmpdict['state'], tmpdict['group'])
        key2 = (srvId, tmpdict['queue'], tmpdict['state'])
        key3 = (srvId, tmpdict['queue'], tmpdict['state'], tmpdict['group'])
        
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
        

    def runningCREAMJobsForVO(self, voname):
        key = (DataCollector.CREAMTYPE, 'running', voname)
        if key in self.njStateVO:
            return self.njStateVO[key]
        return 0
    
    def runningCREAMJobsOnQueue(self, qname):
        key = (DataCollector.CREAMTYPE, qname, 'running')
        if key in self.njQueueState:
            return self.njQueueState[key]
        return 0

    def runningCREAMJobsOnQueueForVO(self, qname, voname):
        key = (DataCollector.CREAMTYPE, qname, 'running', voname)
        if key in self.njQueueStateVO:
            return self.njQueueStateVO[key]
        return 0

    def queuedCREAMJobsForVO(self, voname):
        key = (DataCollector.CREAMTYPE, 'queued', voname)
        if key in self.njStateVO:
            return self.njStateVO[key]
        return 0
    
    def queuedCREAMJobsOnQueue(self, qname):
        key = (DataCollector.CREAMTYPE, qname, 'queued')
        if key in self.njQueueState:
            return self.njQueueState[key]
        return 0


    def queuedCREAMJobsOnQueueForVO(self, qname, voname):
        key = (DataCollector.CREAMTYPE, qname, 'queued', voname)
        if key in self.njQueueStateVO:
            return self.njQueueStateVO[key]
        return 0

    


    def runningESJobsForVO(self, voname):
        key = (DataCollector.ESTYPE, 'running', voname)
        if key in self.njStateVO:
            return self.njStateVO[key]
        return 0
    
    def runningESJobsOnQueue(self, qname):
        key = (DataCollector.ESTYPE, qname, 'running')
        if key in self.njQueueState:
            return self.njQueueState[key]
        return 0

    def runningESJobsOnQueueForVO(self, qname, voname):
        key = (DataCollector.ESTYPE, qname, 'running', voname)
        if key in self.njQueueStateVO:
            return self.njQueueStateVO[key]
        return 0

    def queuedESJobsForVO(self, voname):
        key = (DataCollector.ESTYPE, 'queued', voname)
        if key in self.njStateVO:
            return self.njStateVO[key]
        return 0
    
    def queuedESJobsOnQueue(self, qname):
        key = (DataCollector.ESTYPE, qname, 'queued')
        if key in self.njQueueState:
            return self.njQueueState[key]
        return 0

    def queuedESJobsOnQueueForVO(self, qname, voname):
        key = (DataCollector.ESTYPE, qname, 'queued', voname)
        if key in self.njQueueStateVO:
            return self.njQueueStateVO[key]
        return 0


class WaitTimeEstimator(DataCollector):

    def __init__(self, config, mjTable):
        DataCollector.__init__(self, config, mjTable)

    def register(self, evndict):
    
        key1 = evndict['queue']
        
        #
        # TODO missing free slots per queue
        #
        if self.free > 0:
            if not key1 in self.ert:
                self.ert[key1] = self.adjett(0)
                self.wrt[key1] = self.adjwrt(0)
            return

        if evndict['state'] == 'running' and 'qtime' in evndict and 'start' in evndict:
            tmpt = evndict['start'] - evndict['qtime']
            
            if not key1 in self.ert:
                self.ert[key1] = tmpt
            else:
                self.ert[key1] += tmpt
                
            if not key1 in self.wrt or self.wrt[key1] < tmpt:
                self.wrt[key1] = tmpt
            
        
    def estimate(self):
        
        for qKey in self.ert:
            runningJobs = self.runningCREAMJobsOnQueue(qKey) + self.runningESJobsOnQueue(qKey)
            self.ert[qKey] = self.adjett(self.ert[qKey] / runningJobs)
            
            self.wrt[qKey] = self.adjwrt(self.wrt[qKey])


    def adjett(self, rawval):
        if rawval < self.cycle:
            return int(self.cycle / 2.)
        else:
            return int(rawval)

    def adjwrt(self, rawval):
        if rawval < self.cycle:
            return int(self.cycle)
        else:
            return int(rawval)







class DataHandler(Thread):

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
                        #
                        # TODO report errors and goon
                        #
                        pass

                line = self.stream.readline();
        
            self.collector.estimate()

        except:
            etype, evalue, etraceback = sys.exc_info()
            #sys.excepthook(etype, evalue, etraceback)
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
    
    collector = WaitTimeEstimator(config, maxjobTable)
    
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

