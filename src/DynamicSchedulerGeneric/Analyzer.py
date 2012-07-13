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
        
        self.ettQueue = {}
        
    def register(self, evndict):
        pass
    
    def estimate_ett(self):
        pass

    def load(self, event):
        tmpdict = eval(event, {"__builtins__" : {}})
        
        if not 'group' in tmpdict:
            raise AnalyzeException("Missing group in " + event)
        if not 'queue' in tmpdict:
            raise AnalyzeException("Missing queue in " + event)
        if not 'state' in tmpdict:
            raise AnalyzeException("Missing state in " + event)
        
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
        









class WaitTimeEstimator(DataCollector):

    def __init__(self, config, mjTable):
        DataCollector.__init__(self, config, mjTable)
        self.min_rtime = -1

    def register(self, evndict):
    
        key1 = evndict['queue']
        
        if self.free > 0:
            if not key1 in self.ettQueue:
                self.ettQueue[key1] = self._adjusted_ett(0)
            return

        if evndict['state'] == 'queued' and 'qtime' in evndict:
            
            tmpt = self._adjusted_ett(self.now - evndict['qtime'])
            
            if not key1 in self.ettQueue or self.ettQueue[key1] < tmpt:
                self.ettQueue[key1] = tmpt
                
        if evndict['state'] == 'running' and 'start' in evndict:
            if evndict['start'] > self.min_rtime:
                self.min_rtime = evndict['start']
    
    def estimate_ett(self):
        pass

    def _adjusted_ett(self, rawval):
        if rawval < self.cycle:
            return int(self.cycle / 2.)
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
        
            self.collector.estimate_ett()

        except:
            self.internerr = str(sys.exc_info()[0])


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

