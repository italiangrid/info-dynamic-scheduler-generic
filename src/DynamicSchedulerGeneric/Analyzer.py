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
        
        self.active = 0
        self.free = 0
        self.now = 0
        self.cycle = 0
        
        self.njStateVO = {}
        self.njQueueState = {}
        self.njQueueStateVO = {}
        
        self.ettVO = {}
        self.ettQueue = {}
        self.ettQueueVO = {}
        
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

    def setAttribute(self, key, value):
        if key == 'nactive':
            self.active = int(value)
        if key == 'nfree':
            self.free = int(value)
        if key == 'now':
            self.now = int(value)
        if key == 'schedcycle':
            self.cycle = int(value)
        










class WaitTimeEstimator(DataCollector):

    def __init__(self, config, mjTable):
        DataCollector.__init__(self, config, mjTable)
        

    def register(self, evndict):
        if evndict['state'] == 'queued' and 'qtime' in evndict:
            
            tmpt = self.now - evndict['qtime']
            
            key1 = evndict['group']
            if not key1 in self.ettVO or self.ettVO[key1] < tmpt:
                self.ettVO[key1] = tmpt

            key2 = evndict['queue']
            if not key2 in self.ettQueue or self.ettQueue[key2] < tmpt:
                self.ettQueue[key2] = tmpt
                
            key3 = (evndict['queue'], evndict['group'])
            if not key3 in self.ettQueueVO or self.ettQueueVO[key3] < tmpt:
                self.ettQueueVO[key3] = tmpt

    
    def estimate_ett(self):
        pass





class DataHandler(Thread):

    def __init__(self, in_stream, collector):
        Thread.__init__(self)
        self.stream = in_stream
        self.collector = collector
        self.evn_re = re.compile("^\s*(\{[^}]+\})\s*$")
        self.prop_re = re.compile("^\s*(\w+)\s+(\d+)\s*$")
        
        
    def run(self):
    
        line = self.stream.readline();
        while line:
            ematch = self.evn_re.match(line)
            if ematch:
                try:
                    self.collector.load(ematch.group(1))
                except AnalyzeException, collect_error:
                    #
                    # TODO report errors and goon
                    #
                    pass
            
            pmatch = self.prop_re.match(line)
            if pmatch:
                key = pmatch.group(1).lower()
                value = pmatch.group(2)
                self.collector.setAttribute(key, value)
            
            line = self.stream.readline();
        
        self.collector.estimate_ett()





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
        
    def get_error(self):
        return None


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
    
    return collector

