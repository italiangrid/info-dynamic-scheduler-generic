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

class GLUE1Exception(Exception):
    
    def __init__(self, msg):
        Exception.__init__(self, msg)


ce_regex = re.compile("dn:\s*GlueCEUniqueID\s*=\s*[^$]+")
vo_regex = re.compile("dn:\s*GlueVOViewLocalID\s*=\s*[^$]+")
attr_regex = re.compile("(Glue\w+)\s*:\s*([^$]+)")
    

class GlueCEContainer:

    def __init__(self):
        self.id = None
        self.queue = None
        self.acbrs = set()
        
    def load(self, line):
    
        tmpm = attr_regex.match(line)
        if tmpm == None:
            return
            
        key = tmpm.group(1)
        value = tmpm.group(2)
            
        if key == "GlueCEName":
            
            self.queue = value
                
        elif key == 'GlueCEUniqueID':
            
            self.id = value
                
        elif key == "GlueCEAccessControlBaseRule":
            
            if value.startswith("VO:") or value.startswith("VOMS:"):
                voname = value.split(':')[1]
                if len(voname) == 0:
                    raise GLUE1Exception("Empty VO name in acbr")
                self.acbrs.add(voname)


        
    def close(self):
        if not self.queue:
            raise GLUE1Exception("Missing mandatory attribute GlueCEName")
        if not self.id:
            raise GLUE1Exception("Missing mandatory attribute GlueCEUniqueID")
        

class GlueVOViewContainer:

    def __init__(self):
        self.id = None
        self.fkey = None
        
    def load(self, line):
        tmpm = attr_regex.match(line)
        if tmpm == None:
            return
            
        key = tmpm.group(1)
        value = tmpm.group(2)
        
        if key == "GlueVOViewLocalID":
            
            #
            # TODO verify this is the name of the VO  (see ACBR)
            #
            self.id = value
            
        elif key == "GlueChunkKey":
        
            tmpl = value.split('=')
            if len(tmpl)<2:
                raise GLUE1Exception("Wrong format for GlueChunkKey")
            label = tmpl[0].strip()
            tmpk = tmpl[1].strip()
            if label == "GlueCEUniqueID" and len(tmpk) > 0:
                self.fkey = tmpk

    def close(self):
        if self.fkey == None:
            raise GLUE1Exception("Missing foreing key for GlueCEUniqueID")




def process(config, collector, out=sys.stdout):

    if not config.has_option('Main','static_ldif_file'):
        raise GLUE1Exception("Missing static_ldif_file in configuration")
    
    ce_fkeys = {}

    #
    # Scanning static file for GlueCEUniqueID
    #
    gluece = None
    static_file = None

    try:
    
        static_file = open(config.get('Main','static_ldif_file'),'r')
    
        goon = True
        while goon:
        
            tmps = static_file.readline()
            if tmps == '':
                goon = False
            line = tmps.strip()
        
            if len(line) == 0:
            
                if gluece <> None:
            
                    gluece.close()
                    
                    ce_fkeys[gluece.id] = gluece.queue
                
                    nwait = collector.queuedCREAMJobsOnQueue(gluece.queue)
                        
                    nrun = collector.runningCREAMJobsOnQueue(gluece.queue)
                
                    out.write("GlueCEStateWaitingJobs: %d\n" % nwait)
                    out.write("GlueCEStateRunningJobs: %d\n" % nrun)
                    out.write("GlueCEStateTotalJobs: %d\n" % (nrun + nwait))
                    
                    if collector.isSetERT(gluece.queue):
                        out.write("GlueCEStateEstimatedResponseTime: %d\n" 
                                  % collector.getERT(gluece.queue))
                    if collector.isSetWRT(gluece.queue):
                        out.write("GlueCEStateWorstResponseTime: %d\n" 
                                  % collector.getWRT(gluece.queue))
                    
                    out.write("GlueCEStateFreeJobSlots: %d" % collector.free)
                    
                    out.write("\n");
                
                gluece = None
            
            elif ce_regex.match(line):
        
                gluece = GlueCEContainer()
                out.write(line + '\n')
            
            elif gluece <> None:
                gluece.load(line)
                    
    finally:
        if static_file:
            static_file.close()
    
    #
    # Scanning static file for GlueVOViewLocalID
    #
    gluevoview = None
    static_file = None
    
    try:
    
        static_file = open(config.get('Main','static_ldif_file'),'r')
    
        goon = True
        while goon:
        
            tmps = static_file.readline()
            if tmps == '':
                goon = False
            line = tmps.strip()
        
            if len(line) == 0:
            
                if gluevoview <> None:
            
                    gluevoview.close()
                    
                    if not gluevoview.fkey in ce_fkeys:
                        raise GLUE1Exception("Invalid foreign key for " + gluevoview.id)
                    queue = ce_fkeys[gluevoview.fkey]
                    
                    nwait = collector.queuedCREAMJobsOnQueueForVO(queue, gluevoview.id)

                    nrun = collector.runningCREAMJobsOnQueueForVO(queue, gluevoview.id)
                    
                    out.write("GlueCEStateWaitingJobs: %d\n" % nwait)
                    out.write("GlueCEStateRunningJobs: %d\n" % nrun)
                    out.write("GlueCEStateTotalJobs: %d\n" % (nrun + nwait))

                    if collector.isSetERT(queue):
                        out.write("GlueCEStateEstimatedResponseTime: %d\n" 
                                  % collector.getERT(queue))
                    if collector.isSetWRT(queue):
                        out.write("GlueCEStateWorstResponseTime: %d\n" 
                                  % collector.getWRT(queue))
                    
                    nfreeSlots = collector.freeSlots(queue, gluevoview.id)
                    if nfreeSlots >= 0:
                        out.write("GlueCEStateFreeJobSlots: %d" % nfreeSlots)

                    out.write("\n");
                    
                gluevoview = None
            
            elif vo_regex.match(line):
        
                gluevoview = GlueVOViewContainer()
                out.write(line + '\n')
            
            elif gluevoview <> None:
                gluevoview.load(line)
                    
    finally:
        if static_file:
            static_file.close()
    
    
    
    
