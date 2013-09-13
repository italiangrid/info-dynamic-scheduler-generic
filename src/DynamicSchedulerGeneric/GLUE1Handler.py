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

from DynamicSchedulerGeneric import Utils as DynSchedUtils

class GLUE1Exception(Exception):
    
    def __init__(self, msg):
        Exception.__init__(self, msg)


ce_regex = re.compile("dn:\s*GlueCEUniqueID\s*=\s*.+")
vo_regex = re.compile("dn:\s*GlueVOViewLocalID\s*=\s*.+")
attr_regex = re.compile("(Glue\w+)\s*:\s*(.+)")
chunk_key_regex = re.compile("GlueCEUniqueID\s*=\s*(.+)")
acbr_regex = re.compile("(VO|VOMS)\s*:\s*(.+)")
    
class ACBR:

    def __init__(self, acbrStr):
        parsed = acbr_regex.match(acbrStr)
        if not parsed:
            raise GLUE1Exception("Wrong ACBR definition: %s" % acbrStr)
        self.fmt = parsed.group(1)
        self.name = parsed.group(2).strip()
        
class GlueCEContainer:

    def __init__(self):
        self.id = None
        self.queue = None
        self.acbrs = set()
        
    def check(self):
        if not self.queue:
            raise GLUE1Exception("Missing mandatory attribute GlueCEName")
        if not self.id:
            raise GLUE1Exception("Missing mandatory attribute GlueCEUniqueID")
        

class GlueVOViewContainer:

    def __init__(self):
        self.id = None
        self.name = None
        self.fkey = None

    def check(self):
        if self.fkey == None:
            raise GLUE1Exception("Missing foreing key for GlueCEUniqueID for %s" % self.id)
        if self.name == None:
            raise GLUE1Exception("Missing ACBR for %s" % self.id)


def parseGLUETemplate(tplFilename, glueCETable, voViewTable, ce_fkeys):

    static_file = None

    try:
    
        currGLUECE = None
        currVOView = None
    
        static_file = open(tplFilename)
        
        for line in static_file:
        
            parsed = ce_regex.match(line)
            if parsed:
                currGLUECE = line.strip()
                glueCETable[currGLUECE] = GlueCEContainer()
                continue
            
            parsed = vo_regex.match(line)
            if parsed:
                currVOView = line.strip()
                voViewTable[currVOView] = GlueVOViewContainer()
                continue
            
            parsed = attr_regex.match(line)
            if parsed:
                key = parsed.group(1)
                value =  parsed.group(2).strip()
                
                if key == "GlueCEName" and currGLUECE:
            
                    glueCETable[currGLUECE].queue = value
                
                elif key == 'GlueCEUniqueID' and currGLUECE:
            
                    glueCETable[currGLUECE].id = value
                    
                elif key == "GlueCEAccessControlBaseRule" and currGLUECE:
                
                    acbrItem = ACBR(value)
                    glueCETable[currGLUECE].acbrs.add(acbrItem.name)
                
                elif key == "GlueVOViewLocalID" and currVOView:
            
                    voViewTable[currVOView].id = value
                
                elif key == "GlueChunkKey" and currVOView:
                    
                    parsed = chunk_key_regex.match(value)
                    if parsed:
                        voViewTable[currVOView].fkey = parsed.group(1).strip()
                
                elif key == "GlueCEAccessControlBaseRule" and currVOView:
                    
                    # select the first valid ACBR to be the vo name
                    if voViewTable[currVOView].name == None:
                        acbrItem = ACBR(value)
                        voViewTable[currVOView].name = acbrItem.name
                
                continue
            
            if len(line.strip()) == 0:
            
                if currGLUECE:
                    glueCETable[currGLUECE].check()
                    tmpid = glueCETable[currGLUECE].id
                    tmpqueue = glueCETable[currGLUECE].queue
                    ce_fkeys[tmpid] = tmpqueue
                
                if currVOView:
                    voViewTable[currVOView].check()

                currGLUECE = None
                currVOView = None
            
        #close cycle
        if currGLUECE:
            glueCETable[currGLUECE].check()
            tmpid = glueCETable[currGLUECE].id
            tmpqueue = glueCETable[currGLUECE].queue
            ce_fkeys[tmpid] = tmpqueue
                
        if currVOView:
            voViewTable[currVOView].check()

    finally:
        if static_file:
            static_file.close()


def process(config, collector, out=sys.stdout):

    glueCETable = dict()
    voViewTable = dict()
    ce_fkeys = dict()

    ldifList = DynSchedUtils.getLDIFFilelist(config, 'static-file-CE.ldif')
    
    for ldifFilename in ldifList:
        parseGLUETemplate(ldifFilename, glueCETable, voViewTable, ce_fkeys)

    for glueceDN in glueCETable:
    
        ceData = glueCETable[glueceDN]
        
        out.write("%s\n" % glueceDN)
        
        nwait = collector.queuedJobsOnQueue(ceData.queue)
        nrun = collector.runningJobsOnQueue(ceData.queue)
                
        out.write("GlueCEStateWaitingJobs: %d\n" % nwait)
        out.write("GlueCEStateRunningJobs: %d\n" % nrun)
        out.write("GlueCEStateTotalJobs: %d\n" % (nrun + nwait))
                    
        if collector.isSetERT(ceData.queue):
            out.write("GlueCEStateEstimatedResponseTime: %d\n" % collector.getERT(ceData.queue))
        else:
            out.write("GlueCEStateEstimatedResponseTime: 0\n")
                        
        if collector.isSetWRT(ceData.queue):
            out.write("GlueCEStateWorstResponseTime: %d\n" % collector.getWRT(ceData.queue))
        else:
            out.write("GlueCEStateWorstResponseTime: 0\n")
                    
        out.write("GlueCEStateFreeJobSlots: %d" % collector.free)
                    
        out.write("\n");
    

    for voviewDN in voViewTable:
        
        voData = voViewTable[voviewDN]

        out.write("%s\n" % voviewDN)
        
        if not voData.fkey in ce_fkeys:
            raise GLUE1Exception("Invalid foreign key for " + voviewDN)
                    
        queue = ce_fkeys[voData.fkey]
                    
        nwait = collector.queuedJobsOnQueueForVO(queue, voData.name)
        nrun = collector.runningJobsOnQueueForVO(queue, voData.name)
                    
        out.write("GlueCEStateWaitingJobs: %d\n" % nwait)
        out.write("GlueCEStateRunningJobs: %d\n" % nrun)
        out.write("GlueCEStateTotalJobs: %d\n" % (nrun + nwait))

        if collector.isSetERT(queue):
            out.write("GlueCEStateEstimatedResponseTime: %d\n" % collector.getERT(queue))
        else:
            out.write("GlueCEStateEstimatedResponseTime: 0\n")
                    
        if collector.isSetWRT(queue):
            out.write("GlueCEStateWorstResponseTime: %d\n" % collector.getWRT(queue))
        else:
            out.write("GlueCEStateWorstResponseTime: 0\n")
                    
        nfreeSlots = collector.freeSlots(queue, voData.name)
        if nfreeSlots >= 0:
            out.write("GlueCEStateFreeJobSlots: %d" % nfreeSlots)

        out.write("\n");

