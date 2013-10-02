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

class GLUE2Exception(Exception):
    
    def __init__(self, msg):
        Exception.__init__(self, msg)


share_regex = re.compile("dn:\s*GLUE2ShareID\s*=\s*.+")
pol_regex = re.compile("dn:\s*GLUE2PolicyID\s*=\s*.+")
attr_regex = re.compile("(GLUE2\w+)\s*:\s*(.+)")


class ShareContainer:

    def __init__(self):
        self.id = None
        self.mqueue = None

    def check(self):
        if not self.id:
            raise GLUE2Exception("Missing mandatory attribute GLUE2ShareID")
        if not self.mqueue:
            raise GLUE2Exception("Missing mandatory attribute GLUE2ComputingShareMappingQueue")


class PolicyContainer:

    def __init__(self):
        self.vo = None
        self.fkey = None

    def check(self):
        if not self.vo:
            raise GLUE2Exception("Missing mandatory attribute GLUE2PolicyUserDomainForeignKey")
        if not self.fkey:
            raise GLUE2Exception("Missing mandatory attribute GLUE2MappingPolicyShareForeignKey")


def parseGLUETemplate(ldifFilename, shareTable, policyTable, share_fkeys):
    
    static_file = None
    
    try:
    
        currShare = None
        currPolicy = None
        
        static_file = open(ldifFilename)
        
        for line in static_file:
        
            parsed = share_regex.match(line)
            if parsed:
                currShare = line.strip()
                shareTable[currShare] = ShareContainer()
                continue


            parsed = pol_regex.match(line)
            # Ignore policy for endpoint in case
            if parsed and line.find('GLUE2Share') > 0:
                currPolicy = line.strip()
                policyTable[currPolicy] = PolicyContainer()
                continue

            parsed = attr_regex.match(line)
            if parsed:
                key = parsed.group(1)
                value =  parsed.group(2).strip()
                
                if key == 'GLUE2ShareID' and currShare:
                
                    shareTable[currShare].id = value
                
                elif key == 'GLUE2ComputingShareMappingQueue' and currShare:
                
                    shareTable[currShare].mqueue = value
                
                elif key == 'GLUE2PolicyUserDomainForeignKey' and currPolicy:
                
                    policyTable[currPolicy].vo = value
                
                elif key == 'GLUE2MappingPolicyShareForeignKey' and currPolicy:
                
                    policyTable[currPolicy].fkey = value
                
                continue

            if len(line.strip()) == 0:
            
                if currShare:
                    shareTable[currShare].check()
                
                if currPolicy:
                    policyTable[currPolicy].check()
                    share_fkeys[policyTable[currPolicy].fkey] = policyTable[currPolicy]

                currShare = None
                currPolicy = None

        #close cycle
        if currShare:
            shareTable[currShare].check()
                
        if currPolicy:
            policyTable[currPolicy].check()
            share_fkeys[policyTable[currPolicy].fkey] = policyTable[currPolicy]
        
        for shareID in share_fkeys:
            missing = True
            for shareData in shareTable.values():
                if shareID == shareData.id:
                    missing = False
            if missing:
                raise GLUE2Exception("Invalid foreign key " + shareID)


    finally:
        if static_file:
            static_file.close()


def process(config, collector, out=sys.stdout):
    
    shareTable = dict()
    policyTable = dict()
    share_fkeys = dict()

    ldifList = DynSchedUtils.getLDIFFilelist(config, 'ComputingShare.ldif')
    
    for ldifFilename in ldifList:
        parseGLUETemplate(ldifFilename, shareTable, policyTable, share_fkeys)

    for shareDN in shareTable:
    
        shareData = shareTable[shareDN]
        policyData = share_fkeys[shareData.id]
        
        out.write("%s\n" % shareDN)
        
        nwait = collector.queuedJobsOnQueueForVO(shareData.mqueue, policyData.vo)
        nrun = collector.runningJobsOnQueueForVO(shareData.mqueue, policyData.vo)
        
        out.write("GLUE2ComputingShareRunningJobs: %d\n" % nrun)
        out.write("GLUE2ComputingShareWaitingJobs: %d\n" % nwait)
        out.write("GLUE2ComputingShareTotalJobs: %d\n" % (nrun + nwait))
                    
        if collector.isSetERT(shareData.mqueue):
            out.write("GLUE2ComputingShareEstimatedAverageWaitingTime: %d\n" % collector.getERT(shareData.mqueue))
        else:
            out.write("GLUE2ComputingShareEstimatedAverageWaitingTime: 0\n")
                        
        if collector.isSetWRT(shareData.mqueue):
            out.write("GLUE2ComputingShareEstimatedWorstWaitingTime: %d\n" % collector.getWRT(shareData.mqueue))
        else:
            out.write("GLUE2ComputingShareEstimatedWorstWaitingTime: 0\n")
                    
        nfreeSlots = collector.freeSlots(shareData.mqueue, policyData.vo)
        if nfreeSlots >= 0:
            out.write("GLUE2ComputingShareFreeSlots: %d\n" % nfreeSlots)
            
        out.write("\n")
        


