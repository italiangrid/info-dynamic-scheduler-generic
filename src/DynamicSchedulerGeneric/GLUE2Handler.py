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

class GLUE2Exception(Exception):
    
    def __init__(self, msg):
        Exception.__init__(self, msg)


share_regex = re.compile("dn:\s*(GLUE2ShareID\s*=\s*[^$]+)")
pol_regex = re.compile("dn:\s*GLUE2PolicyID\s*=\s*[^$]+")
attr_regex = re.compile("(GLUE2\w+)\s*:\s*([^$]+)")


class ShareContainer:

    def __init__(self, dn):
        self.dn = dn
        self.id = None
        self.mqueue = None

    def load(self, line):
        tmpm = attr_regex.match(line)
        if tmpm == None:
            return
            
        key = tmpm.group(1)
        value = tmpm.group(2)
            
        if key == "GLUE2ShareID":
            self.id = value
        elif key == 'GLUE2ComputingShareMappingQueue':
            self.mqueue = value
    
    def close(self):
        if not self.id:
            raise GLUE2Exception("Missing mandatory attribute GLUE2ShareID")
        if not self.mqueue:
            raise GLUE2Exception("Missing mandatory attribute GLUE2ComputingShareMappingQueue")


class PolicyContainer:

    def __init__(self):
        self.vo = None
        self.fkey = None

    def load(self, line):
        tmpm = attr_regex.match(line)
        if tmpm == None:
            return
            
        key = tmpm.group(1)
        value = tmpm.group(2)
            
        if key == "GLUE2PolicyUserDomainForeignKey":
            self.vo = value
        elif key == "GLUE2MappingPolicyShareForeignKey":
            self.fkey = value
    
    def close(self):
        if not self.vo:
            raise GLUE2Exception("Missing mandatory attribute GLUE2PolicyUserDomainForeignKey")
        if not self.fkey:
            raise GLUE2Exception("Missing mandatory attribute GLUE2MappingPolicyShareForeignKey")

def process(config, collector, out=sys.stdout):
    if not config.has_option('Main','static_glue2_ldif_file_computingshare'):
        raise GLUE1Exception("Missing static_glue2_ldif_file_computingshare in configuration")
    
    share_fkeys = {}
    
    #
    # Scanning static file for GLUE2ShareID
    #
    
    static_file = None
    share = None
    
    try:
        static_file = open(config.get('Main','static_glue2_ldif_file_computingshare'),'r')
        
        goon = True
        while goon:
        
            tmps = static_file.readline()
            if tmps == '':
                goon = False
            line = tmps.strip()
            
            if len(line) == 0:
            
                if share <> None:
                    share.close()
                    share_fkeys[share.id] = share
                
                share = None
            
            tmpm = share_regex.match(line)
            if tmpm <> None:
                share = ShareContainer(tmpm.group(1))
                
            elif share <> None:
                share.load(line)
            
    finally:
        if static_file:
            static_file.close()




    #
    # Scanning static file for GLUE2PolicyID
    #
    static_file = None
    policy = None
    
    try:
        static_file = open(config.get('Main','static_glue2_ldif_file_computingshare'),'r')
        
        goon = True
        while goon:
        
            tmps = static_file.readline()
            if tmps == '':
                goon = False
            line = tmps.strip()
            
            if len(line) == 0:
                
                if policy <> None:
                    policy.close()
                    
                    share_id = policy.fkey
                    if not share_id in share_fkeys:
                        raise GLUE2Exception("Invalid foreign key for " + share_id)
                    mqueue = share_fkeys[share_id].mqueue
                    
                    key1 = (mqueue, 'queued', policy.vo)
                    if key1 in collector.njQueueStateVO:
                        nwait = collector.njQueueStateVO[key1]
                    else:
                        nwait = 0
                    
                    key2 = (mqueue, 'running', policy.vo)
                    if key2 in collector.njQueueStateVO:
                        nrun = collector.njQueueStateVO[key2]
                    else:
                        nrun = 0
                    
                    out.write("dn: %s\n" % share_fkeys[share_id].dn)
                    out.write("GLUE2ComputingShareRunningJobs: %d\n" % nrun)
                    out.write("GLUE2ComputingShareWaitingJobs: %d\n" % nwait)
                    out.write("GLUE2ComputingShareTotalJobs: %d\n" % (nrun + nwait))
                    
                    if mqueue in collector.ert:
                        out.write("GLUE2ComputingShareEstimatedAverageWaitingTime: %d\n" 
                                  % collector.ert[mqueue])
                    if mqueue in collector.wrt:
                        out.write("GLUE2ComputingShareEstimatedWorstWaitingTime: %d\n" 
                                  % collector.wrt[mqueue])
                    
                    nfreeSlots = collector.freeSlots(mqueue, policy.vo)
                    if nfreeSlots >= 0:
                        out.write("GLUE2ComputingShareFreeSlots: %d" % nfreeSlots)
                    out.write("\n")
                    
                policy = None
                
            elif pol_regex.match(line):
                policy = PolicyContainer()
                
            elif policy <> None:
                policy.load(line)
            
    finally:
        if static_file:
            static_file.close()




