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

import shlex
import subprocess
import ConfigParser

def readConfigurationFromFile(configfile):

    config = ConfigParser.ConfigParser()
    
    conffile_desc = None
    try:
        conffile_desc = open(configfile)
        config.readfp(conffile_desc)
    finally:
        if conffile_desc <> None:
            conffile_desc.close()

    vomap = dict()
    if config.has_option('Main','vomap'):
        lines = config.get('Main','vomap').split('\n')
        for line in lines:
            tmpl = line.split(':')
            if len(tmpl) == 2:
                group = tmpl[0].strip()
                vo = tmpl[1].strip()
                vomap[group] = vo
    config.set('Main','vomap', vomap)
    
    return config



def getMaxJobsTable(config):
    if not config.has_option('Scheduler','vo_max_jobs_cmd'):
        return dict()
    
    try:
        raw_cmd = config.get('Scheduler','vo_max_jobs_cmd')
        process = subprocess.Popen(shlex.split(raw_cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdoutdata, stderrdata) = process.communicate()
        if process.returncode:
            raise Exception("VO max jobs backend command returned " + process.returncode)

        #
        # TODO replace with a simple parser, get rid of the eval()
        #
        idx1 = stdoutdata.find("{")
        idx2 = stdoutdata.find("}")
        if idx1 < 0 or idx2 < 0 or idx2 < idx1:
            raise Exception('Malformed output for %s' % raw_command)
        dictString = stdoutdata[idx1:idx2+1]
        grpDict = eval(dictString, {"__builtins__" : {}})
        
        result = dict()
        vomap = config.get('Main','vomap')
        for grpName in grpDict:
            if vomap.has_key(grpName):
                tmps = vomap[grpName]
            else:
                tmps = grpName
            if result.has_key(tmps):
                result[tmps] = result[tmps] + grpDict[grpName]
            else:
                result[tmps] = grpDict[grpName]
        return result
        
    except OSError, os_error:
        raise Exception('Error running "%s": %s' % (raw_cmd, repr(os_error)))
    except ValueError, value_error:
        raise Exception('Wrong arguments for "%s": %s' % (raw_cmd, repr(value_error)))


