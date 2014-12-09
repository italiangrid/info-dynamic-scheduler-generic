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
import imp
import re
import glob

GLUE1FORMAT = 'glue1'
GLUE2FORMAT = 'glue2'

class UtilsException(Exception):
    
    def __init__(self, msg):
        Exception.__init__(self, msg)
        

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
    
    if not config.has_option('Main','outputformat'):
        config.set('Main','outfmtcode', '%s+%s' % (GLUE1FORMAT, GLUE2FORMAT))
    else:
        tmpOpt = config.get('Main','outputformat').lower().strip()
        if tmpOpt == "glue1":
            config.set('Main','outfmtcode', GLUE1FORMAT) 
        elif tmpOpt == "glue2":
            config.set('Main','outfmtcode', GLUE2FORMAT)
        elif tmpOpt == "both":
            config.set('Main','outfmtcode', '%s+%s' % (GLUE1FORMAT, GLUE2FORMAT))
        else:
            raise UtilsException("Wrong argument outputformat: %s", tmpOpt)
    
    return config



def getMaxJobsTable(config):
    if not config.has_option('Scheduler','vo_max_jobs_cmd'):
        return dict()
    
    try:
        raw_cmd = config.get('Scheduler','vo_max_jobs_cmd')
        process = subprocess.Popen(shlex.split(raw_cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdoutdata, stderrdata) = process.communicate()
        if process.returncode:
            raise UtilsException("VO max jobs backend command returned " + str(process.returncode))

        #
        # TODO replace with a simple parser, get rid of the eval()
        #
        idx1 = stdoutdata.find("{")
        idx2 = stdoutdata.find("}")
        if idx1 < 0 or idx2 < 0 or idx2 < idx1:
            raise UtilsException('Malformed output for %s' % raw_command)
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
        raise UtilsException('Error running "%s": %s' % (raw_cmd, repr(os_error)))
    except ValueError, value_error:
        raise UtilsException('Wrong arguments for "%s": %s' % (raw_cmd, repr(value_error)))


def loadEstimator(config):
    if config.has_option('Main','estimator'):
        estStr = config.get('Main','estimator')
    else:
        estStr = 'DynamicSchedulerGeneric/PersistentEstimators:BasicEstimator'
    
    idx = estStr.find(':')
    if idx < 0:
        moduleName = estStr
        className = ''
    else:
        tmpl = estStr.split(':')
        moduleName = tmpl[0]
        className = tmpl[1]
    
    modFile = None
    try:
    
        modFile, modPath, modDescr = imp.find_module(moduleName)
        estMod = imp.load_module(moduleName, modFile, modPath, modDescr)
        
        classList = estMod.getEstimatorList()
        
        if className == '':
            return classList[0]
        
        for classItem in classList:
            if classItem.__name__ == className:
                return classItem
        
        raise Exception("Cannot find class %s" % className)
        
    finally:
        if modFile:
            modFile.close()
    
    return None


bdiiCfgRegex = re.compile('^\s*BDII_([^=\s]+)\s*=(.+)$')

def getBDIIConfig(bdiiConffile):

    result = dict()
    
    cFile = None
    try:
        cFile = open(bdiiConffile)
        
        for line in cFile:
            parsed = bdiiCfgRegex.match(line)
            if parsed:
                result[parsed.group(1).lower()] = parsed.group(2).strip()

    finally:
        if cFile:
            cFile.close()
    
    return result


def getLDIFFilelist(config, shortcut=None, custom_path_tag=None):

    if custom_path_tag and config.has_option('Main', custom_path_tag):
        return [config.get('Main', custom_path_tag)]

    if config.has_option('Main','bdii-configfile'):
        bdiiConfig = getBDIIConfig(config.get('Main', 'bdii-configfile'))
    else:
        bdiiConfig = getBDIIConfig('/etc/bdii/bdii.conf')
    
    if 'ldif_dir' in bdiiConfig:
        ldifDir = bdiiConfig['ldif_dir']
    else:
        ldifDir = '/var/lib/bdii/gip/ldif'

    #
    # shortcut: check a predefined ldif-file or return all ones
    #
    tmpl = glob.glob(ldifDir + '/*.ldif')
    
    if not shortcut:
        return tmpl
    
    tmpf = ldifDir + '/' + shortcut
    
    if tmpf in tmpl:
        return [tmpf]
    
    return tmpl



