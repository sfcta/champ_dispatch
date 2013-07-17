"""  

  http://github.com/sfcta/dispatch

  Timesheet, Copyright 2013 San Francisco County Transportation Authority
                            San Francisco, CA, USA
                            http://www.sfcta.org/
                            info@sfcta.org

  This file is part of dispatch.

  Dispatch is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  Timesheet is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Timesheet.  If not, see <http://www.gnu.org/licenses/>.
"""

import Pyro.core
import os, sys, time, traceback
import threading
import atexit

def threaded(f):
    def wrapper(*args):
        t = threading.Thread(target=f, args=args)
        t.start()
    return wrapper

def startThread(daemon):
    daemon.requestLoop()

def exceptcatcher(type,value,tb):
    print "\nHalted or CTRL-C pressed."
    traceback.print_tb(tb)
    print str(value)

def killDaemon():
    daemon.shutdown(True)

def getEnvVarsFromOS():
    sysvars = {}

    # These are the variables that we don't want to overwrite on the client machines.
    # Anything NOT on this list will get propagated to the helpers.
    removeList = [
        "ALLUSERSPROFILE", "APPDATA", "CLIENTNAME", "CommonProgramFiles",
        "COMPUTERNAME", "ComSpec", "FP_NO_HOST_CHECK", "HOMEDRIVE",
        "HOMEPATH", "PATH", "LOGONSERVER", "NUMBER_OF_PROCESSORS",
        "OS", "ProgramFiles", "PROMPT", "PYTHONPATH", "SESSIONNAME",
        "SystemDrive", "SystemRoot", "USERDNSDOMAIN", "USERDOMAIN",
        "USERNAME", "USERPROFILE", "windir"]

    for var in os.environ.keys():
        if var in removeList:
            continue
        sysvars[var] = os.getenv(var)

    return sysvars


def parseJobset(jobsetfilename):
    try:
        jset = open(jobsetfilename,'r')
        workdir = os.getcwd()
        jobset = []

        for line in jset:
            cmd = line.strip()
            if (len(cmd)>0):
                if (cmd.startswith("wd ") | cmd.startswith("cd ")):
                    workdir = cmd[3:]
                    continue
                # TODO Could also add a function for SETting env vars here too
                jobset.append(Job(cmd,workdir,sysEnv))
        return jobset

    except:
        print "Error reading",jobsetfilename
        sys.exit(2)


class Job:
    def __init__(self,cmd,workdir,env):
        self.cmd = cmd
        self.workdir = workdir
        self.env = env
    
class JobList(Pyro.core.ObjBase):
    def __init__(self, jobs=None):
        Pyro.core.ObjBase.__init__(self)
        self.listlock = threading.Lock()
        self.numJobs = 0
        self.killHelperBees = False

        # Create job "dictionaries"
        self.AvailableJobs = {}
        self.TakenJobs = {}

        if (jobs != None):
            for job in jobs:
                self.AvailableJobs[self.numJobs] = job
                self.numJobs += 1
        
    def append(self, job):
        # Any operations on the joblist must be wrapped in the listlock!
        self.listlock.acquire()
        self.AvailableJobs[self.numJobs] = job
        self.numJobs += 1
        self.listlock.release()

    def killMe(self):
            return self.killHelperBees
            
    def get(self):
        self.listlock.acquire()
        jobnum, job = -1, None

        if len(self.AvailableJobs)>0:
            jobnum, job = self.AvailableJobs.popitem()
            self.TakenJobs[jobnum] = job
            print "\nSending job:",job.cmd,'\n            ',job.workdir
            time.sleep(1)

        self.listlock.release()
        return jobnum, job

    def alldone(self, jobnum, job, rtncode, logname):
        try:
            self.listlock.acquire()

            # Check exit code!!
            failed = False
            if (rtncode != 0):
                failed = True
                # runtpp uses weird return codes:  0=ok, 1=warn, 2=error.  Only halt on 2.
                if (job.cmd.startswith("runtpp") and rtncode==1):
                    failed = False

            if (failed):
                print "ERROR! Job:",job.cmd,'returned code',rtncode
                self.AvailableJobs = []
                print "-----------------------------------------------\n",time.asctime(),":  Errors. Killing remaining jobs."

                # Set kill flag so helperbees know that they must quit!
                self.killHelperBees = True

                # Try this to close out the dispatcher and return the (failed) exitcode.
                self.listlock.release()
                # daemon.shutdown()  -- commented out because python is screwy on exit, not getting rtncode.

                # Python isn't returning the error code correctly, so pause here:
                #sys.exit(rtncode)
                print "FAILED with error code",rtncode," -- PRESS CTRL-C to quit."
                get_input()

            self.TakenJobs.pop(jobnum)
            stilltogo = len(self.TakenJobs)+len(self.AvailableJobs)

            if (stilltogo==0):
                print "-----------------------------------------------\n",time.asctime(),":  All jobs completed.\n"
                self.listlock.release()
                daemon.shutdown()
                sys.exit(rtncode)

            self.listlock.release()

            # Remove temp logfile
            os.remove(logname)
        except:
            pass

# ----- Startup -------------------------------------
if __name__ == "__main__":
    if len(sys.argv)<=2:
        print "Usage:  python dispatcher.py  jobsetfile.jset  MACHINE1  MACHINE2  MACHINE3...\n"
        sys.exit(2)

    sysEnv = getEnvVarsFromOS()
    Jobs = JobList(parseJobset(sys.argv[1]))

    # Die gracefully
    atexit.register(killDaemon)
    
    # Uncomment this to suppress tracebacks on errors:
    sys.excepthook = exceptcatcher  


    print "\n-----------------------------------------------\nSF-CHAMP Dispatcher\n",time.asctime(),": ",len(Jobs.AvailableJobs), "jobs queued.\n",sys.argv[1],"\n-----------------------------------------------"

    # Set up the request dispatcher. 
    Pyro.core.initServer(banner=0)
    useport = -1
    daemon = None

    # Find a port # that is unused by other running dispatchers
    for pt in range(6411,6420):
        try:
            print "Trying to start daemon for point %d" % pt
            daemon = Pyro.core.Daemon(port=pt,norange=1)
            useport = pt
            # If that worked, we're done
            print "Useport = %d" % useport
            break;
        except Exception as inst:
            print type(inst)
            print inst
            pass

    # Got a working port, now let's connect to it
    uri = daemon.connect(Jobs,"joblist")

    # Figure out which helpers to call
    machineNames = []

    for i in range(2,len(sys.argv)):
        machines = sys.argv[i].split(' ')
        for j in machines:
            machineNames.append(j)
    print "Calling:",machineNames,' from dispatcher #',str(pt-6410)

    # Call for help!
    dispatcherURI = 'PYROLOC://' + os.getenv("COMPUTERNAME") + ':'+str(useport)+'/joblist'  
    print dispatcherURI

    helperbees = []
    for i in machineNames:
        helpURI = 'PYROLOC://' + i + '/help'
        try:
            bee = Pyro.core.getProxyForURI(helpURI)
            print bee
            bee.help(dispatcherURI)
            helperbees.append(bee)
        except:
            print sys.exc_info()
            traceback.print_exc()
            # print ''.join(Pyro.util.getPyroTraceback(x))
            print "Couldn't find",i

    Jobs.helpers = helperbees
    if len(helperbees)>0:
        startThread(daemon)
    else:
        killDaemon()

