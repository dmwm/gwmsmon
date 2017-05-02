#!/usr/bin/python
""" General functions which can be reused in all views """
import os
import sys
import json
import optparse
import ConfigParser
import rrdtool
import urllib2
import htcondor
import datetime
import time
import subprocess
import signal
from timeout import timeout


class TimeoutError1(Exception):
     pass
 
class timeout1:
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError1(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


def parseArgs():
    """ parse all arguments from config file. """
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", help="Prodview configuration file", dest="config", default=None)
    parser.add_option("-p", "--pool", help="HTCondor pool to analyze", dest="pool")
    parser.add_option("-o", "--output", help="Top-level output dir", dest="output")
    parser.add_option("-t", "--type", help="Type which to execute", dest="type")
    opts, args = parser.parse_args()
    keys = [{"key": "prodview", "subkey": "basedir", "variable": "prodview"},
            {"key": "prodview", "subkey": "historyhours", "variable": "historyhoursprod"},
            {"key": "analysisview", "subkey": "basedir", "variable": "analysisview"},
            {"key": "analysisview", "subkey": "historyhours", "variable": "historyhoursanal"},
            {"key": "cmsconnectview", "subkey": "basedir", "variable": "cmsconnectview"},
            {"key": "institutionalview", "subkey": "basedir", "variable": "institutionalview"},
            {"key": "totalview", "subkey": "basedir", "variable": "totalview"},
            {"key": "poolview", "subkey": "basedir", "variable": "poolview"},
            {"key": "factoryview", "subkey": "basedir", "variable": "factoryview"},
            {"key": "htcondor", "subkey": "pool", "variable": "pool"},
            {"key": "htcondor", "subkey": "pool1", "variable": "pool1"},
            {"key": "utilization", "subkey": "timespan", "variable": "utilization"},
            {"key": "elasticserver", "subkey": "baseurl", "variable": "elasticserver"}]
    if args:
        parser.print_help()
        print >> sys.stderr, "%s takes no arguments." % args[0]
        sys.exit(1)

    cp = ConfigParser.ConfigParser()
    if opts.config:
        if not os.path.exists(opts.config):
            print >> sys.stderr, "Config file %s does not exist." % opts.config
            sys.exit(1)
        cp.read(opts.config)
    elif os.path.exists("/etc/prodview.conf"):
        cp.read("/etc/prodview.conf")
    for item in keys:
        if cp.has_option(item['key'], item['subkey']):
            setattr(opts, item['variable'], cp.get(item['key'], item['subkey']))

    return opts, args


def dropObj(obj, dirname, fname):
    """Write obj to specified directory
       obj -> dict/list
       dirname -> directory where to write
       fname -> filename
    """
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    fnameTmp = os.path.join(dirname, fname + ".tmp")
    fname = os.path.join(dirname, fname)
    json.dump(obj, open(fnameTmp, "w"))
    os.rename(fnameTmp, fname)

@timeout(30)
def getFromURL(url):
    """Get content from URL"""
    try:
        req = urllib2.Request(url)
        opener = urllib2.build_opener()
        f = opener.open(req)
        return f.read()
    except urllib2.URLError as er:
        print er
        return None


def updateRrdLine(fname, line):
    """Update rrd file
    fname -> full file path with filename
    line -> rrd line for update"""
    try:
        rrdtool.update(fname, line)
    except rrdtool.error as e:
        print 'Unable to update:', e, fname, line


def validateInt(numb):
    """ Validate integer which is used for ExprTree
    There was an issue with production not specifying memory
    numb -> value
    """
    try:
        dummyOut = int(numb)
        return numb
    except TypeError as er:
        del er
        return int(numb.eval())

def createEmptyRRD(output, startTime):
    """ Create fake empty.rrd for sites which have no data. """
    fname = str(os.path.join(output, "empty.rrd"))
    tempKeys = ["DS:Running:GAUGE:360:U:U", "DS:MatchingIdle:GAUGE:360:U:U", "DS:MaxRunning:GAUGE:360:U:U", "DS:CpusUse:GAUGE:360:U:U", "DS:CpusPen:GAUGE:360:U:U"]
    tempUpdLine = "%d:0:0:0:0:0" % startTime
    rrdUpdate(fname, tempKeys, tempUpdLine, startTime)

def querySchedd(ad, const, keys):
    """query schedd with const and keys
    ad -> ad got from collector
    const -> condor -const '*'
    keys -> which keys to take from classads"""
    output = []
    schedd = htcondor.Schedd(ad)
    try:
        output = schedd.xquery(const, keys)
    except Exception as e:
        # logging
        # Also it has to be saved and showed in the schedd view
        print "Failed querying", ad["Name"], e
    return output

def getCollectors(pool, pool1, main=False):
    """Get both collectors"""
    if main:
        coll = htcondor.Collector(pool.split(":")[0])
        coll1 = htcondor.Collector(pool1.split(":")[0])
        return coll, coll1
    else:
        coll =  htcondor.Collector(pool)
        coll1 = htcondor.Collector(pool1)
        return coll, coll1

def getSchedds(opts, pool, query, keys, split = False):
    """TODO doc"""
    scheddAds = None
    print 'Getting schedd names from: ', pool if not split else pool.split(":")[0]
    print time.time()
    try:
        with timeout1(10):
            if pool:
                if split:
                    coll = htcondor.Collector(pool.split(":")[0])
                else:
                    coll = htcondor.Collector(pool)
            else:
                coll = htcondor.Collector()
            try:
                scheddAds = coll.query(htcondor.AdTypes.Schedd, query, keys)
            except IOError as er:
                print 'Got IOError', er
    except TimeoutError1 as er:
        print 'Got error: %s' % er

    if not scheddAds:
        # This should not happen, if happens, means something wrong...
        if opts.pool1:
            scheddAds, coll = getSchedds(opts, opts.pool1, query, keys, split)
    return scheddAds, coll


def rrdUpdate(fname, createVars, updateLine, gstartup):
    """ TODO Doc """
    createVars = ["--step", "180", "--start", "%s" % str(gstartup - 180)] + createVars
    createVars += ["RRA:AVERAGE:0.5:1:1000", "RRA:AVERAGE:0.5:20:2000"]
    try:
        if not os.path.exists(fname):
            os.makedirs(os.path.dirname(fname))
    except OSError:
        pass
    if not os.path.exists(fname):
        rrdtool.create(fname, *createVars)
    updateRrdLine(fname, updateLine)


def roundTime(dt=None, roundTo=60):
    """Round a datetime object to any time laps in seconds
    dt : datetime.datetime object, default now.
    roundTo : Closest number of seconds to round to, default 1 minute.
    """
    dt = datetime.datetime.fromtimestamp(dt)
    if dt == None : dt = datetime.datetime.now()
    seconds = (dt - dt.min).seconds
    # // is a floor division, not a comment on following line:
    rounding = (seconds+roundTo/2) // roundTo * roundTo
    return int(time.mktime((dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)).timetuple()))


def queryCommandLineSchedd(collector, values):
    try:
        bashCommand = "condor_status -pool %s -schedd -af:n %s" % (collector, values)
        out, err = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE).communicate() 
        N = len(values.split(" "))
        outL = out.split("\n")
        subList = [outL[n:n+N] for n in range(0, len(outL), N)]
    except:
        return []
    return subList


def returnCorrectOut(inputD):
    correctOut = ""
    if 'responses' in inputD:
        if isinstance(inputD['responses'], list):
            correctOut = inputD['responses'][0]
        elif isinstance(inputD['responses'], dict):
            correctOut = inputD['responses']
        else:
            correctOut = inputD['responses']
    else:
        if isinstance(inputD, list):
            correctOut = inputD[0]
        elif isinstance(inputD, dict):
            correctOut = inputD
    return correctOut

def database_output_server(values, url):
    url = url + "/cms-*/_msearch?timeout=0&ignore_unavailable=true"
    command = "curl -v -XGET --compressed -k '%s' --data-binary $'%s'" % (url, str(values).replace("'", "\""))
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = p.communicate()
    p_status = p.wait()
    try:
        d = json.loads(output[0])
    except ValueError as er:
          return {}, er
    return returnCorrectOut(d), False
