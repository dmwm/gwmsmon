#!/usr/bin/python
""" General functions which can be reused in all views """
import os
import sys
import json
import optparse
import ConfigParser
import rrdtool
import datetime
import rrdtool
import urllib2
import htcondor

def parseArgs():
    """TODO: move to one function... """
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", help="Prodview configuration file", dest="config", default=None)
    parser.add_option("-p", "--pool", help="HTCondor pool to analyze", dest="pool")
    parser.add_option("-o", "--output", help="Top-level output dir", dest="output")
    opts, args = parser.parse_args()

    opts.inputd = ""
    opts.timespan = ""

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

    if cp.has_option("prodview", "basedir"):
        opts.outputp = cp.get("prodview", "basedir")

    if cp.has_option("analysiscrab2view", "basedir"):
        opts.outputc2 = cp.get("analysiscrab2view", "basedir")

    if cp.has_option("analysisview", "basedir"):
        opts.outputc3 = cp.get("analysisview", "basedir")

    if cp.has_option("totalview", "basedir"):
        opts.inputd = cp.get("totalview", "basedir")

    if cp.has_option("utilization", "timespan"):
        opts.timespan = int(cp.get("utilization", "timespan"))

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


def updateRrd(fname, line):
    """Update rrd file
    fname -> full file path with filename
    line -> rrd line for update"""
    try:
        rrdtool.update(fname, line)
    except rrdtool.error as e:
        print e
        print fname
        print line


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
