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


def parseArgs():
    """ parse all arguments from config file. """
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", help="Prodview configuration file", dest="config", default=None)
    parser.add_option("-p", "--pool", help="HTCondor pool to analyze", dest="pool")
    parser.add_option("-o", "--output", help="Top-level output dir", dest="output")
    opts, args = parser.parse_args()
    keys = [{"key": "prodview", "subkey": "basedir", "variable": "prodview"},
            {"key": "analysiscrab2view", "subkey": "basedir", "variable": "analysiscrab2view"},
            {"key": "analysisview", "subkey": "basedir", "variable": "analysisview"},
            {"key": "totalview", "subkey": "basedir", "variable": "totalview"},
            {"key": "scheddview", "subkey": "basedir", "variable": "scheddview"},
            {"key": "factoryview", "subkey": "basedir", "variable": "factoryview"},
            {"key": "htcondor", "subkey": "pool", "variable": "pool"}, {"key": "htcondor", "subkey": "pool1", "variable": "pool1"},
            {"key": "utilization", "subkey": "timespan", "variable": "utilization"}]
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


def getSchedds(opts, pool, query, keys):
    """TODO doc"""
    if pool:
        coll = htcondor.Collector(pool)
    else:
        coll = htcondor.Collector()

    scheddAds = coll.query(htcondor.AdTypes.Schedd, query, keys)
    if not scheddAds:
        # This should not happen, if happens, means something wrong...
        if opts.pool1:
            scheddAds = getSchedds(opts, opts.pool1, query, keys)
    return scheddAds, coll
