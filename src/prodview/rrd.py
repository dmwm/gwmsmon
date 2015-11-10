
import os
import tempfile

import rrdtool

def get_rrd_interval(interval):
    if interval == "hourly":
        rrd_interval = "h"
    elif interval == "daily":
        rrd_interval = "d"
    elif interval == "weekly":
        rrd_interval = "w"
    elif interval == "monthly":
        rrd_interval = "m"
    elif interval == "yearly":
        rrd_interval = "y"
    else:
        raise ValueError("Unknown interval: %s" % interval)
    return rrd_interval

def clean_and_return(fd, pngpath):
    try:
        os.unlink(pngpath)
    finally:
        return os.fdopen(fd).read()

def subtask_site(basedir, interval, request, subtask, site):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, request, subtask, "%s.rrd" % site)
    if not os.path.exists(fname):
        raise ValueError("No information present (request=%s, subtask=%s, site=%s)" % (request, subtask, site))
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "%s Job Counts" % site,
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:MatchingIdle=%s:MatchingIdle:AVERAGE" % fname,
            "LINE1:Running#000000:Running",
            "LINE2:MatchingIdle#FF0000:MatchingIdle",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:                max     avg     cur\\n",
            "COMMENT:Running      ",
            "GPRINT:Running:MAX:%-6.0lf",
            "GPRINT:Running:AVERAGE:%-6.0lf",
            "GPRINT:Running:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:MatchingIdle ",
            "GPRINT:MatchingIdle:MAX:%-6.0lf",
            "GPRINT:MatchingIdle:AVERAGE:%-6.0lf",
            "GPRINT:MatchingIdle:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)


def subtask(basedir, interval, request, subtask):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, request, subtask, "subtask.rrd")
    if not os.path.exists(fname):
        raise ValueError("No information present (request=%s, subtask=%s)" % (request, subtask))
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "Subtask %s Job Counts" % subtask,
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:Idle=%s:Idle:AVERAGE" % fname,
            "LINE1:Running#000000:Running",
            "LINE2:Idle#FF0000:Idle",
            "COMMENT:%s" % subtask,
            "COMMENT:\\n",
            "COMMENT:           max     avg     cur\\n",
            "COMMENT:Running ",
            "GPRINT:Running:MAX:%-6.0lf",
            "GPRINT:Running:AVERAGE:%-6.0lf",
            "GPRINT:Running:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Idle    ",
            "GPRINT:Idle:MAX:%-6.0lf",
            "GPRINT:Idle:AVERAGE:%-6.0lf",
            "GPRINT:Idle:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)


def request(basedir, interval, request):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, request, "request.rrd")
    if not os.path.exists(fname):
        raise ValueError("No information present (request=%s)" % request)
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "Request %s Job Counts" % request,
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:Idle=%s:Idle:AVERAGE" % fname,
            "LINE1:Running#000000:Running",
            "LINE2:Idle#FF0000:Idle",
            "COMMENT:Request Statistics",
            "COMMENT:\\n",
            "COMMENT:           max     avg     cur\\n",
            "COMMENT:Running ",
            "GPRINT:Running:MAX:%-6.0lf",
            "GPRINT:Running:AVERAGE:%-6.0lf",
            "GPRINT:Running:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Idle    ",
            "GPRINT:Idle:MAX:%-6.0lf",
            "GPRINT:Idle:AVERAGE:%-6.0lf",
            "GPRINT:Idle:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)


def request_starvation(basedir, interval, request):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, request, "request.rrd")
    if not os.path.exists(fname):
        raise ValueError("No starvation information present (request=%s)" % request)
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "Request %s Starvation" % request,
            "DEF:LowerPrioRunning=%s:LowerPrioRunning:AVERAGE" % fname,
            "DEF:HigherPrioIdle=%s:HigherPrioIdle:AVERAGE" % fname,
            "LINE1:LowerPrioRunning#000000:LowerPrioRunning",
            "LINE2:HigherPrioIdle#FF0000:HigherPrioIdle",
            "COMMENT:\\n",
            "COMMENT:                       max     avg     cur\\n",
            "COMMENT:LowerPrioRunning    ",
            "GPRINT:LowerPrioRunning:MAX:%-6.0lf",
            "GPRINT:LowerPrioRunning:AVERAGE:%-6.0lf",
            "GPRINT:LowerPrioRunning:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:HigherPrioIdle      ",
            "GPRINT:HigherPrioIdle:MAX:%-6.0lf",
            "GPRINT:HigherPrioIdle:AVERAGE:%-6.0lf",
            "GPRINT:HigherPrioIdle:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)


def request_site(basedir, interval, request, site):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, request, "%s.rrd" % site)
    if not os.path.exists(fname):
        raise ValueError("No information present (request=%s, site=%s)" % (request, site))
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "%s Job Counts" % site,
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:MatchingIdle=%s:MatchingIdle:AVERAGE" % fname,
            "LINE1:Running#000000:Running",
            "LINE2:MatchingIdle#FF0000:MatchingIdle",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:                max     avg     cur\\n",
            "COMMENT:Running      ",
            "GPRINT:Running:MAX:%-6.0lf",
            "GPRINT:Running:AVERAGE:%-6.0lf",
            "GPRINT:Running:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:MatchingIdle ",
            "GPRINT:MatchingIdle:MAX:%-6.0lf",
            "GPRINT:MatchingIdle:AVERAGE:%-6.0lf",
            "GPRINT:MatchingIdle:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)

def site(basedir, interval, site):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, "%s.rrd" % site)
    if not os.path.exists(fname):
        fname = os.path.join(basedir, "empty.rrd")
        if not os.path.exists(fname):
            raise ValueError("No information present (site=%s)" % site)
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "%s Job Counts" % site,
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:MatchingIdle=%s:MatchingIdle:AVERAGE" % fname,
            "LINE1:Running#000000:Running",
            "LINE2:MatchingIdle#FF0000:MatchingIdle",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:                max     avg     cur\\n",
            "COMMENT:Running      ",
            "GPRINT:Running:MAX:%-6.0lf",
            "GPRINT:Running:AVERAGE:%-6.0lf",
            "GPRINT:Running:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:MatchingIdle ",
            "GPRINT:MatchingIdle:MAX:%-6.0lf",
            "GPRINT:MatchingIdle:AVERAGE:%-6.0lf",
            "GPRINT:MatchingIdle:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)


def site_util(basedir, interval, site):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, "%s-UTIL.rrd" % site)
    if not os.path.exists(fname):
        fname = os.path.join(basedir, "empty.rrd")
        if not os.path.exists(fname):
            raise ValueError("No information present (site=%s)" % site)
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "%s Max Running Achieved" % site,
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:MaxRunning=%s:MaxRunning:AVERAGE" % fname,
            "LINE1:Running#000000:Running",
            "LINE2:MaxRunning#0000FF:MaxRunning",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:              max     avg     cur\\n",
            "COMMENT:Running    ",
            "GPRINT:Running:MAX:%-6.0lf",
            "GPRINT:Running:AVERAGE:%-6.0lf",
            "GPRINT:Running:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:MaxRunning ",
            "GPRINT:MaxRunning:MAX:%-6.0lf",
            "GPRINT:MaxRunning:AVERAGE:%-6.0lf",
            "GPRINT:MaxRunning:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)


def summary(basedir, interval):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, "summary.rrd")
    if not os.path.exists(fname):
        raise ValueError("No information present" % site)
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "Pool Summary",
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:Idle=%s:Idle:AVERAGE" % fname,
            "LINE1:Running#000000:Running",
            "LINE2:Idle#FF0000:Idle",
            "COMMENT:Pool Summary",
            "COMMENT:\\n",
            "COMMENT:           max     avg     cur\\n",
            "COMMENT:Running ",
            "GPRINT:Running:MAX:%-6.0lf",
            "GPRINT:Running:AVERAGE:%-6.0lf",
            "GPRINT:Running:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Idle    ",
            "GPRINT:Idle:MAX:%-6.0lf",
            "GPRINT:Idle:AVERAGE:%-6.0lf",
            "GPRINT:Idle:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)

def request_held(basedir, interval, request, site):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = ""
    if request:
        fname = os.path.join(basedir, site, "%s.rrd" % request)
    else:
        fname = os.path.join(basedir, "%s.rrd" % site)
    if not os.path.exists(fname):
        raise ValueError("No information present (request=%s, site=%s)" % (request, site))
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "%s Held Counts" % site,
            "DEF:Held=%s:Held:AVERAGE" % fname,
            "DEF:MaxHeld=%s:MaxHeld:AVERAGE" % fname,
            "LINE1:Held#FF0000:Held",
            "LINE2:MaxHeld#0000FF:MaxHeld",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:           max     avg     cur\\n",
            "COMMENT:Held    ",
            "GPRINT:Held:MAX:%-6.0lf",
            "GPRINT:Held:AVERAGE:%-6.0lf",
            "GPRINT:Held:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:MaxHeld ",
            "GPRINT:MaxHeld:MAX:%-6.0lf",
            "GPRINT:MaxHeld:AVERAGE:%-6.0lf",
            "GPRINT:MaxHeld:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)

def request_idle(basedir, interval, request, site):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = ""
    if request:
        fname = os.path.join(basedir, site, "%s.rrd" % request)
    else:
        fname = os.path.join(basedir, "%s.rrd" % site)
    if not os.path.exists(fname):
        raise ValueError("No information present (request=%s, site=%s)" % (request, site))
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "%s Idle Counts" % site,
            "DEF:Idle=%s:Idle:AVERAGE" % fname,
            "DEF:MaxIdle=%s:MaxIdle:AVERAGE" % fname,
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "LINE1:Idle#FFFF00:Idle",
            "LINE2:MaxIdle#0000FF:MaxIdle",
            "LINE2:Running#000000:Running",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:           max     avg     cur\\n",
            "COMMENT:Idle    ",
            "GPRINT:Idle:MAX:%-6.0lf",
            "GPRINT:Idle:AVERAGE:%-6.0lf",
            "GPRINT:Idle:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:MaxIdle ",
            "GPRINT:MaxIdle:MAX:%-6.0lf",
            "GPRINT:MaxIdle:AVERAGE:%-6.0lf",
            "GPRINT:MaxIdle:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Running ",
            "GPRINT:Running:MAX:%-6.0lf",
            "GPRINT:Running:AVERAGE:%-6.0lf",
            "GPRINT:Running:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)


def request_joint(basedir, interval, request, site):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = ""
    if request:
        fname = os.path.join(basedir, site, "%s.rrd" % request)
    else:
        fname = os.path.join(basedir, "%s.rrd" % site)
    if not os.path.exists(fname):
        raise ValueError("No information present (request=%s, site=%s)" % (request, site))
    rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "%s Idle Counts" % site,
            "DEF:Idle=%s:Idle:AVERAGE" % fname,
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:MaxHeld=%s:MaxHeld:AVERAGE" % fname,
            "DEF:Held=%s:Held:AVERAGE" % fname,
            "LINE1:Idle#FFFF00:Idle",
            "LINE2:Running#000000:Running",
            "LINE2:MaxHeld#0000FF:MaxHeld",
            "LINE2:Held#FF0000:Held",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:           max     avg     cur\\n",
            "COMMENT:Idle    ",
            "GPRINT:Idle:MAX:%-6.0lf",
            "GPRINT:Idle:AVERAGE:%-6.0lf",
            "GPRINT:Idle:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Running ",
            "GPRINT:Running:MAX:%-6.0lf",
            "GPRINT:Running:AVERAGE:%-6.0lf",
            "GPRINT:Running:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:MaxHeld ",
            "GPRINT:MaxHeld:MAX:%-6.0lf",
            "GPRINT:MaxHeld:AVERAGE:%-6.0lf",
            "GPRINT:MaxHeld:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Held    ",
            "GPRINT:Held:MAX:%-6.0lf",
            "GPRINT:Held:AVERAGE:%-6.0lf",
            "GPRINT:Held:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)

def pilot_graph(basedir, interval, site, gType):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, "%s-USAGE.rrd" % site)
    if not os.path.exists(fname):
        fname = os.path.join(basedir, "empty.rrd")
        if not os.path.exists(fname):
            raise ValueError("No information present (site=%s)" % site)
    if gType == 'static':
        rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Static pilots",
            "--lower-limit", "0",
            "--title", "%s Static Pilots Counts" % site,
            "DEF:StatRunning=%s:StatRunning:AVERAGE" % fname,
            "DEF:StatIdle=%s:StatIdle:AVERAGE" % fname,
            "LINE1:StatRunning#000000:StatRunning",
            "LINE2:StatIdle#FF0000:StatIdle",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:           max     avg     cur\\n",
            "COMMENT:Running ",
            "GPRINT:StatRunning:MAX:%-6.0lf",
            "GPRINT:StatRunning:AVERAGE:%-6.0lf",
            "GPRINT:StatRunning:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Idle    ",
            "GPRINT:StatIdle:MAX:%-6.0lf",
            "GPRINT:StatIdle:AVERAGE:%-6.0lf",
            "GPRINT:StatIdle:LAST:%-6.0lf\\n",
            )
    elif gType == 'partitionable':
        rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Partitionable cpus",
            "--lower-limit", "0",
            "--title", "%s Partitionable Cpus Count" % site,
            "DEF:PartRunning=%s:PartRunning:AVERAGE" % fname,
            "DEF:PartIdle=%s:PartIdle:AVERAGE" % fname,
            "LINE1:PartRunning#000000:PartCpusUse",
            "LINE2:PartIdle#FF0000:PartIdle",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:           max     avg     cur\\n",
            "COMMENT:In Use  ",
            "GPRINT:PartRunning:MAX:%-6.0lf",
            "GPRINT:PartRunning:AVERAGE:%-6.0lf",
            "GPRINT:PartRunning:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Idle    ",
            "GPRINT:PartIdle:MAX:%-6.0lf",
            "GPRINT:PartIdle:AVERAGE:%-6.0lf",
            "GPRINT:PartIdle:LAST:%-6.0lf\\n",
            )
    elif gType == 'full':
        rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Partitionable Cpus",
            "--lower-limit", "0",
            "--title", "%s All Pilots Count" % site,
            "DEF:PartRunning=%s:PartRunning:AVERAGE" % fname,
            "DEF:PartIdle=%s:PartIdle:AVERAGE" % fname,
            "DEF:StatRunning=%s:StatRunning:AVERAGE" % fname,
            "DEF:StatIdle=%s:StatIdle:AVERAGE" % fname,
            "LINE1:PartRunning#000000:PartCpusUse",
            "LINE2:PartIdle#FF0000:PartIdle",
            "LINE3:StatRunning#00FF:StatRunning",
            "LINE4:StatIdle#FFFF00:StatIdle",
            "COMMENT:%s" % site,
            "COMMENT:\\n",
            "COMMENT:                          max     avg     cur\\n",
            "COMMENT:Partitionable Cpus used",
            "GPRINT:PartRunning:MAX:%-6.0lf",
            "GPRINT:PartRunning:AVERAGE:%-6.0lf",
            "GPRINT:PartRunning:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Partitionable Cpus Idle",
            "GPRINT:PartIdle:MAX:%-6.0lf",
            "GPRINT:PartIdle:AVERAGE:%-6.0lf",
            "GPRINT:PartIdle:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Static Running         ",
            "GPRINT:StatRunning:MAX:%-6.0lf",
            "GPRINT:StatRunning:AVERAGE:%-6.0lf",
            "GPRINT:StatRunning:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:Static Idle            ",
            "GPRINT:StatIdle:MAX:%-6.0lf",
            "GPRINT:StatIdle:AVERAGE:%-6.0lf",
            "GPRINT:StatIdle:LAST:%-6.0lf\\n",
            )
    else:
        raise ValueError("No information present (site=%s type=%s)" % (site, gType))
    return clean_and_return(fd, pngpath)

