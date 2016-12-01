import os
import tempfile

import rrdtool

COLORS = {"Idle": "#A52A2A",
        "MatchingIdle": "#A52A2A",
        "Running": "#000000",
        "MaxHeld": "#0000FF",
        "Held": "#FF0000",
        "CpusUse": "#0000FF",
        "CpusPen": "#FF0000",
        "MaxCpusUse": "#FF0000",
        "MaxRunning": "#008000",
        "PartCpusUse": "#0000FF",
        "PartIdle": "#FF0000",
        "StatRunning": "#000000",
        "StatIdle": "#FFFF00",
        "NegTime": "#000000",
        "Ideally": "#0000FF",
        "DiffCurrent": "#000000",
        "0line": "#0000FF"}

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
            "DEF:CpusUse=%s:Running:AVERAGE" % fname,
            "DEF:CpusPen=%s:Running:AVERAGE" % fname,
            "LINE1:Running%s:Running" % COLORS['Running'],
            "LINE2:MatchingIdle%s:MatchingIdle" % COLORS['MatchingIdle'],
            "LINE3:CpusUse%s:CpusUse" % COLORS['CpusUse'],
            "LINE4:CpusPen%s:CpusPen" % COLORS['CpusPen'],
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
            "COMMENT:CpusUse ",
            "GPRINT:CpusUse:MAX:%-6.0lf",
            "GPRINT:CpusUse:AVERAGE:%-6.0lf",
            "GPRINT:CpusUse:LAST:%-6.0lf\\n",
            "COMMENT:CpusPen ",
            "GPRINT:CpusPen:MAX:%-6.0lf",
            "GPRINT:CpusPen:AVERAGE:%-6.0lf",
            "GPRINT:CpusPen:LAST:%-6.0lf\\n",
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
            "DEF:CpusUse=%s:CpusUse:AVERAGE" % fname,
            "DEF:CpusPen=%s:CpusPen:AVERAGE" % fname,
            "LINE1:Running%s:Running" % COLORS['Running'],
            "LINE2:Idle%s:Idle" % COLORS['Idle'],
            "LINE3:CpusUse%s:CpusUse"% COLORS['CpusUse'],
            "LINE4:CpusPen%s:CpusPen" % COLORS['CpusPen'],
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
            "COMMENT:CpusUse ",
            "GPRINT:CpusUse:MAX:%-6.0lf",
            "GPRINT:CpusUse:AVERAGE:%-6.0lf",
            "GPRINT:CpusUse:LAST:%-6.0lf\\n",
            "COMMENT:CpusPen ",
            "GPRINT:CpusPen:MAX:%-6.0lf",
            "GPRINT:CpusPen:AVERAGE:%-6.0lf",
            "GPRINT:CpusPen:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)

def priority_summary_graph(basedir, interval, jobType, siteName = None):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = ""
    if siteName:
        fname = os.path.join(basedir, "priorities-%s-%s.rrd" % (siteName.lower(), jobType.lower()))
    else:
        fname = os.path.join(basedir, "priorities-%s.rrd" % jobType.lower())
    if not os.path.exists(fname):
        raise ValueError("No information present" % site)
    rrdtool.graph(pngpath,
                  "--imgformat", "PNG",
                  "--width", "250",
                  "--start", "-1%s" % get_rrd_interval(interval),
                  "--vertical-label", "Jobs",
                  "--lower-limit", "0",
                  "--title", "%s Job Priority" % jobType if not siteName else "%s at %s" % (jobType, siteName),
                  'DEF:R0=%s:R0:AVERAGE' % fname,
                  'DEF:R1=%s:R1:AVERAGE' % fname,
                  'DEF:R2=%s:R2:AVERAGE' % fname,
                  'DEF:R3=%s:R3:AVERAGE' % fname,
                  'DEF:R4=%s:R4:AVERAGE' % fname,
                  'DEF:R5=%s:R5:AVERAGE' % fname,
                  'DEF:R6=%s:R6:AVERAGE' % fname,
                  'DEF:R7=%s:R7:AVERAGE' % fname,
                  'AREA:R0#ff0000:High Priority',
                  'AREA:R1#ff7f00:Block 1 (110k):STACK',
                  'AREA:R2#ffff00:Block 2 (90k):STACK',
                  'AREA:R3#00ff00:Block 3 (85k):STACK',
                  'AREA:R4#0000ff:Block 4 (80k):STACK',
                  'AREA:R5#6600ff:Block 5 (70k):STACK',
                  'AREA:R6#8800ff:Block 6 (63k):STACK',
                  'AREA:R7#000000:Low Priority:STACK')
    return clean_and_return(fd, pngpath)

def oldrequest(basedir, interval, request):
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
            "LINE1:Running%s:Running" % COLORS['Running'],
            "LINE2:Idle%s:Idle" % COLORS['Idle'],
            "COMMENT:Request Statistics",
            "COMMENT:\\n",
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
            "DEF:CpusUse=%s:CpusUse:AVERAGE" % fname,
            "DEF:CpusPen=%s:CpusPen:AVERAGE" % fname,
            "LINE1:Running%s:Running" % COLORS['Running'],
            "LINE2:Idle%s:Idle" % COLORS['Idle'],
            "LINE3:CpusUse%s:CpusUse" % COLORS['CpusUse'],
            "LINE4:CpusPen%s:CpusPen" % COLORS['CpusPen'],
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
            "COMMENT:CpusUse ",
            "GPRINT:CpusUse:MAX:%-6.0lf",
            "GPRINT:CpusUse:AVERAGE:%-6.0lf",
            "GPRINT:CpusUse:LAST:%-6.0lf\\n",
            "COMMENT:CpusPen ",
            "GPRINT:CpusPen:MAX:%-6.0lf",
            "GPRINT:CpusPen:AVERAGE:%-6.0lf",
            "GPRINT:CpusPen:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)

def scheddwarning(basedir, interval, request):
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
                  "--title", "Scheduler %s status" % request,
                  "DEF:OK=%s:OK:AVERAGE" % fname,
                  "DEF:WARNING=%s:WARNING:AVERAGE" % fname,
                  "DEF:CRITICAL=%s:CRITICAL:AVERAGE" % fname,
                  "DEF:UNKNOWN=%s:UNKNOWN:AVERAGE" % fname,
                  "AREA:OK#00FF00:OK",
                  "AREA:WARNING#FFFF00:WARNING",
                  "AREA:CRITICAL#FF0000:CRITICAL",
                  "AREA:UNKNOWN#000000:UNKNOWN",
                  "COMMENT:\\n",
                  "COMMENT:              cur\\n",
                  "COMMENT:OK         ",
                  "GPRINT:OK:LAST:%-6.0lf",
                  "COMMENT:\\n",
                  "COMMENT:WARNING    ",
                  "GPRINT:WARNING:LAST:%-6.0lf",
                  "COMMENT:\\n",
                  "COMMENT:CRITICAL   ",
                  "GPRINT:CRITICAL:LAST:%-6.0lf",
                  "COMMENT:\\n",
                  "COMMENT:UNKNOWN    ",
                  "GPRINT:UNKNOWN:LAST:%-6.0lf\\n",
                  )
    return clean_and_return(fd, pngpath)


def dagmans(basedir, interval, request):
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
                  "--title", "Dagmans on %s Counts" % request,
                  "DEF:MaxDagmans=%s:MaxDagmans:AVERAGE" % fname,
                  "DEF:RunningDagmans=%s:RunningDagmans:AVERAGE" % fname,
                  "DEF:IdleDagmans=%s:IdleDagmans:AVERAGE" % fname,
                  "LINE1:MaxDagmans#FF0000:MaxDagmans",
                  "LINE2:RunningDagmans#00FF00:RunningDagmans",
                  "LINE3:IdleDagmans#0000FF:IdleDagmans",
                  "COMMENT:Dagmans Statistics",
                  "COMMENT:\\n",
                  "COMMENT:                  max     avg     cur\\n",
                  "COMMENT:MaxDagmans     ",
                  "GPRINT:MaxDagmans:MAX:%-6.0lf",
                  "GPRINT:MaxDagmans:AVERAGE:%-6.0lf",
                  "GPRINT:MaxDagmans:LAST:%-6.0lf",
                  "COMMENT:\\n",
                  "COMMENT:RunningDagmans ",
                  "GPRINT:RunningDagmans:MAX:%-6.0lf",
                  "GPRINT:RunningDagmans:AVERAGE:%-6.0lf",
                  "GPRINT:RunningDagmans:LAST:%-6.0lf",
                  "COMMENT:\\n",
                  "COMMENT:IdleDagmans    ",
                  "GPRINT:IdleDagmans:MAX:%-6.0lf",
                  "GPRINT:IdleDagmans:AVERAGE:%-6.0lf",
                  "GPRINT:IdleDagmans:LAST:%-6.0lf\\n",
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
            "DEF:CpusUse=%s:CpusUse:AVERAGE" % fname,
            "DEF:CpusPen=%s:CpusPen:AVERAGE" % fname,
            "LINE1:Running%s:Running" % COLORS['Running'],
            "LINE2:MatchingIdle%s:MatchingIdle" % COLORS['MatchingIdle'],
            "LINE3:CpusUse%s:CpusUse" % COLORS['CpusUse'],
            "LINE4:CpusPen%s:CpusPen" % COLORS['CpusPen'],
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
            "COMMENT:CpusUse ",
            "GPRINT:CpusUse:MAX:%-6.0lf",
            "GPRINT:CpusUse:AVERAGE:%-6.0lf",
            "GPRINT:CpusUse:LAST:%-6.0lf\\n",
            "COMMENT:CpusPen ",
            "GPRINT:CpusPen:MAX:%-6.0lf",
            "GPRINT:CpusPen:AVERAGE:%-6.0lf",
            "GPRINT:CpusPen:LAST:%-6.0lf\\n",
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
            "DEF:CpusUse=%s:CpusUse:AVERAGE" % fname,
            "DEF:CpusPen=%s:CpusPen:AVERAGE" % fname,
            "LINE1:Running%s:Running" % COLORS['Running'],
            "LINE2:MatchingIdle%s:MatchingIdle" % COLORS['MatchingIdle'],
            "LINE3:CpusUse%s:CpusUse" % COLORS['CpusUse'],
            "LINE4:CpusPen%s:CpusPen" % COLORS['CpusPen'],
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
            "COMMENT:CpusUse ",
            "GPRINT:CpusUse:MAX:%-6.0lf",
            "GPRINT:CpusUse:AVERAGE:%-6.0lf",
            "GPRINT:CpusUse:LAST:%-6.0lf\\n",
            "COMMENT:CpusPen ",
            "GPRINT:CpusPen:MAX:%-6.0lf",
            "GPRINT:CpusPen:AVERAGE:%-6.0lf",
            "GPRINT:CpusPen:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)


def site_fair(basedir, interval, site):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, "%s-FAIRSHARE.rrd" % site)
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
            "--title", "%s Fairshare" % site,
            "DEF:CpusInUseProd=%s:CpusInUseProd:AVERAGE" % fname,
            "DEF:CpusInUseAna=%s:CpusInUseAna:AVERAGE" % fname,
            "AREA:CpusInUseProd%s:CpusInUseProd" % COLORS['MaxRunning'],
            "AREA:CpusInUseAna%s:CpusInUseAna:STACK" % "#ff0000",
            "COMMENT:\\n",
            "COMMENT:                   max     avg     cur\\n",
            "COMMENT:CpusInUseProd    ",
            "GPRINT:CpusInUseProd:MAX:%-6.0lf",
            "GPRINT:CpusInUseProd:AVERAGE:%-6.0lf",
            "GPRINT:CpusInUseProd:LAST:%-6.0lf",
            "COMMENT:\\n",
            "COMMENT:CpusInUseAna     ",
            "GPRINT:CpusInUseAna:MAX:%-6.0lf",
            "GPRINT:CpusInUseAna:AVERAGE:%-6.0lf",
            "GPRINT:CpusInUseAna:LAST:%-6.0lf\\n",
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
            "DEF:CpusUse=%s:CpusUse:AVERAGE" % fname,
            "DEF:MaxCpusUse=%s:MaxCpusUse:AVERAGE" % fname,
            "LINE1:Running%s:Running" % COLORS['Running'],
            "LINE2:MaxRunning%s:MaxRunning" % COLORS['MaxRunning'],
            "LINE3:CpusUse%s:CpusUse" % COLORS['CpusUse'],
            "LINE4:MaxCpusUse%s:MaxCpusUse" % COLORS['MaxCpusUse'],
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
            "COMMENT:CpusUse ",
            "GPRINT:CpusUse:MAX:%-6.0lf",
            "GPRINT:CpusUse:AVERAGE:%-6.0lf",
            "GPRINT:CpusUse:LAST:%-6.0lf\\n",
            "COMMENT:MaxCpusUse ",
            "GPRINT:MaxCpusUse:MAX:%-6.0lf",
            "GPRINT:MaxCpusUse:AVERAGE:%-6.0lf",
            "GPRINT:MaxCpusUse:LAST:%-6.0lf\\n",
            )
    return clean_and_return(fd, pngpath)


def summary(basedir, interval, fileName):
    fd, pngpath = tempfile.mkstemp(".png")
    fname = os.path.join(basedir, "%s.rrd" % fileName)
    if fileName == 'oldsummary':
        fname = os.path.join(basedir, "summary.rrd")
    if not os.path.exists(fname):
        raise ValueError("No information present %s" % fname)
    if fileName == 'oldsummary':
        rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "Pool Summary",
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:Idle=%s:Idle:AVERAGE" % fname,
            "LINE1:Running%s:Running" % COLORS['Running'],
            "LINE2:Idle%s:Idle" % COLORS['Idle'],
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
    elif fileName == 'summary':
        rrdtool.graph(pngpath,
            "--imgformat", "PNG",
            "--width", "250",
            "--start", "-1%s" % get_rrd_interval(interval),
            "--vertical-label", "Jobs",
            "--lower-limit", "0",
            "--title", "Pool Summary",
            "DEF:Running=%s:Running:AVERAGE" % fname,
            "DEF:Idle=%s:Idle:AVERAGE" % fname,
            "DEF:CpusUse=%s:CpusUse:AVERAGE" % fname,
            "DEF:CpusPen=%s:CpusPen:AVERAGE" % fname,
            "LINE1:Running%s:Running" % COLORS['Running'],
            "LINE2:Idle%s:Idle" % COLORS['Idle'],
            "LINE3:CpusUse%s:CpusUse" % COLORS['CpusUse'],
            "LINE4:CpusPen%s:CpusPen" % COLORS['CpusPen'],
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
            "COMMENT:CpusUse ",
            "GPRINT:CpusUse:MAX:%-6.0lf",
            "GPRINT:CpusUse:AVERAGE:%-6.0lf",
            "GPRINT:CpusUse:LAST:%-6.0lf\\n",
            "COMMENT:CpusPen ",
            "GPRINT:CpusPen:MAX:%-6.0lf",
            "GPRINT:CpusPen:AVERAGE:%-6.0lf",
            "GPRINT:CpusPen:LAST:%-6.0lf\\n",
                )
## TODO
    elif fileName == 'negotiation':
        rrdtool.graph(pngpath,
                "--imgformat", "PNG",
             "--width", "250",
             "--start", "-1%s" % get_rrd_interval(interval),
             "--vertical-label", "Jobs",
             "--lower-limit", "0",
             "--title", "Negotiation time",
             "DEF:NegotiationTime=%s:NegotiationTime:AVERAGE" % fname,
             "DEF:Ideally=%s:Ideally:AVERAGE" % fname,
             "LINE1:NegotiationTime#000000:NegotiationTime",
             "LINE2:Ideally#0000FF:Ideally",
             "COMMENT:\\n",
             "COMMENT:                   max     avg     cur\\n",
             "COMMENT:Negotiation Time ",
             "GPRINT:NegotiationTime:MAX:%-6.0lf",
             "GPRINT:NegotiationTime:AVERAGE:%-6.0lf",
             "GPRINT:NegotiationTime:LAST:%-6.0lf",
             "COMMENT:\\n",
             "COMMENT:Ideally          ",
             "GPRINT:Ideally:MAX:%-6.0lf",
             "GPRINT:Ideally:AVERAGE:%-6.0lf",
             "GPRINT:Ideally:LAST:%-6.0lf\\n",
             )
## TODO
    elif fileName == 'difference':
        rrdtool.graph(pngpath,
             "--imgformat", "PNG",
             "--width", "250",
             "--start", "-1%s" % get_rrd_interval(interval),
             "--vertical-label", "Jobs",
             "--lower-limit", "0",
             "--title", "Difference between collectors",
             "DEF:Current=%s:Current:AVERAGE" % fname,
             "LINE1:Current#000000:Current",
             "LINE2:0#0000FF:0",
             "COMMENT:\\n",
             "COMMENT:              max     avg     cur\\n",
             "COMMENT:Difference  ",
             "GPRINT:Current:MAX:%-6.0lf",
             "GPRINT:Current:AVERAGE:%-6.0lf",
             "GPRINT:Current:LAST:%-6.0lf\\n",
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
            "LINE1:Held%s:Held" % COLORS['Held'],
            "LINE2:MaxHeld%s:MaxHeld" % COLORS['MaxHeld'],
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
            "LINE1:Idle#A52A2A:Idle",
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
            "LINE1:Idle%s:Idle" % COLORS['Idle'],
            "LINE2:Running%s:Running" % COLORS['Running'],
            "LINE2:MaxHeld%s:MaxHeld" % COLORS['MaxHeld'],
            "LINE2:Held%s:Held" % COLORS['Held'],
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

