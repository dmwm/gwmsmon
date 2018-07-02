#!/usr/bin/env python
"""
This is for counting jobs from each condor schedd
Creates the following files: CondorMonitoring.json, CondorJobs_Workflows.json, Running*.txt and Pending*.txt ( * in types )
"""
import sys
import os
import traceback
import re
import urllib2
import time
import smtplib
import htcondor as condor

from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

try:
    import json
except ImportError:
    import simplejson as json

# Mailing list for notifications
mailingSender = 'noreply@cern.ch'
mailingList = {'prodschedd': ['cms-comp-ops-workflow-team@cern.ch', 'justas.balcas@cern.ch'],
               'tier0schedd': ['cms-comp-ops-workflow-team@cern.ch'],
               'crabschedd': ['justas.balcas@cern.ch']}

## Job Collectors (Condor pools)
## Alan updated to alias on 20/Apr/2016
global_pool = ['vocms0815.cern.ch:9620']

# There is no point of querying T0 pool as schedds are also flocking to the Global pool
# After discussions with Antonio, it was expected to run Prompt-Reco on T1s and
# Both pools are returning same list of schedulers and jobs, so here comes a double counting
# tier0_pool = ['cmsgwms-collector-tier0.cern.ch']
# For now commenting out and leaving it for Antonio to discuss in SI meeting
tier0_pool = []

##The following groups should be updated according to https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsWorkflowTeamWmAgentRealeases
relvalAgents = ['vocms053.cern.ch', 'vocms026.cern.ch']
testAgents = ['cmssrv113.fnal.gov', 'vocms040.cern.ch', 'vocms009.cern.ch', 'vocms0224.cern.ch', 'vocms0230.cern.ch']

##Job expected types
jobTypes = ['Processing', 'Production', 'Skim', 'Harvest', 'Merge', 'LogCollect', 'Cleanup', 'RelVal', 'Express',
            'Repack', 'Reco', 'Crab3']

## Job counting / Condor monitoring
baseSiteList = {}  # Site list
baseSiteCapacity = {}  # List of sites and amount of cpu resources it provides
jobCounting = {}  # Actual job counting
pendingCache = {}  # pending jobs cache
pendingSites = []  # Unique list of pending sites
totalRunningSite = {}  # Total running per site
jobs_failedTypeLogic = {"prodschedd": {}, 'crabschedd': {}, 'tier0schedd': {}}  # Jobs that failed the type logic assignment
output_json = "CondorMonitoring.json"  # Output json file name
##Counting jobs for Workflows
overview_workflows = {}
json_name_workflows = "CondorJobs_Workflows.json"  # Output json file name

##SSB links
site_link = "http://dashb-ssb.cern.ch/dashboard/templates/sitePendingRunningJobs.html?site="
overalls_link = "http://dashb-ssb-dev.cern.ch/dashboard/templates/sitePendingRunningJobs.html?site=All%20"
url_site_status = 'http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=237&batch=1&lastdata=1'
url_site_capacity = 'http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?columnid=160&batch=1&lastdata=1'


def createSiteList():
    """
    Creates a initial site list with the data from site status in Dashboard
    """
    cachelocation = '/tmp/sitelist-ssb.txt'
    sites = ""
    try:
        sites = urllib2.urlopen(url_site_status).read()
        with open(cachelocation, 'w') as fd:
            fd.write(sites)
    except:
        if os.path.isfile(cachelocation):
            with open(cachelocation, 'r') as fd:
                sites = fd.read()
        else:
            raise Exception('Unable to get site list and cache file does not exist')
    try:
        site_status = json.read(sites)['csvdata']
    except:
        site_status = json.loads(sites)['csvdata']

    for site in site_status:
        name = site['VOName']
        status = site['Status']
        if siteName(name):
            baseSiteList[name] = status


def getSiteCapacity():
    """
    Get the expected sites CPU resources from Dashboard
    """
    cachelocation = '/tmp/sitecapacity-ssb.txt'
    sites = ""
    try:
        sites = urllib2.urlopen(url_site_capacity).read()
        with open(cachelocation, 'w') as fd:
            fd.write(sites)
    except:
        if os.path.isfile(cachelocation):
            with open(cachelocation, 'r') as fd:
                sites = fd.read()
        else:
            raise Exception('Unable to get site list and cache file does not exist')
    try:
        site_pledges = json.read(sites)['csvdata']
    except:
        site_pledges = json.loads(sites)['csvdata']

    for site in site_pledges:
        name = site['VOName']
        if site['Value'] == None:
            value = 0
        else:
            value = int(site['Value'])
        if siteName(name):
            baseSiteCapacity[name] = value


def initJobDictonaries():
    """
    Init running/pending jobs for each site in the baseSiteList
    """
    for site in baseSiteList.keys():
        if '_Disk' not in site:  # Avoid _Disk suffixes
            jobCounting[site] = {}


def siteName(candidate):
    """
    Check candidate as a site name. Should pass:
        T#_??_*
    Returns True if it is a site name
    """
    regexp = "^T[0-3%](_[A-Z]{2}(_[A-Za-z0-9]+)*)$"
    if re.compile(regexp).match(candidate) != None:
        return True
    else:
        return False


def addSite(site):
    """
    Add a site to all the dictionaries
    """
    print "DEBUG: Adding site %s to base lists" % site
    if site not in jobCounting.keys():
        jobCounting[site] = {}
    if site not in baseSiteList.keys():
        baseSiteList[site] = 'on'


def addSchedd(site, sched):
    """
    Add a schedd to all the dictionaries for a given site
    """
    if sched not in jobCounting[site].keys():
        jobCounting[site][sched] = {}
        for jType in jobTypes:
            jobCounting[site][sched][jType] = {}


def addCore(site, sched, jType, cores):
    """
    Add a Number of cores for a given site, schedd and type to all the dictionaries
    """
    if cores not in jobCounting[site][sched][jType].keys():
        jobCounting[site][sched][jType][cores] = {}
        for status in ['Running', 'Pending']:
            jobCounting[site][sched][jType][cores][status] = 0.0


def increaseRunning(site, sched, jType, cores):
    """
    Increase the number of running jobs for the given site, schedd, type and cores
    This always increase job count by 1
    """
    if site not in jobCounting.keys():
        addSite(site)
    if sched not in jobCounting[site].keys():
        addSchedd(site, sched)
    if cores not in jobCounting[site][sched][jType].keys():
        addCore(site, sched, jType, cores)
    # Now do the actual counting
    jobCounting[site][sched][jType][cores]['Running'] += 1


def increasePending(site, sched, jType, cores, num):
    """
    Increase the number of pending jobs for the given site and type
    This handles smart counting: sum the relative pending 'num'
    """
    if site not in jobCounting.keys():
        addSite(site)
    if sched not in jobCounting[site].keys():
        addSchedd(site, sched)
    if cores not in jobCounting[site][sched][jType].keys():
        addCore(site, sched, jType, cores)
    # Now do the actual counting
    jobCounting[site][sched][jType][cores]['Pending'] += num


def increaseRunningWorkflow(workflow, siteToExtract, cores):
    """
    Increases the number of running jobs per workflow
    """
    if workflow not in overview_workflows.keys():
        addWorkflow(workflow)
        if siteToExtract in overview_workflows[workflow]['runningJobs'].keys():
            overview_workflows[workflow]['runningJobs'][siteToExtract] += cores
            overview_workflows[workflow]['condorJobs'] += cores
        else:
            overview_workflows[workflow]['runningJobs'][siteToExtract] = cores
            overview_workflows[workflow]['condorJobs'] += cores
    else:
        if siteToExtract in overview_workflows[workflow]['runningJobs'].keys():
            overview_workflows[workflow]['runningJobs'][siteToExtract] += cores
            overview_workflows[workflow]['condorJobs'] += cores
        else:
            overview_workflows[workflow]['runningJobs'][siteToExtract] = cores
            overview_workflows[workflow]['condorJobs'] += cores


def increasePendingWorkflow(workflow, siteToExtract, cores):
    """
    Increases the number of pending jobs per workflow
    """
    if workflow not in overview_workflows.keys():
        addWorkflow(workflow)
        overview_workflows[workflow]['condorJobs'] += cores
        overview_workflows[workflow]['pendingJobs'] += cores
        overview_workflows[workflow]['desiredSites'] = overview_workflows[workflow]['desiredSites'].union(
            set(siteToExtract))
    else:
        overview_workflows[workflow]['condorJobs'] += cores
        overview_workflows[workflow]['pendingJobs'] += cores
        overview_workflows[workflow]['desiredSites'] = overview_workflows[workflow]['desiredSites'].union(
            set(siteToExtract))


def addWorkflow(workflow):
    """
    Add a new workflow to overview_workflows
    """
    overview_workflows[workflow] = {
        'condorJobs': 0,
        'runningJobs': {},
        'pendingJobs': 0,
        'desiredSites': set()
    }


def jobType(jobId, schedd, typeToExtract, schedd_type):
    """
    This deduces job type from given info about scheduler and taskName
    Only intended as a backup in case job type from the classAds is not standard
    """
    jType = ''
    if schedd in relvalAgents:
        jType = 'RelVal'
    if schedd.startswith('crab3'):
        jType = 'Crab3'
    elif 'Cleanup' in typeToExtract:
        jType = 'Cleanup'
    elif 'LogCollect' in typeToExtract:
        jType = 'LogCollect'
    elif 'harvest' in typeToExtract.lower():
        jType = 'Harvest'
    elif 'Merge' in typeToExtract:
        jType = 'Merge'
    elif 'skim' in typeToExtract.lower():
        jType = 'Skim'
    elif 'Express' in typeToExtract:
        jType = 'Express'
    elif 'Repack' in typeToExtract:
        jType = 'Repack'
    elif 'Reco' in typeToExtract:
        jType = 'Reco'
    elif 'Production' in typeToExtract or 'MonteCarloFromGEN' in typeToExtract:
        jType = 'Production'
    elif any([x in typeToExtract for x in ['Processing', 'StepOneProc', 'StepTwoProc', 'StepThreeProc']]):
        jType = 'Processing'
    elif 'StoreResults' in typeToExtract:
        jType = 'Merge'
    elif schedd in testAgents:
        jType = 'Processing'
    else:
        jType = 'Processing'
        jobs_failedTypeLogic[schedd_type][jobId] = dict(scheduler=schedd, BaseType=typeToExtract)
    return jType


def relativePending(siteToExtract):
    """
    Return the remaining slots available (in principle) to run jobs for the given sites
    If there is no slots available, ruturn the same value for all the given (same chance to run)
    """
    relative = {}
    total = 0.0
    for site in siteToExtract:
        if site in totalRunningSite.keys():
            running = totalRunningSite[site]
        else:
            running = 0.0
        if site in baseSiteCapacity.keys():
            pledge = baseSiteCapacity[site]
        else:
            pledge = 0.0

        relative[site] = pledge - running
        if relative[site] < 0.0:
            relative[site] = 0.0
        total += relative[site]

    # if total = 0, it means that there is not available slots for any site, set the same for all sites
    if total == 0.0:
        total = len(siteToExtract)
        for site in relative.keys():
            relative[site] = 1.0

    return relative, total


def getJobsOveralls():
    """
    This creates the overall job counting by site and server
    """
    totalBySite = {}
    totalByServer = {}
    totalByTask = {}
    totalJobs = {}

    totalJobs = {
        'Running': 0.0,
        'Pending': 0.0
    }
    for task in jobTypes:
        totalByTask[task] = {
            'Running': 0.0,
            'Pending': 0.0
        }
    for site in jobCounting.keys():
        # Add site to the overalls by site, then add each tasks
        totalBySite[site] = {
            'Running': 0.0,
            'Pending': 0.0
        }
        for task in jobTypes:
            totalBySite[site][task] = {
                'Running': 0.0,
                'Pending': 0.0
            }

        for schedd in jobCounting[site].keys():
            # If schedd is not in totalByServer, then add it
            if not schedd in totalByServer.keys():
                totalByServer[schedd] = {
                    'Running': 0.0,
                    'Pending': 0.0
                }
                for task in jobTypes:
                    totalByServer[schedd][task] = {
                        'Running': 0.0,
                        'Pending': 0.0
                    }
            for jType in jobCounting[site][schedd].keys():
                for ncore in jobCounting[site][schedd][jType].keys():
                    run_jobs = jobCounting[site][schedd][jType][ncore]['Running']
                    pen_jobs = jobCounting[site][schedd][jType][ncore]['Pending']
                    # Add to total by sites
                    totalBySite[site][jType]['Running'] += run_jobs
                    totalBySite[site][jType]['Pending'] += pen_jobs
                    totalBySite[site]['Running'] += run_jobs
                    totalBySite[site]['Pending'] += pen_jobs
                    # Add to total by servers
                    totalByServer[schedd][jType]['Running'] += run_jobs
                    totalByServer[schedd][jType]['Pending'] += pen_jobs
                    totalByServer[schedd]['Running'] += run_jobs
                    totalByServer[schedd]['Pending'] += pen_jobs
                    # Add to total by Task
                    totalByTask[jType]['Running'] += run_jobs
                    totalByTask[jType]['Pending'] += pen_jobs
                    # Add to total jobs
                    totalJobs['Running'] += run_jobs
                    totalJobs['Pending'] += pen_jobs

    return totalBySite, totalByServer, totalByTask, totalJobs


def createReports(currTime):
    """
    1. Prints a report for the given dictionary
    2. Creates a text file to feed each column in SSB Running/Pending view
    3. Creates the output json file to feed SSB historical view
    4. Creates workflow overview json
    """
    date = currTime.split('h')[0]
    hour = currTime.split('h')[1]

    totalBySite, totalByServer, totalByTask, totalJobs = getJobsOveralls()

    sites = totalBySite.keys()
    servers = totalByServer.keys()
    sites.sort()
    servers.sort()

    # Init output files (txt)
    for status in ['Running', 'Pending']:
        for jType in jobTypes:
            with open('./' + status + jType + '.txt', 'w+') as f:
                pass
        with open('./' + status + "Total" + '.txt', 'w+') as f:
            pass

    for status in ['Running', 'Pending']:

        # Print header report to stdout
        title_line = "| %25s |" % status
        aux_line = "| %25s |" % ('-' * 25)
        for jType in jobTypes:
            title_line += " %10s |" % jType
            aux_line += " %10s |" % ('-' * 10)
        title_line += " %10s |" % 'Total'
        aux_line += " %10s |" % ('-' * 10)
        print aux_line, '\n', title_line, '\n', aux_line

        # Fill output files with all the SITES info. Also print out reports to stdout
        for site in sites:
            site_line = "| %25s |" % site
            for jType in jobTypes:
                typeJobs = int(totalBySite[site][jType][status])
                site_line += " %10s |" % typeJobs

                with open('./' + status + jType + '.txt', 'a') as f:
                    f.write("%s %s\t%s\t%s\t%s\t%s%s\n" % (date, hour, site, str(typeJobs), 'green', site_link, site))
            siteJobs = int(totalBySite[site][status])
            site_line += " %10s |" % siteJobs

            with open('./' + status + "Total" + '.txt', 'a') as f:
                f.write("%s %s\t%s\t%s\t%s\t%s%s\n" % (date, hour, site, str(siteJobs), 'green', site_link, site))
            print site_line

        overalls_line = "| %25s |" % 'Overalls'
        for jType in jobTypes:
            totalTypeJobs = int(totalByTask[jType][status])
            overalls_line += " %10s |" % totalTypeJobs

            with open('./' + status + jType + '.txt', 'a') as f:
                f.write("%s %s\t%s%s\t%s\t%s\t%s%s\n" % (date, hour, 'Overall', 'Sites',
                                                         str(totalTypeJobs), 'green', overalls_link, 'T3210'))

        totalJobsTable = int(totalJobs[status])
        overalls_line += " %10s |" % totalJobsTable
        with open('./' + status + "Total" + '.txt', 'a') as f:
            f.write("%s %s\t%s%s\t%s\t%s\t%s%s\n" % (date, hour, 'Overall', 'Sites',
                                                     str(totalJobsTable), 'green', overalls_link, 'T3210'))
        print aux_line, '\n', overalls_line, '\n', aux_line, '\n'

        # Fill output files with all the SERVERS info. Also print out reports to stdout
        print aux_line, '\n', title_line, '\n', aux_line
        for server in servers:
            site_line = "| %25s |" % server
            for jType in jobTypes:
                typeJobs = int(totalByServer[server][jType][status])
                site_line += " %10s |" % typeJobs

                with open('./' + status + jType + '.txt', 'a') as f:
                    f.write("%s %s\t%s\t%s\t%s\t%s%s%s\n" % (date, hour, server, str(typeJobs),
                                                             'green', site_link, server, '&server'))

            siteJobs = int(totalByServer[server][status])
            site_line += " %10s |" % siteJobs

            with open('./' + status + "Total" + '.txt', 'a') as f:
                f.write("%s %s\t%s\t%s\t%s\t%s%s%s\n" % (date, hour, server, str(siteJobs),
                                                         'green', site_link, server, '&server'))

            print site_line

        overalls_line = "| %25s |" % 'Overalls'
        for jType in jobTypes:
            totalTypeJobs = int(totalByTask[jType][status])
            overalls_line += " %10s |" % totalTypeJobs

            with open('./' + status + jType + '.txt', 'a') as f:
                f.write("%s %s\t%s%s\t%s\t%s\t%s%s\n" % (date, hour, 'Overall', 'Servers',
                                                         str(totalTypeJobs), 'green', overalls_link, 'Servers'))

        totalJobsTable = int(totalJobs[status])
        overalls_line += " %10s |" % totalJobsTable
        with open('./' + status + "Total" + '.txt', 'a') as f:
            f.write("%s %s\t%s%s\t%s\t%s\t%s%s\n" % (date, hour, 'Overall', 'Servers',
                                                     str(totalJobsTable), 'green', overalls_link, 'Servers'))
        print aux_line, '\n', overalls_line, '\n', aux_line, '\n'

    # Create output json file
    jsonCounting = {"UPDATE": {"TimeDate": currTime}, "Sites": []}
    for site in jobCounting.keys():
        siteInfo = {}
        siteInfo["Site"] = site
        siteInfo["Running"] = 0.0
        siteInfo["Pending"] = 0.0
        siteInfo["Servers"] = []
        for server in jobCounting[site].keys():
            serverInfo = {}
            serverInfo["Server"] = server
            serverInfo["Running"] = 0.0
            serverInfo["Pending"] = 0.0
            serverInfo["Types"] = []
            for jType in jobCounting[site][server].keys():
                typeInfo = {}
                typeInfo["Type"] = jType
                typeInfo["Running"] = 0.0
                typeInfo["Pending"] = 0.0
                typeInfo["NCores"] = []
                for core in jobCounting[site][server][jType].keys():
                    coreInfo = {}
                    coreInfo["Cores"] = core
                    coreInfo["Running"] = jobCounting[site][server][jType][core]["Running"]
                    coreInfo["Pending"] = jobCounting[site][server][jType][core]["Pending"]

                    typeInfo["NCores"].append(coreInfo)
                    typeInfo["Running"] += coreInfo["Running"]
                    typeInfo["Pending"] += coreInfo["Pending"]

                serverInfo["Types"].append(typeInfo)
                serverInfo["Running"] += typeInfo["Running"]
                serverInfo["Pending"] += typeInfo["Pending"]

            siteInfo["Servers"].append(serverInfo)
            siteInfo["Running"] += serverInfo["Running"]
            siteInfo["Pending"] += serverInfo["Pending"]

        jsonCounting["Sites"].append(siteInfo)

    # Write the output json
    with open(output_json, 'w+') as jsonfileJobs:
        jsonfileJobs.write(json.dumps(jsonCounting, sort_keys=True, indent=9))

    # Creates json file for jobs per workflow
    with open(json_name_workflows, 'w+') as jsonfileWorkflows:
        jsonfileWorkflows.write(json.dumps(overview_workflows, default=set_default, sort_keys=True, indent=4))


def send_mail(send_from, send_to, subject, text, files=[], server="localhost"):
    """
    Method to send emails
    """
    assert isinstance(send_to, list)    
    assert isinstance(files, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(f, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


def set_default(obj):
    """
    JSON enconder doesn't support sets, parse them to lists
    """
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def get_int(value):
    """ Return integer from a Classad, which can be either ExprTree or int """
    try:
        dummyOut = int(value)
        return dummyOut
    except TypeError:
        return int(value.eval())

def get_str(value):
    """Return string from a Classad, which can be either ExprTree or str"""
    try:
        dummyOut = str(value)
        return dummyOut
    except TypeError:
        return str(value.eval())

def sendFailedJobsMail(mailingType, failedJobs):
    body_text = 'A job type is unknown for the JobCounter script\n'
    body_text += 'Please have a look to the following jobs:\n\n %s' % str(failedJobs)
    send_mail(mailingSender,
              mailingList[mailingType],
              '[Condor Monitoring] Failed task type logic problem',
              body_text)
    print 'ERROR: I find jobs that failed the type assignment logic, I will send an email to: %s' % str(mailingList[mailingType])

def main():
    """
    Main algorithm
    """
    starttime = datetime.now()
    print 'INFO: Script started on: ', starttime
    dummyJobCounter = 0
    # get time (date and hour)
    currTime = time.strftime("%Y-%m-%dh%H:%M:%S")

    # Create base dictionaries for running/pending jobs per site
    createSiteList()  # Sites from SSB
    getSiteCapacity()  # Get cpu capacity by site from SSB
    initJobDictonaries()  # Init running/pending dictionaries
    classAds = {'prodschedd': ['ClusterID', 'ProcId', 'JobStatus', 'CMS_JobType', 'WMAgent_SubTaskName', 'RequestCpus', 'DESIRED_Sites', 'MachineAttrGLIDEIN_CMSSite0'],
                'tier0schedd': ['ClusterID', 'ProcId', 'JobStatus', 'CMS_JobType', 'WMAgent_SubTaskName', 'RequestCpus', 'DESIRED_Sites', '    MachineAttrGLIDEIN_CMSSite0'],
                'crabschedd': ['ClusterID', 'ProcId', 'JobStatus', 'TaskType', 'CRAB_UserHN', 'CRAB_ReqName', 'RequestCpus', 'DESIRED_Sites', 'MATCH_GLIDEIN_CMSSite']}
    jobKeys = {'prodschedd': {'taskname': 'WMAgent_SubTaskName', 'sitename': 'MachineAttrGLIDEIN_CMSSite0'},
               'tier0schedd': {'taskname': 'WMAgent_SubTaskName', 'sitename': 'MachineAttrGLIDEIN_CMSSite0'},
               'crabschedd': {'taskname': 'CRAB_ReqName', 'sitename': 'MATCH_GLIDEIN_CMSSite'}}
    # Going through each collector and process a job list for each scheduler
    all_collectors = global_pool + tier0_pool
    for collector_name in all_collectors:

        print "INFO: Querying collector %s" % collector_name

        schedds = {}

        collector = condor.Collector(collector_name)
        scheddAds = collector.query(condor.AdTypes.Schedd, 'true', ['Name', 'MyAddress', 'ScheddIpAddr', 'CMSGWMS_Type'])
        for ad in scheddAds:
            schedds[ad['Name']] = dict(schedd_type=ad.get('CMSGWMS_Type', ''),
                                       schedd_ad=ad)

        print "DEBUG: Schedulers ", schedds.keys()

        for schedd_name in schedds:

            schedd_type = schedds[schedd_name]['schedd_type']
            if schedd_type not in ['prodschedd', 'crabschedd', 'tier0schedd']:
                print 'Skipping this scheduler: %s as its type is not prodschedd or tier0schedd or crabschedd' % schedd_name
                continue

            print "INFO: Getting jobs from collector: %s scheduler: %s" % (collector_name, schedd_name)

            schedd_ad = schedds[schedd_name]['schedd_ad']
            schedd = condor.Schedd(schedd_ad)
            try:
                jobs = schedd.xquery('true', classAds[schedd_type])
            except RuntimeError:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                print "ERROR: Failed to query schedd %s with:\n" % schedd_name
                print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            try:
                for job in jobs:
                    try:
                        jobId = str(job['ClusterID']) + '.' + str(job['ProcId'])
                        status = int(job['JobStatus'])

                        if jobKeys[schedd_type]['taskname'] not in job:
                            # CRAB has a lot of these which are not tagged and they are task processes.
                            # They should not be accounted for anything.
                            #print 'I found a job coming from %s, but it does not have needed classads. JobID %s' % (schedd_type, jobId)
                            continue
                        workflow = None
                        task = None
                        if schedd_type == 'prodschedd' or schedd_type == 'tier0schedd':
                            workflow = job['WMAgent_SubTaskName'].split('/')[1]
                            task = job['WMAgent_SubTaskName'].split('/')[-1]
                            jType = job['CMS_JobType']
                        elif schedd_type == 'crabschedd':
                            if 'TaskType' not in job:
                                # Crab is now running dummy jobs on scheduler which is monitoring whole task...
                                # These jobs do not have TaskType defined.
                                continue
                            workflow = re.sub('[:]', '', job['CRAB_UserHN'])
                            task = job['CRAB_ReqName']
                            jType = 'Crab3'
                            if job['TaskType'] == 'ROOT':
                                # Means it is a ROOT task, which is running only on scheduler.
                                # and it should not be accounted.
                                continue
                        else:
                            print 'How did you got here?! Are you developing something?'
                            raise
                        try:  # it can be an ExprTree
                            cpus = get_int(job['RequestCpus'])
                        except:
                            # Catch any except in case something in the future would change in HTCondor or how ExprTree is evaluated.
                            # It is not correct to assume it is 1 cpu. Skip this job.
                            print 'Failed to extract RequestCpus from this job %s' % job
                            continue
                        siteToExtract = []
                        if 'DESIRED_Sites' in job:
                            try:
                                siteToExtract = [site for site in job['DESIRED_Sites'].replace(' ', '').split(",") if site]
                            except KeyError as er:
                                siteToExtract = []
                                errMsg = 'Received KeyError %s. Job classads: %s' % (er, job)
                                print errMsg
                                if jobId:
                                    jobs_failedTypeLogic[schedd_type][jobId] = dict(scheduler=schedd_name, BaseType=job, err=errMsg)
                                else:
                                    # This should not happen, but just in case...
                                    jobs_failedTypeLogic[schedd_type][dummyJobCounter] = dict(scheduler=schedd_name, BaseType=job, err=errMsg)
                                    dummyJobCounter += 1
                            except AttributeError as er:
                                errMsg = 'AttributeError %s with DESIRED_Sites with job: %s' % (er, job)
                                print errMsg
                                if jobId:
                                    jobs_failedTypeLogic[schedd_type][jobId] = dict(scheduler=schedd_name, BaseType=job, err=errMsg)
                                else:
                                    # This should not happen, but just in case...
                                    jobs_failedTypeLogic[schedd_type][dummyJobCounter] = dict(scheduler=schedd_name, BaseType=job, err=errMsg)
                                    dummyJobCounter += 1
                                siteToExtract = [site for site in get_str(job['DESIRED_Sites']).replace(' ', '').split(",") if site]
                        if not siteToExtract:
                            # There are some cases in CRAB, which it makes to have zombie jobs without any DESIRED_Sites.
                            # See here: https://github.com/dmwm/CRABServer/issues/4933
                            # Skip it as it will not be able to run anywhere..
                            siteToExtract = ['NoSiteDefined']

                        if schedd_name in relvalAgents:  # If RelVal job
                            jType = 'RelVal'
                        elif task == 'Reco':  # If PromptReco job (Otherwise type is Processing)
                            jType = 'Reco'
                        elif jType not in jobTypes:  # If job type is not standard
                            jType = jobType(jobId, schedd_name, task, schedd_type)

                        siteRunning = job.get(jobKeys[schedd_type]['sitename'], '')
                        if siteName(siteRunning) and status == 2:  # If job is currently running
                            increaseRunning(siteRunning, schedd_name, jType, cpus)
                            increaseRunningWorkflow(workflow, siteRunning, 1)
                        elif status == 1:  # Pending
                            for penSite in siteToExtract:
                                sitename = penSite.replace('_Disk', '')
                                pendingCache.setdefault(schedd_name, {}).setdefault(jType, {}).setdefault(cpus, {}).setdefault(sitename, 0)
                                pendingCache[schedd_name][jType][cpus][sitename] += 1
                                if sitename not in pendingSites:
                                    pendingSites.append(sitename)
                            increasePendingWorkflow(workflow, siteToExtract, 1)
                        else:  # Ignore jobs in another state
                            continue
                    except KeyError as er:
                        errMsg = 'Received KeyError %s. Job classads: %s' % (er, job)
                        print errMsg
                        if jobId:
                            jobs_failedTypeLogic[schedd_type][jobId] = dict(scheduler=schedd_name, BaseType=job, err=errMsg)
                        else:
                            # This should not happen, but just in case...
                            jobs_failedTypeLogic[schedd_type][dummyJobCounter] = dict(scheduler=schedd_name, BaseType=job, err=errMsg)
                            dummyJobCounter += 1
            except RuntimeError as er:
                print 'Received RuntimeError %s. Often means that scheduler is overloaded and not replying' % er
    print "INFO: Querying Schedds for this collector is done"

    # Get total running
    for site in jobCounting.keys():
        totalRunningSite[site] = 0.0
        for schedd in jobCounting[site].keys():
            for jType in jobCounting[site][schedd].keys():
                for ncore in jobCounting[site][schedd][jType].keys():
                    totalRunningSite[site] += jobCounting[site][schedd][jType][ncore]['Running']

    relative, total = relativePending(pendingSites)  # total != 0 always
    for job_schedd, schedditem in pendingCache.items():
        for jType, cpusitem in schedditem.items():
            for cpus, siteitem in cpusitem.items():
                for penSite, penCount in siteitem.items():
                    relative_pending = (relative[penSite] / total) * penCount
                    increasePending(penSite, job_schedd, jType, cpus, relative_pending)

    print "INFO: Smart pending job counting is done \n"

    # Handling jobs that failed task extraction logic
    for key, values in jobs_failedTypeLogic.items():
        if values:
            sendFailedJobsMail(key, values)

    print 'INFO: Creating reports...'
    createReports(currTime)

    print 'INFO: The script has finished after: ', datetime.now() - starttime


if __name__ == "__main__":
    main()
