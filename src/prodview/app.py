
import os
import re
import ConfigParser

import genshi.template

import rrd

_initialized = None
_loader = None
_cp = None
_view = None

def check_initialized(environ):
    global _initialized
    global _loader
    global _cp
    if not _initialized:
        if 'prodview.templates' in environ:
            _loader = genshi.template.TemplateLoader(environ['prodview.templates'], auto_reload=True)
        else:
            _loader = genshi.template.TemplateLoader('/usr/share/prodview/templates', auto_reload=True)
        tmp_cp = ConfigParser.ConfigParser()
        if 'prodview.config' in environ:
            tmp_cp.read(environ['prodview.config'])
        else:
            tmp_cp.read('/etc/prodview.conf')
        _cp = tmp_cp
        _initialized = True


def static_file_server(fname):
    def static_file_server_internal(environ, start_response):
        for result in serve_static_file(fname, environ, start_response):
            yield result
    return static_file_server_internal


def serve_static_file(fname, environ, start_response):
    static_file = os.path.join(_cp.get(_view, "basedir"), fname)

    try:
        fp = open(static_file, "r")
    except:
        raise
        yield not_found(environ, start_response)
        return

    status = '200 OK'
    headers = [('Content-type', 'application/json'),
               ('Cache-control', 'max-age=60, public')]
    start_response(status, headers)

    while True:
        buffer = fp.read(4096)
        if not buffer:
            break
        yield buffer


_totals_json_re = re.compile(r'^/*json/totals$')
totals_json = static_file_server("totals.json")


_summary_json_re = re.compile(r'^/*json/summary$')
summary_json = static_file_server("summary.json")

_max_used_json_re = re.compile(r'^/*json/maxused$')
max_used = static_file_server("maxused.json")

_site_summary_json_re = re.compile(r'^/*json/site_summary$')
site_summary_json = static_file_server("site_summary.json")


_site_totals_json_re = re.compile(r'^/*json/+([-_A-Za-z0-9]+)/*$')
def site_totals_json(environ, start_response):
    path = environ.get('PATH_INFO', '')
    m = _site_totals_json_re.match(path)
    site = m.groups()[0]
    fname = os.path.join(_cp.get(_view, "basedir"), site, "totals.json")

    for result in serve_static_file(fname, environ, start_response):
        yield result


_site_request_summary_json_re = re.compile(r'^/*json/+(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/summary/*$')
def site_request_summary_json(environ, start_response):
    path = environ.get('PATH_INFO', '')
    m = _site_request_summary_json_re.match(path)
    site = m.groups()[0]
    fname = os.path.join(_cp.get(_view, "basedir"), site, "summary.json")

    for result in serve_static_file(fname, environ, start_response):
        yield result


_request_totals_json_re = re.compile(r'^/*json/+([-_A-Za-z0-9]+)/+totals$')
def request_totals_json(environ, start_response):
    path = environ.get('PATH_INFO', '')
    m = _request_totals_json_re.match(path)
    request = m.groups()[0]
    fname = os.path.join(_cp.get(_view, "basedir"), request, "totals.json")
    
    for result in serve_static_file(fname, environ, start_response):
        yield result


_request_summary_json_re = re.compile(r'^/*json/+([-_A-Za-z0-9]+)/+summary$')
def request_summary_json(environ, start_response):
    path = environ.get('PATH_INFO', '')
    m = _request_summary_json_re.match(path)
    request = m.groups()[0]
    fname = os.path.join(_cp.get(_view, "basedir"), request, "summary.json")

    for result in serve_static_file(fname, environ, start_response):
        yield result


_request_site_summary_json_re = re.compile(r'^/*json/+([-_A-Za-z0-9]+)/+site_summary$')
def request_site_summary_json(environ, start_response):
    path = environ.get('PATH_INFO', '')
    m = _request_site_summary_json_re.match(path)
    request = m.groups()[0]
    fname = os.path.join(_cp.get(_view, "basedir"), request, "site_summary.json")

    for result in serve_static_file(fname, environ, start_response):
        yield result


_request_graph_re = re.compile(r'^/*graphs/+([-_A-Za-z0-9]+)/?(hourly|weekly|daily|monthly|yearly)?/?$')
def request_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)

    path = environ.get('PATH_INFO', '')
    m = _request_graph_re.match(path)
    interval = "daily"
    request = m.groups()[0]
    if m.groups()[1]:
        interval=m.groups()[1]
    return [ rrd.request(_cp.get(_view, "basedir"), interval, request) ]


_request_starvation_graph_re = re.compile(r'^/*graphs/+([-_A-Za-z0-9]+)/starvation/?(hourly|weekly|daily|monthly|yearly)?/?$')
def request_starvation_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)

    path = environ.get('PATH_INFO', '')
    m = _request_starvation_graph_re.match(path)
    interval = "daily"
    request = m.groups()[0]
    if m.groups()[1]:
        interval=m.groups()[1]

    return [ rrd.request_starvation(_cp.get(_view, "basedir"), interval, request) ]

def validate_request(path, request_re):
        m = request_re.match(path)
        grouped = m.groups()
        site = grouped[0]
        request = None if not grouped[1] else grouped[1]
        interval = 'daily' if not grouped[2] else grouped[2]
        return site, request, interval

_request_held_graph_re = re.compile(r'^/*graphs/+(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/?([-_A-Za-z0-9]+)?/held/?(hourly|weekly|daily|monthly|yearly)?/?$')
def request_held_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)
    path = environ.get('PATH_INFO', '')
    site, request, interval = validate_request(path, _request_held_graph_re)
    return [ rrd.request_held(_cp.get(_view, "basedir"), interval, request, site) ]

_request_idle_graph_re = re.compile(r'^/*graphs/+(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/?([-_A-Za-z0-9]+)?/idle/?(hourly|weekly|daily|monthly|yearly)?/?$')
def request_idle_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)
    path = environ.get('PATH_INFO', '')
    site, request, interval = validate_request(path, _request_idle_graph_re)
    return [ rrd.request_idle(_cp.get(_view, "basedir"), interval, request, site) ]

_request_joint_graph_re = re.compile(r'^/*graphs/+(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/?([-_A-Za-z0-9]+)?/joint/?(hourly|weekly|daily|monthly|yearly)?/?$')
def request_joint_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)
    path = environ.get('PATH_INFO', '')
    site, request, interval = validate_request(path, _request_joint_graph_re)
    return [ rrd.request_joint(_cp.get(_view, "basedir"), interval, request, site) ]

_subtask_graph_re = re.compile(r'^/*graphs/+([-_A-Za-z0-9]+)/+([-_A-Za-z0-9]+)/?(hourly|weekly|daily|monthly|yearly)?/?$')
def subtask_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)

    path = environ.get('PATH_INFO', '')
    m = _subtask_graph_re.match(path)
    interval = "daily"
    request = m.groups()[0]
    subtask = m.groups()[1]
    if m.groups()[2]:
        interval=m.groups()[2]

    return [ rrd.subtask(_cp.get(_view, "basedir"), interval, request, subtask) ]


_site_graph_re = re.compile(r'^/*graphs/(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/?(hourly|weekly|daily|monthly|yearly)?/?$')
def site_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)

    path = environ.get('PATH_INFO', '')
    m = _request_graph_re.match(path)
    interval = "daily"
    site = m.groups()[0]
    if m.groups()[1]:
        interval=m.groups()[1]

    return [ rrd.site(_cp.get(_view, "basedir"), interval, site) ]

_site_graph_util_re = re.compile(r'^/*graphs/(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/utilization/?(hourly|weekly|daily|monthly|yearly)?/?$')
def site_graph_util(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)
 
    path = environ.get('PATH_INFO', '')
    m = _site_graph_util_re.match(path)
    interval = "daily"
    site = m.groups()[0]
    if m.groups()[1]:
         interval=m.groups()[1]
 
    return [ rrd.site_util(_cp.get(_view, "basedir"), interval, site) ]

_pilot_graph_re = re.compile(r'^/*graphs/(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/(static|partitionable|full)/?(hourly|weekly|daily|monthly|yearly)?/?$')
def pilot_graph_use(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)
 
    path = environ.get('PATH_INFO', '')
    m = _pilot_graph_re.match(path)
    interval = "daily"
    site = m.groups()[0]
    gType = m.groups()[1]
    if m.groups()[2]:
        interval=m.groups()[2]

    return [ rrd.pilot_graph(_cp.get(_view, "basedir"), interval, site, gType) ]


_request_site_graph_re = re.compile(r'^/*graphs/([-_A-Za-z0-9]+)/(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/?(hourly|weekly|daily|monthly|yearly)?/?$')
def request_site_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)

    path = environ.get('PATH_INFO', '')
    m = _request_site_graph_re.match(path)
    interval = "daily"
    request = m.groups()[0]
    site = m.groups()[1]
    if m.groups()[2]:
        interval=m.groups()[2]

    return [ rrd.request_site(_cp.get(_view, "basedir"), interval, request, site) ]


_summary_graph_re = re.compile(r'^/*graphs/summary/?(hourly|weekly|daily|monthly|yearly)?/?$')
def summary_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)

    path = environ.get('PATH_INFO', '')
    m = _summary_graph_re.match(path)
    interval = "daily"
    if m.groups()[0]:
        interval=m.groups()[0]

    return [ rrd.summary(_cp.get(_view, "basedir"), interval) ]


_request_re = re.compile(r'^/*([-_A-Za-z0-9]+)/?$')
def request(environ, start_response):
    status = '200 OK' # HTTP Status
    headers = [('Content-type', 'text/html'),
              ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)

    path = environ.get('PATH_INFO', '')
    m = _request_re.match(path)
    request = m.groups()[0]

    tmpl = _loader.load('request.html')

    return [tmpl.generate(request=request).render('html', doctype='html')]


_site_re = re.compile(r'^/*(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/?$')
def site(environ, start_response):
    status = '200 OK' # HTTP Status
    headers = [('Content-type', 'text/html'),
              ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)

    path = environ.get('PATH_INFO', '')
    m = _request_re.match(path)
    site = m.groups()[0]

    tmpl = _loader.load('site.html')

    return [tmpl.generate(site=site).render('html', doctype='html')]



def index(environ, start_response):
    status = '200 OK' # HTTP Status
    headers = [('Content-type', 'text/html'),
              ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)
    
    tmpl = _loader.load('index.html')
    
    return [tmpl.generate().render('html', doctype='html')]
    

def not_found(environ, start_response):
    status = '404 Not Found'
    headers = [('Content-type', 'text/html'),
              ('Cache-Control', 'max-age=60, public'),
              ('Location', '/')]
    start_response(status, headers)
    path = environ.get('PATH_INFO', '').lstrip('/')
    return # ["Resource %s not found" % path]


subtask = not_found
subtask_site_graph = not_found


# Add url's here for new pages
urls = [
    (re.compile(r'^/*$'), index),
    (_totals_json_re, totals_json),
    (_summary_json_re, summary_json),
    (_max_used_json_re, max_used),
    (_site_summary_json_re, site_summary_json),
    (_site_totals_json_re, site_totals_json),
    (_site_request_summary_json_re, site_request_summary_json),
    (_request_totals_json_re, request_totals_json),
    (_request_summary_json_re, request_summary_json),
    (_request_site_summary_json_re, request_site_summary_json),
    #(re.compile(r'^graphs/([-_A-Za-z0-9]+)/prio/?$'), request_prio_graph),
    (_site_graph_re, site_graph),
    (_site_graph_util_re, site_graph_util),
    (_summary_graph_re, summary_graph),
    (_request_starvation_graph_re, request_starvation_graph),
    (_request_held_graph_re, request_held_graph),
    (_request_idle_graph_re, request_idle_graph),
    (_request_joint_graph_re, request_joint_graph),
    (_pilot_graph_re, pilot_graph_use),
    (_request_graph_re, request_graph),
    (_request_site_graph_re, request_site_graph),
    (_subtask_graph_re, subtask_graph),
    (re.compile(r'^graphs/([-_A-Za-z0-9]+)/([-_A-Za-z0-9]+)/(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/?$'), subtask_site_graph),
    (_site_re, site),
    (_request_re, request),
    (re.compile(r'^([-_A-Za-z0-9]+)/([-_A-Za-z0-9]+)/?$'), subtask),
]


def application(environ, start_response):
    global _view
    check_initialized(environ)

    path = environ.get('PATH_INFO', '').lstrip('/')
    _view = environ.get('REQUEST_URI', '').split('/')[1]
    for regex, callback in urls:
        match = regex.match(path)
        if match:
            environ['jobview.url_args'] = match.groups()
            return callback(environ, start_response)
    return not_found(environ, start_response)


