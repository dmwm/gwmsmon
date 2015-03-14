
import re
import ConfigParser

import genshi.template

import rrd

_initialized = None
_loader = None
_cp = None

def check_initialized(environ):
    global _initialized
    global _loader
    global _cp
    if not initialized:
        if 'prodview.templates' in environ:
            _loader = TemplateLoader(environ['prodview.templates'], auto_reload=True)
        else:
            loader = TemplateLoader('/usr/share/prodview/templates', auto_reload=True)
        tmp_cp = ConfigParser.ConfigParser()
        if 'prodview.config' in environ:
            tmp_cp.read(environ['prodview.config'])
        else:
            tmp_cp.read('/etc/prodview.conf')
        cp = tmp_cp
        initialized = True


_request_graph_re = re.compile(r'/+graphs/+^([-_A-Za-z0-9]+)/?([a-zA-Z]+)?/?$')
def request_graph(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'image/png'),
               ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)

    path = environ.get('PATH_INFO', '')
    m = _reqeust_graph_re.match(path)
    interval = "daily"
    request = m.groups()[0]
    if m.groups()[1]:
        interval=m.groups()[1]

    return [ rrd.request(cp.get("prodview", "basedir"), interval, request) ]


def index(environ, start_response):
    status = '200 OK' # HTTP Status
    headers = [('Content-type', 'text/html'),
              ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)
    
    tmpl = loader.load('index.html')
    
    site_name = cp.get("jobview", "site_name")

    return [tmpl.generate(site_name=site_name).render('html', doctype='html')]
    

def not_found(environ, start_response):
    status = '404 Not Found'
    headers = [('Content-type', 'text/html'),
              ('Cache-Control', 'max-age=60, public')]
    start_response(status, headers)
    return ["Resource not found"]


request = not_found
subtask = not_found
request_site_graph = not_found
subtask_graph = not_found
subtask_site_graph = not_found


# Add url's here for new pages
urls = [
    (re.compile(r'^$'), index),
    (re.compile(r'^([-_A-Za-z0-9]+)/?$'), request),
    (re.compile(r'^([-_A-Za-z0-9]+)/([-_A-Za-z0-9]+)/?$'), subtask),
    (_request_graph_re, request_graph),
    #(re.compile(r'^graphs/([-_A-Za-z0-9]+)/prio/?$'), request_prio_graph),
    (re.compile(r'^graphs/([-_A-Za-z0-9]+)/(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/?$'), request_site_graph),
    (re.compile(r'^graphs/([-_A-Za-z0-9]+)/([-_A-Za-z0-9]+)/?$'), subtask_graph),
    (re.compile(r'^graphs/([-_A-Za-z0-9]+)/([-_A-Za-z0-9]+)/(T[0-9]_[A-Z]{2,2}_[-_A-Za-z0-9]+)/?$'), subtask_site_graph),
]


def application(environ, start_response):
    check_initialized(environ)

    path = environ.get('PATH_INFO', '').lstrip('/')
    for regex, callback in urls:
        match = regex.match(path)
        if match:
            environ['jobview.url_args'] = match.groups()
            return callback(environ, start_response)
    return not_found(environ, start_response)


