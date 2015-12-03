
from distutils.core import setup

setup(name='prodview',
      version='0.5',
      description='Simple monitoring page for CMS production',
      author='Brian Bockelman',
      author_email='bbockelm@cse.unl.edu',
      url='https://github.com/bbockelm/prodview',
      packages=['prodview'],
      package_dir = {'': 'src'},
      data_files=[('/etc/', ['packaging/prodview.conf']),
                  ('/etc/prodview/', ['packaging/cleanup.sh', 'packaging/cleanup-tmp.sh']),
                  ('/var/www/wsgi-scripts/', ['packaging/prodview.wsgi']),
                  ('/usr/share/prodview/templates/', ['templates/views/index.html', 'templates/views/request.html', 'templates/views/site.html']),
                  ('/etc/httpd/conf.d/', ['packaging/prodview-httpd.conf', 'packaging/welcome.conf']),
                  ('/etc/cron.d/', ['packaging/prodview.cron', 'packaging/analysisview.cron', 'packaging/analysiscrab2.cron', 'packaging/totalview.cron', 'packaging/utilization.cron']),
                  ('/var/www/html/', ['templates/index.html', 'templates/css/bootstrap.css', 'templates/css/bootstrap.min.css', 'templates/js/bootstrap.min.js', 'templates/js/jquery.min.js']),
                 ],
      scripts=["src/prodview-update", "src/analysisview-update", "src/analysisviewcrab2-update", "src/totalview-update", "src/utilization"]
     )
