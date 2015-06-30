
from distutils.core import setup

setup(name='prodview',
      version='0.3',
      description='Simple monitoring page for CMS production',
      author='Brian Bockelman',
      author_email='bbockelm@cse.unl.edu',
      url='https://github.com/bbockelm/prodview',
      packages=['prodview'],
      package_dir = {'': 'src'},
      data_files=[('/etc/', ['packaging/prodview.conf']),
                  ('/etc/prodview/', ['packaging/cleanup.sh']),
                  ('/var/www/wsgi-scripts/', ['packaging/prodview.wsgi']),
                  ('/usr/share/prodview/templates/', ['templates/views/index.html', 'templates/views/request.html', 'templates/views/site.html']),
                  ('/etc/httpd/conf.d/', ['packaging/prodview-httpd.conf', 'packaging/welcome.conf']),
                  ('/etc/cron.d/', ['packaging/prodview.cron', 'packaging/analysisview.cron', 'packaging/analysiscrab2.cron', 'packaging/totalview.cron']),
                  ('/var/www/html/', ['templates/index.html']),
                 ],
      scripts=["src/prodview-update", "src/analysisview-update", "src/analysisviewcrab2-update", "src/totalview-update"]
     )
