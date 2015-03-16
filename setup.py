
from distutils.core import setup

setup(name='prodview',
      version='0.1',
      description='Simple monitoring page for CMS production',
      author='Brian Bockelman',
      author_email='bbockelm@cse.unl.edu',
      url='https://github.com/bbockelm/prodview',
      packages=['prodview'],
      package_dir = {'': 'src'},
      data_files=[('/etc', ['packaging/prodview.conf']),
                  ('/var/www/wsgi-scripts', ['packaging/prodview.wsgi']),
                  ('/usr/share/prodview/templates', ['templates/index.html', 'templates/request.html']),
                  ('/etc/httpd/conf.d/', ['packaging/prodview-httpd.conf']),
                  ('/etc/cron.d/', ['packaging/prodview.cron'])
                 ],
      scripts=["src/prodview-update"]
     )

