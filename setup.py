###############################################################################
#   REQUIREMENTS:
#   build-essentials
#   python-dev
#   python-setuptools
#   apt-get install build-essentials python-dev python-setuptools
###############################################################################

from setuptools import setup
import beach

setup( name = 'beach',
       version = beach.__version__,
       description = 'Simple private python cloud framework',
       url = 'https://github.com/refractionPOINT/py-beach',
       author = 'refractionPOINT',
       author_email = 'maxime@refractionpoint.com',
       license = 'GPLv2',
       packages = [ 'beach' ],
       zip_safe = False,
       install_requires = [ 'gevent',
                            'pyzmq',
                            'netifaces',
                            'pyyaml',
                            'psutil' ],
       long_description = 'Python private compute cloud framework with a focus on ease of deployment and expansion rather than pure performance.' )