
import os

from setuptools import setup


def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r') as f:
        return f.read()


setup(
    name='esdm-pav-client',
    version='1.4.0',
    description='Python API and Client for the ESDM PAV runtime',
    long_description=(read('README.md') + '\n\n'),
    url='https://github.com/OphidiaBigData/esdm-pav-client',
    license='GPLv3+',
    packages=['esdm_pav_client'],
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering',
        #'Private :: Do Not Upload',
    ],
    install_requires=[
        'pyophidia',
        'click',
        'graphviz==0.14'
    ],
    entry_points  = {
        'console_scripts': [
            'esdm-pav-client = esdm_pav_client.cli.client:run',
        ],
    },
    zip_safe=False,
)
