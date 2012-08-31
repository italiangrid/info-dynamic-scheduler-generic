#!/usr/bin/env python

from distutils.core import setup
import ConfigParser

pkg_version = '0.0.0'
try:
    parser = ConfigParser.ConfigParser()
    parser.read('setup.cfg')
    pkg_version = parser.get('global','pkgversion')
except:
    pass

setup(
      name='dynsched-generic',
      version=pkg_version,
      description='Tool calculating information related to grid job scheduling',
      long_description='''This is a program that calculates information related to scheduling
grid jobs at a grid site.  The program takes information (provided in a
site-agnostic format) about the current state of the LRMS and scheduler.
Output is in a format compatible with the GIP of gLite.''',
      license='Apache Software License',
      author='CREAM group',
      author_email='CREAM group <cream-support@lists.infn.it>',
      packages=['DynamicSchedulerGeneric'],
      package_dir = {'': 'src'},
      data_files=[
                  ('usr/libexec', ['src/lcg-info-dynamic-scheduler'])
                 ]
     )


