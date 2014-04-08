#!/usr/bin/env python

import sys, os, os.path, shlex, subprocess
from subprocess import Popen as execScript
from distutils.core import setup
from distutils.command.bdist_rpm import bdist_rpm as _bdist_rpm

pkg_name = 'dynsched-generic'
pkg_version = '2.5.4'
pkg_release = '1'

source_items = "setup.py src tests"

class bdist_rpm(_bdist_rpm):

    def run(self):

        topdir = os.path.join(os.getcwd(), self.bdist_base, 'rpmbuild')
        builddir = os.path.join(topdir, 'BUILD')
        srcdir = os.path.join(topdir, 'SOURCES')
        specdir = os.path.join(topdir, 'SPECS')
        rpmdir = os.path.join(topdir, 'RPMS')
        srpmdir = os.path.join(topdir, 'SRPMS')
        
        cmdline = "mkdir -p %s %s %s %s %s" % (builddir, srcdir, specdir, rpmdir, srpmdir)
        execScript(shlex.split(cmdline)).communicate()
        
        cmdline = "tar -zcf %s %s" % (os.path.join(srcdir, pkg_name + '.tar.gz'), source_items)
        execScript(shlex.split(cmdline)).communicate()
        
        specOut = open(os.path.join(specdir, pkg_name + '.spec'),'w')
        cmdline = "sed -e 's|@PKGVERSION@|%s|g' -e 's|@PKGRELEASE@|%s|g' project/%s.spec.in" % (pkg_version, pkg_release, pkg_name)
        execScript(shlex.split(cmdline), stdout=specOut, stderr=sys.stderr).communicate()
        specOut.close()
        
        cmdline = "rpmbuild -ba --define '_topdir %s' %s.spec" % (topdir, os.path.join(specdir, pkg_name))
        execScript(shlex.split(cmdline)).communicate()

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
                 ],
      cmdclass={'bdist_rpm': bdist_rpm}
     )


