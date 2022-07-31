# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Modified from:
# http://www.benjack.io/2018/02/02/python-cpp-revisited.html

import os
import sys
import sysconfig
import platform
import subprocess

from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext

class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)

class CMakeBuild(build_ext):
    def run(self):
        try:
            subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError(
                "CMake >= 3.12 must be installed to build the following extensions: " +
                ", ".join(e.name for e in self.extensions))

        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        extdir = os.path.abspath(
            os.path.dirname(self.get_ext_fullpath(ext.name)))
        cmake_args =  ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir]
        cmake_args += ['-DWITH_PYTHON=True']
        cmake_args += ['-DCMAKE_CXX_STANDARD=11']
        # ensure we use a consistent python version
        cmake_args += ['-DPython3_EXECUTABLE=' + sys.executable]
        cfg = 'Debug' if self.debug else 'Release'
        build_args = ['--config', cfg]

        if platform.system() == "Windows":
            cmake_args += ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}'.format(
                           cfg.upper(),
                           extdir)]
            if sys.maxsize > 2**32:
                cmake_args += ['-T', 'host=x64']
                cmake_args += ['-DCMAKE_GENERATOR_PLATFORM=x64']
                build_args += ['--', '/m']
        else:
            cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg]
            build_args += ['--', '-j2']

        env = os.environ.copy()
        env['CXXFLAGS'] = '{} -DVERSION_INFO=\\"{}\\"'.format(
            env.get('CXXFLAGS', ''),
            self.distribution.get_version())
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)
        subprocess.check_call(['cmake', ext.sourcedir] + cmake_args,
                              cwd=self.build_temp, env=env)
        subprocess.check_call(['cmake', '--build', '.', '--target', 'python'] + build_args,
                              cwd=self.build_temp, env=env)
        print() # add an empty line to pretty print

setup(
    name='datasketches',
    version='3.5.0',
    author='Apache Software Foundation',
    author_email='dev@datasketches.apache.org',
    description='The Apache DataSketches Library for Python',
    license='Apache License 2.0',
    url='http://datasketches.apache.org',
    long_description=open('python/README.md').read(),
    long_description_content_type='text/markdown',
    packages=find_packages('python'), # python pacakges only in this dir
    package_dir={'':'python'},
    # may need to add all source paths for sdist packages w/o MANIFEST.in
    ext_modules=[CMakeExtension('datasketches')],
    cmdclass={'build_ext': CMakeBuild},
    install_requires=['numpy'],
    zip_safe=False
)
