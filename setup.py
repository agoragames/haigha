from distutils.core import setup
import haigha
import os


def is_package(path):
    return (
        os.path.isdir(path) and
        os.path.isfile(os.path.join(path, '__init__.py'))
        )

def find_packages(path, base="" ):
    """ Find all packages in path """
    packages = {}
    for item in os.listdir(path):
        print item
        dir = os.path.join(path, item)
        print dir
        if is_package( dir ):
            print "Adding packages"
            if base:
                module_name = "%(base)s.%(item)s" % vars()
            else:
                module_name = item
            packages[module_name] = dir
            packages.update(find_packages(dir, module_name))
    return packages

packages = find_packages('.')
print packages.keys()

setup(
    name='Haigha',
    version=haigha.VERSION,
#    package_dir = packages,
    packages = packages.keys(),
    license="MIT License",
    long_description=open('README.rst').read(),
)
