
# tap-redshift
import re
from os import path
from setuptools import setup, find_packages


def read(*paths):
    filename = path.join(path.abspath(path.dirname(__file__)), *paths)
    with open(filename) as f:
        return f.read()


def find_version(*paths):
    contents = read(*paths)
    match = re.search(r'^__version__ = [\'"]([^\'"]+)[\'"]', contents, re.M)
    if not match:
        raise RuntimeError('Unable to find version string.')
    return match.group(1)


setup(
      name='tap-redshift',
      version=find_version('tap_redshift', '__init__.py'),
      description='Singer.io tap for extracting data from Redshift',
      author='ShootProof',
      author_email='',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap-redshift'],
      packages=find_packages(),
      install_requires=[
        'attrs==18.2.0',
        'pendulum==1.2.0',
        'singer-python==5.0.4',
        'backoff==1.3.2',
        'psycopg2-binary==2.8.6',
      ],
      setup_requires=[
        'pytest-runner>=2.11,<3.0a',
      ],
      tests_require=[
        'pytest-mock>=1.6.3',
        'mock>=2.0.0',
        'coverage==4.5.1',
        'doublex>=1.8.4,<2.0a',
        'flake8>=2.6.0,<3.4.1a',
        'pyhamcrest>=1.9.0,<2.0a',
        'pytest>=3.2.3,<4.0a',
      ],
      entry_points={
        'console_scripts': [
            'tap-redshift=tap_redshift:main',
        ],
      },
)
