from setuptools import setup

setup(
    name='data-service',
    version='0.1',
    packages=['main.resources.tests', 'main.resources.dataservice',
              'main.resources.dataservice.api'],
    install_requires=[
          'PyHDFS','tornado','Tornado-JSON','happybase', 'cm-api', 'futures', 'mock', 'magicmock'
    ],
    url='',
    license='MIT',
    author='venkvisw',
    author_email='',
    description='Data API for managing platform datasets'
)
