#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='recipewriterq',
      version='0.3.4',
      packages= find_packages(),
      install_requires=[
          'celery==3.1.22',
          'bagit==1.5.4',
          'requests==2.20.0',
          'PyYAML==3.12',
          'boto3',
          'lxml',
      ],
)
