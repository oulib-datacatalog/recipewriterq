#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='recipewriterq',
      version='0.0',
      packages= find_packages(),
      install_requires=[
          'celery==3.1.22',
          'bagit==1.5.4',
          'lxml>=3.5.0',
          'requests==2.13.0',
      ],
)
