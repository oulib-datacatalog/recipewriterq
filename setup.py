#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='recipewriterq',
      version='0.0',
      packages= find_packages(),
      install_requires=[
          'celery==3.1.22',
          'requests==2.9.1',
          'pandas',
      ],
)
