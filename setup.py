from setuptools import setup, find_packages

setup(name='azrael',
      version='0.0.1',
      description='Xyla\'s SnapChat client.',
      url='https://github.com/xyla-io/azrael',
      author='Xyla',
      author_email='gklei89@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
        'pandas',
        'pytest',
        'flask',
        'requests',
        'pyOpenSSL',
        'maya',
      ],
      zip_safe=False)
