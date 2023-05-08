from setuptools import setup


with open("README.md", 'r') as f:
    long_description = f.read()
    
setup(
  name = 'eniris',
  packages = ['eniris'],
  version = '0.5.0',
  description = 'Eniris API driver for Python',
  license='MIT',
  long_description=long_description,
  long_description_content_type="text/markdown",
  author = 'Enris BV',
  author_email = 'info@eniris.be',
  url = 'https://github.com/eniris-international/eniris-api-python-driver',
  download_url = 'https://github.com/eniris-international/eniris-api-python-driver/archive/refs/tags/v0.5.0.tar.gz',
  keywords = ['eniris', 'api', 'rest'],   # Keywords that define your package best
  install_requires=[
    'requests',
  ],
  classifiers=[
    'Development Status :: 4 - Beta',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3',
  ],
)