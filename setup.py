from setuptools import setup, find_packages


with open("README.md", "r", encoding="utf8") as f:
    long_description = f.read()

VERSION = "0.7.26"
setup(
    name="eniris",
    packages=find_packages(),
    package_data={
        "eniris": ["py.typed"],
    },
    version=VERSION,
    description="Eniris API driver for Python",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Enris BV",
    author_email="info@eniris.be",
    url="https://github.com/eniris-international/eniris-api-python-driver",
    download_url=("https://github.com/eniris-international/eniris-api-python-driver/"
      f"archive/refs/tags/v{VERSION}.tar.gz"),
    keywords=["eniris", "api", "rest"],  # Keywords that define your package best
    install_requires=["requests"],
    classifiers=[
        "Development Status :: 4 - Beta",
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the
        # current state of your package
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
)
