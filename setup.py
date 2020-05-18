from setuptools import setup, find_packages

PKG_NAME = "xoto3"
about: dict = dict()
exec(open(f"{PKG_NAME}/__about__.py").read(), about)

setup(
    name=PKG_NAME,
    version=about["__version__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    description="High level utilities for a subset of boto3 operations common for AWS serverless development in Python.",
    packages=find_packages(),
    package_data={"": ["py.typed"]},
    python_requires=">=3.6",
    install_requires=[
        "boto3 >= 1.9",
        "typing-extensions >= 3.7",
    ],
    # it is important to keep these install_requires basically in sync with the Pipfile as well.
)
