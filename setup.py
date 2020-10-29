from setuptools import find_packages, setup
import os

version = "0.0.1"

readme = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()

req_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
requirements = [i.strip() for i in open(req_file).readlines()]

setup_params = dict(
    name="pycalcite",
    version=version,
    description="SQLAlchemy Dialect for Calcite",
    author="mclovinxie",
    author_email="mclovin.xxh@gmail.com",
    long_description=readme,
    classifiers=[
        "Development Status :: 3 - Alpha",
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Database :: Front-Ends',
    ],
    keywords='SQLAlchemy Dialect for Calcite',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "pycalcite": ["jar/*.jar", "jar/src/*"],
    },
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects":
            ["pycalcite = pycalcite.dialect:PyCalciteDialect"]
    },
    install_requires=requirements
)

if __name__ == '__main__':
    setup(**setup_params)
