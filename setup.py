import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="collegebaseball",
    version="0.1.0",
    url="https://github.com/nathanblumenfeld/collegebaseball",
    author="Nathan Blumenfeld",
    author_email="nathanblumenfeld@gmail.com",
    description="A college baseball analysis package for Python. Includes functionality for data acquisition and calculation of advanced metrics.",
    long_description_content_type="text/markdown",  # README.md is of type 'markdown
    packages=setuptools.find_packages(),
    install_requires=["pandas", "numpy", "requests", "lxml", "bs4"],
    keywords=["baseball", "ncaa", "ncaa_baseball", "college_baseball", "college_sports"], #descriptive meta-data
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    include_package_data=True,
    package_data={'': ['data/*.parquet']},
)
