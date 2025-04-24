import setuptools

setuptools.setup(
    name="collegebaseball",
    version="1.3.0-alpha",
    url="https://github.com/nathanblumenfeld/collegebaseball",
    author="Nathan Blumenfeld",
    author_email="nathanblumenfeld@gmail.com",
    description="A college baseball analysis package for Python. Includes functionality for data acquisition and calculation of advanced metrics.",
    long_description=open('DESCRIPTION.rst').read(),
    packages=setuptools.find_packages(),
    install_requires=["pandas", "numpy",
                      "requests", "lxml", "bs4", "tqdm"],
    keywords=["baseball", "ncaa", "ncaa_baseball",
              "college_baseball", "college_sports"],
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6'],
    include_package_data=True)
