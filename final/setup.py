import setuptools

setuptools.setup(
    name="final_finch",
    packages=setuptools.find_packages(exclude=["final_finch_tests"]),
    install_requires=[
        "dagster==0.14.15",
        "dagit==0.14.15",
        "pytest",
    ],
)
