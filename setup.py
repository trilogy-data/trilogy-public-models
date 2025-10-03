import setuptools

with open("requirements.txt", "r") as f:
    install_requires = [line.strip().replace("==", ">=") for line in f.readlines()]

setuptools.setup(
    url="",
    author="",
    author_email="preql-community@gmail.com",
    packages=setuptools.find_packages(
        exclude=[
            "dist",
            "build",
            "*.tests",
            "*.tests.*",
            "tests.*",
            "tests",
            "docs",
            ".github",
            "",
            "examples",
        ]
    ),
    package_data={
        "": ["*.preql", "py.typed"],
    },
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
