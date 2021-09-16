import setuptools

with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="app",
    version="0.0.1",

    description="KDF shipper demo app",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="author",

    package_dir={"": "app"},
    packages=setuptools.find_packages(where="app"),

    install_requires=[
        "aws-cdk-lib==2.0.0-rc.21",
        "constructs>=10.0.0,<11.0.0",
    ],

    python_requires=">=3.8",

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
