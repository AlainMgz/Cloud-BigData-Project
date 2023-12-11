from setuptools import setup, find_packages

setup(
    name='myproject',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'requests>=2.26.0',
        'numpy>=1.21.0',
        'matplotlib<4.0'
    ],
    entry_points={
        'console_scripts': [
            'mycommand = myproject.module:main'
        ]
    },
    author='Your Name',
    author_email='your.email@example.com',
    description='A short description of your project',
    url='https://github.com/yourusername/myproject',
)
