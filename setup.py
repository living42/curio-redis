from setuptools import setup

setup(
    name='curio-redis',
    author='living42',
    email='qzeqing@gmail.com',
    package=['curio_redis'],
    install_requires=['curio', 'hiredis'],
    classifiers=[
        'Programming Language :: Python :: 3',
    ]
)
