from setuptools import setup

setup_args = dict(
    name='elastictools',
    version='0.0.1',
    description='Useful tools to work with Elastic stack in Python',
    license='MIT',
    packages=['elastictools'],
    author='Thuc Nguyen',
    author_email='gthuc.nguyen@gmail.com',
    keywords=['Elastic', 'ElasticSearch', 'ElasticStack'],
    url='https://github.com/ncthuc/elastictools'
)


setup_args['install_requires'] = [
    'elasticsearch>=6.0.0,<7.0.0'
]

if __name__ == '__main__':
    setup(**setup_args)
