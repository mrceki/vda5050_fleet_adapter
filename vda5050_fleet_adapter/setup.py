from setuptools import setup

package_name = 'vda5050_fleet_adapter'

setup(
    name=package_name,
    version='0.0.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name, ['config.yaml']),

    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Cenk',
    maintainer_email='cenkcetix@gmail.com',
    description='An experimental fleet adapter for VDA5050',
    license='Apache License 2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'fleet_adapter=vda5050_fleet_adapter.fleet_adapter:main'
        ],
    },
)
