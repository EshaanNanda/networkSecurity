from setuptools import find_packages,setup
from typing import List


def get_requirements()->List[str]:

    requirements_list:List[str]=[]

    try:
        with open("requirements.txt",'r') as file:
            lines=file.readlines()

            for line in lines:
                requirements=line.strip()
                if requirements and requirements!= '-e .':
                    requirements_list.append(requirements)

    except Exception as e:
        print("Requirements.txt file not found",e)

    return requirements_list

setup(
    name='NetworkSecurity',
    version='0.0.1',
    author='Eshaan Nanda',

    
    author_email="eshaannanda04@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements()
)

