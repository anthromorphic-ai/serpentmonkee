from distutils.core import setup
setup(
    name='serpentmonkee',  # How you named your package folder (MyLib)
    packages=['serpentmonkee'],  # Chose the same as "name"
    version='2.99',  # Start with a small number and increase it with every change you make
    # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    license='MIT',
    # Give a short description about your library
    description='works with neo4j x',
    author='author',  # Type in your name
    author_email='serpentmonkee@monki.ii',  # Type in your E-Mail
    # Provide either the link to your github or to your website
    url='https://github.com/anthromorphic-ai/serpentmonkee',
    # I explain this later on
    download_url='https://github.com/anthromorphic-ai/serpentmonkee/archive/2.99.tar.gz',
    # Keywords that define your package best
    keywords=['SOME', 'MEANINGFULL', 'KEYWORDS'],
    install_requires=['redis', 'neo4j==4.2.1',
                      'requests==2.23.0', 'python-dateutil==2.8.1'],
    classifiers=[
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Development Status :: 3 - Alpha',
        # Define that your audience are developers
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',  # Again, pick a license
        # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
