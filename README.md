# crypto-tools
Python tools for cryptocurrencies

---
## User Guide
### User Setup
This package currently supports Python versions 3.9 and above

It is recommended that you use the latest versions pip, poetry, and tox. These can be installed on an Ubuntu system as follows:

`python3 -m pip install --user --upgrade pip poetry tox`

### User Installation
To install this package, then run:

`python3 -m pip install --user --upgrade git+https://github.com/jameslewellyn/crypto-tools.git`

### Usage
Usage has yet to be defined.

---
## Developer Guide
### Developer Setup
Please follow all steps in the above "User Setup" section. Note that you must have run `tox` before making pull requests.

### Developer Source Installation
Clone down the repo:

`git clone https://github.com/jameslewellyn/crypto-tools.git`

`cd crypto-tools`

Create your development venv:

`poetry udate`

Enter the development environment:

`poetry shell`

### Development Steps
Make changes to code.

Test changes locally.

Validate with tox:

`tox`

Submit pull request.
