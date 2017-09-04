# Makefile for Mercury data management framework


HOST=127.0.0.1
TEST_PATH=./tests
RECIPEPREFIX= # prefix char is a space, on purpose; do not delete
PHONY=clean
VIRTUALENV_NAME=mercury_test
VIRTUALENV_ROOT=~/.test_virtualenvs


clean:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	find . -name '*~' -exec rm --force {} +


install-deps:
	pip install -r requirements.txt


test_env:
	mkdir -p $(VIRTUALENV_ROOT)
	virtualenv --no-site-packages $(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)
	cd ~/.virtualenv/mercury_test/bin
	python $(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)/bin/activate_this.py
	$(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)/bin/pip install -r requirements.txt


test:	test_env
	PYTHONPATH=./tests $(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)/bin/python -m unittest discover -t . ./tests -v


test_teamcity:	test_env
	PYTHONPATH=./tests $(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)/bin/python -m teamcity.unittestpy discover -t . ./tests -v


build-dist:
	python setup.py sdist bdist_wheel
	mv *.whl dist/


build-testdist:
	python test_setup.py sdist bdist_wheel
	mv *.whl dist/


clean-dist:
	rm -rf dist/*
	rm -rf build/*


pypi-upload:
	twine upload -r dist/*
