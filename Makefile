# Makefile for Mercury data management framework

HOST=127.0.0.1
TEST_PATH=./tests
PHONY=clean
VIRTUALENV_NAME=mercury_test
VIRTUALENV_ROOT=~/.virtualenvs
IMAGE_VERSION = latest
PROJECT = mercury
COMPOSE = docker-compose -p ${PROJECT}
DOCKER = docker exec -it $(WEB) bash -c
HOST_UID = `id -u`
HOST_GID = `id -g`


# -------------- Basic targets --------------

clean:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	find . -name '*~' -exec rm --force {} +


install-deps:
	pip install -r requirements.txt


test_env:
	mkdir -p $(VIRTUALENV_ROOT)
	virtualenv --no-site-packages $(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)
	cd $(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)/bin
	python $(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)/bin/activate_this.py
	$(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)/bin/pip install -r requirements.txt


test:	test_env
	PYTHONPATH=./tests $(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)/bin/python -m unittest discover -t . ./tests -v


test_teamcity:	test_env
	PYTHONPATH=./tests $(VIRTUALENV_ROOT)/$(VIRTUALENV_NAME)/bin/python -m teamcity.unittestpy discover -t . ./tests -v


build-dist:
	python setup.py sdist bdist_wheel


build-testdist:
	python test_setup.py sdist bdist_wheel


clean-dist:
	rm -rf dist/*
	rm -rf build/*


pypi-upload:
	twine upload -r dist/* --repository pypi


#-------------- Docker-aware build targets --------------


docker-build: FORCE
	docker build -t binarymachines/mercury:${IMAGE_VERSION} -f conf/Dockerfile \
		--build-arg "BINARY_BUILD_VERSION=${IMAGE_VERSION}" .

docker-build-clean: FORCE
	docker build --no-cache -t binarymachines/mercury:${IMAGE_VERSION} -f conf/Dockerfile \
                --build-arg "BINARY_BUILD_VERSION=${IMAGE_VERSION}" .

docker-pull: FORCE
	docker pull binarymachines/mercury:${IMAGE_VERSION}

docker-pull-all: docker-pull FORCE
	docker pull redis
	docker pull couchbase
	docker pull postgres
	docker pull spotify/kafka

docker-push: FORCE
	docker push binarymachines/mercury:${IMAGE_VERSION}

docker-tag-latest: FORCE
	docker tag binarymachines/mercury:${IMAGE_VERSION} binarymachines:latest

docker-test: FORCE
	./docker-test.sh


# ----- Dockerized local development -----

up: FORCE
	${COMPOSE} up -d --no-build mercury

down: FORCE
	${COMPOSE} down --remove-orphans -v

rm: FORCE
	docker-compose rm -f

bounce: down rm up

sh: FORCE
	${COMPOSE} run --rm mercury /bin/sh

lint: FORCE
	${COMPOSE} run --rm mercury /bin/sh -c \
		'flake8 --config=/opt/bamx/test/flake8.ini && \
		echo -e "\n########## code style (flake8) PASSED ##########\n"'

typing: FORCE
	${COMPOSE} run --rm mercury /bin/sh -c \
		'mypy --config-file=/opt/bamx/test/mypy.ini /opt/bamx/src && \
		echo -e "\n########## type check (mypy) PASSED ##########\n"'


test: FORCE
	${COMPOSE} run --rm mercury behave /opt/mercury/src/tests/behave/features

pip-compile: FORCE
	# NOTE: Fix file ownership at the end, instead of running the whole
	# container as the host user/group. Due to an upstream limitation,
	# `pip-compile` needs write access to `/root` for pip caching.
	# https://github.com/jazzband/pip-tools/issues/395
	${COMPOSE} run --rm mercury /bin/sh -c \
		"pip-compile --rebuild --generate-hashes --output-file conf/deps/requirements.txt conf/deps/requirements-unpinned.txt && \
		chown ${HOST_UID}:${HOST_GID} conf/deps/requirements.txt"

clean: FORCE
	${COMPOSE} run --rm mercury find . -name '*.pyc' -delete

FORCE:  # https://www.gnu.org/software/make/manual/html_node/Force-Targets.html#Force-Targets

