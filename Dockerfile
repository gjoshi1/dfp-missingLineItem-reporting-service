FROM python:2.7

RUN apt-get update && apt-get install -y python-pip libmemcached-dev

ADD requirements.txt /code/

RUN pip install -r /code/requirements.txt


ADD . /code
COPY . /code
COPY googleads.yaml /root

RUN chmod 777 /root/googleads.yaml



WORKDIR /code

# Tests
# RUN pip install nose && nosetests . && pip uninstall -y nose
CMD ["uwsgi", "--ini", "/code/server.ini"]




