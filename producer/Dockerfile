FROM ubuntu

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get -y install python3-pip

COPY ./requirements.txt /producer/requirements.txt

WORKDIR /producer

RUN pip3 install -r requirements.txt
COPY . /producer

CMD [ "python3", "producer.py" ]
