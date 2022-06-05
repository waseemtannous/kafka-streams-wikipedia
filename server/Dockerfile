FROM ubuntu

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get -y install python3-pip

COPY ./requirements.txt /server/requirements.txt

WORKDIR /server

RUN pip3 install -r requirements.txt
COPY . /server

CMD [ "python3", "server.py" ]
