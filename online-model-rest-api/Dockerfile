FROM python:3.8

RUN ln -s /usr/bin/python3 /usr/bin/python & \
    ln -s /usr/bin/pip3 /usr/bin/pip

COPY ./requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt

ENV PATH="/opt/program:${PATH}"
COPY . /opt/program

RUN chmod 755 /opt/program
WORKDIR /opt/program
RUN chmod 755 serve
EXPOSE 8080