FROM python:3.7

ADD main.py /

RUN pip install flask

CMD [ "python", "./main.py" ]
