FROM python:3.7

ADD *.py /

RUN pip install flask requests

ENV FLASK_APP main.py

CMD [ "python", "./main.py" ]
