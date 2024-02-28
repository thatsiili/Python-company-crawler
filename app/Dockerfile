FROM python:latest
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY app.py app.py
COPY company_handler.py company_handler.py
CMD ["python", "-u", "app.py"]
