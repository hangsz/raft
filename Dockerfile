FROM python:3.9.0

WORKDIR /raft
COPY . .

RUN pip install -r requirements.txt

ENV PYTHONPATH "${PATHPATH}:."

CMD ["python", "test/client.py"]