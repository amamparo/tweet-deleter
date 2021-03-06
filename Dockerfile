FROM amazon/aws-lambda-python:3.8

COPY Pipfile .
RUN pip install --upgrade pip
RUN pip install pipenv
RUN pipenv lock --clear --requirements > requirements.txt
RUN pip install -r requirements.txt
COPY src/ ${LAMBDA_TASK_ROOT}/src