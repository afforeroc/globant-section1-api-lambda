FROM public.ecr.aws/lambda/python:3.10

ARG APP_NAME

RUN pip install --upgrade pip

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY ${APP_NAME}.py ${LAMBDA_TASK_ROOT}