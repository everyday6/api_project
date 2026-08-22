FROM public.ecr.aws/lambda/python:3.11

COPY requirements-lambda.txt .
RUN pip install --no-cache-dir -r requirements-lambda.txt

COPY src/common ${LAMBDA_TASK_ROOT}/src/common
COPY src/serving ${LAMBDA_TASK_ROOT}/src/serving

CMD ["src.serving.lambda_handler.handler"]
