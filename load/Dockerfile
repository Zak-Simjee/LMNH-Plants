FROM public.ecr.aws/lambda/python:3.8
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
COPY load.py ${LAMBDA_TASK_ROOT}
CMD [ "load.handler" ]