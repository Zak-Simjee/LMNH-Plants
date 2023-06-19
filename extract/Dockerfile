FROM public.ecr.aws/lambda/python:3.7
WORKDIR ${LAMBDA_TASK_ROOT}
#COPY requirem
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY extract.py .
CMD ["extract.handler"]
