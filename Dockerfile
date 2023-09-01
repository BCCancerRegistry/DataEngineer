FROM debian:buster
RUN apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /conda/envs
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-py310_23.5.2-0-Linux-x86_64.sh
ENV PATH=$PATH:/root/miniconda3/condabin:/root/miniconda3/bin
RUN bash Miniconda3-py310_23.5.2-0-Linux-x86_64.sh -bf
RUN mkdir -p ~/airflow 
ENV AIRFLOW_HOME=~/airflow 
ENV AIRFLOW_VERSION=2.6.3 
ENV PYTHON_VERSION=3.10
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
EXPOSE 8080:8080
ENTRYPOINT ["airflow","standalone"]
		