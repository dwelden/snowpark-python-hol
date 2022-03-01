## Citibike Machine Learning Hands-on-Lab with Snowpark Python  

### Requirements:  
-Install [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/macos.html)  
-Activate your Snowflake account for [Snowpark Private Preview access](https://docs.google.com/forms/d/e/1FAIpQLSf0-ns-PQxjYJLCeSDybZyZeGtcab7NNNdU27IssZdRTEJ3Tg/viewform)  
-Complete the Snowflake SE [Python Training](https://snowflake.udemy.com/learning-paths/2001710/) and/or the Test-Out Option at the bottom.  
  
    

### Setup Steps:
!!! NOTE if you have an existing conda environment named `snowpark_030` it will be over-written.

If you have not yet setup oauth with your github account you may be asked for a username and password during the clone step below.  Use your snowflake github account name (ie. sf-gh-xxxxxxx) and the [Personal Access Token](https://github.com/settings/tokens) with repo level access to the `snowflakecorp` organization which you should have created as part of the pre-requisite steps.  
  
```bash
git clone https://github.com/snowflakecorp/TKO22-breakouts
cd TKO22-breakouts/data_science
conda env create -f ./conda-env.yml  --force
conda activate snowpark_030
jupyter notebook
```
  
In Jupyter file browser open `creds.json` and update with your Snowflake account credentials.

### Resources
Following are links to some tools and resources you saw in the hands-on-lab:  
  
- [Amazon SageMaker Studio Lab](https://studiolab.sagemaker.aws/) is powerful development platform for AI and ML.  Studio Lab was released recently as a slim-down version with all the basics you need for giving good demos (including GPUs), none of the complexity of its bigger brother... and its free.  Sign-up with your personal email if you want or use your Snowflake email for a quicker response.  
- [Hex](https://app.hex.tech/snowflake) is a feature-rich, notebook-based tool which combines both Python and SQL. Register with your snowflake email address.   
- [Dataiku](https://design.snowflakepoc.amer-aws.dataiku-dss.io/): Dataiku is a data science and ML platform covering much of the end-to-end workflow. They have provided a POC environment with Snowpark libraries pre-installed.   
- [Astronomer](https://www.astronomer.io/) is the leading developer and contributor to Apache Airflow.  They provided a leading solution for cloud-based, managed, enterprise-grade orchestration. The [Astronomer CLI](https://github.com/astronomer/astro-cli) can be used to build Airflow DAGs locally and run them via docker-compose, as well as to deploy those DAGs to Astronomer-managed Airflow clusters and interact with the Astronomer API in general.  
- [Anaconda](https://anaconda.org/) (previously Continuum) has been the driving force behind much of the Python community for many years.  Today they provide an ML platform as well as leading the communit-based development of conda, miniconda, Jupyter, Dask and many other frameworks.   
- An initial start on a [Lucid Chart](https://lucid.app/lucidchart/3ce345ba-ef43-4bb2-b5f3-f7c538593e60/edit?invitationId=inv_aed1406d-604a-4079-bdbd-740d2d5df95e&page=0_0#) is also available for you to build onto.  
