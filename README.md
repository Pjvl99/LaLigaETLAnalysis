# LaLigaETLAnalysis

- This project consists of an ETL for getting la liga historical statistics from wikipedia and paste them inside snowflake in order to analyze the data.

- Diagram:

![Laliga diagram](https://github.com/Pjvl99/LaLigaETLAnalysis/assets/61527863/d91531fe-a659-4ecc-8baf-c18fa72c35cc)

Steps to run this project:

1. Set your own GCP project and create a GCS Bucket.
2. Set your PROJECT_ID, BUCKET and SERVICE_ACCOUNT env variables.
3. Run docker build -t scrapy_job -f CloudRun/Scrapy/Dockerfile
4. Based on your name of your image, run this command: docker tag SOURCE-IMAGE LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY/IMAGE:TAG
5. To push your image to artifact registry, run this command: docker push LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY/IMAGE
6. In order to run each dataflow job, run the following command:
	python get_historical_classification.py
	--job_name='historical_classification'
	--project=$PROJECT_ID
	--runner=DataflowRunner
	--temp_location $BUCKET/temp
	--region=$REGION
	--service_account_email $SERVICE_ACCOUNT
7. Set your own snowflake project.
8. Deploy and run each snowflake file inside snowflake.


1. This is how the output data looks like inside GCS

![image](https://github.com/Pjvl99/LaLigaETLAnalysis/assets/61527863/4f78d26b-064c-4744-99d4-580b7e487b7c)

2. This is how the batch jobs in dataflow looks like:

![Screenshot from 2024-04-09 07-20-55](https://github.com/Pjvl99/LaLigaETLAnalysis/assets/61527863/abeb90db-6163-4363-91ca-9433e2963294)


![Screenshot from 2024-04-09 07-21-14](https://github.com/Pjvl99/LaLigaETLAnalysis/assets/61527863/9354610c-d421-41f8-9b5f-53371aa7f574)

3. This is how the dataflow output looks like insided GCS:

![image](https://github.com/Pjvl99/LaLigaETLAnalysis/assets/61527863/373fd2a8-e251-47e9-ade9-f485634fbd28)

4. This is how the database and data looks like inside Snowflake:

![Screenshot from 2024-04-09 07-41-25](https://github.com/Pjvl99/LaLigaETLAnalysis/assets/61527863/88fd03c1-242c-4737-b7b6-3863cdd73724)


![Screenshot from 2024-04-09 07-41-50](https://github.com/Pjvl99/LaLigaETLAnalysis/assets/61527863/05e9c315-b6b7-4c98-af97-d72c68dd1454)

5. This is the final dashboard inside snowsight:


![Screenshot from 2024-04-09 07-43-36](https://github.com/Pjvl99/LaLigaETLAnalysis/assets/61527863/4da5a76f-b052-4268-ac87-464fec98f3a1)



