# Data_Engineering_2_Assign_4

ASSIGNMENT

Fetch the Data and Put it into S3
Create an Python script that extracts the Wikipedia view data for a certain day. Implement the following functionalities: 
●	✅  Set Date as the DATE_PARAM variable in your Python script. Set DATE_PARAM to 10/15/2023
●	✅  There is a Pageviews API endpoint to retrieve the 1000 Most viewed articles (it’s called metrics/pageviews/top). Retrieve them for the Pageviews API endpoint across all devices (all-access) for the date set in DATE_PARAM

Find the API endpoint here: https://wikimedia.org/api/rest_v1/

●	✅  Create a local folder called raw-views and save the API response to this folder in a file called raw-views-YYYY-MM-DD, where the YYYY-MM-DD is the DATE_PARAM value in the filename in the YYYY-MM-DD format.
●	✅  Upload the file you created to S3 into your bucket into an object called datalake/raw/raw-views-YYYY-MM-DD.txt
●	Convert the response into a JSON lines formatted file (as we did in the class):
○	✅  Write the file to your computer to data/views/views-YYYY-MM-DD.json
○	✅  Each line must contain the following records: article, views, rank (from the response), and add the date (DATE_PARAM value) and a retrieved_at value (the current local timestamp).
○	✅ The dates must be in UTC
●	✅ Upload the JSON lines file you saved locally to S3 into your bucket into an object called datalake/views/views-YYYY-MM-DD.txt
●	✅ Make sure the script is “idempotent”: So if you re-execute it multiple times for the same date, it won’t fail; it should simply overwrite the local files and re-upload them to S3 (the file and S3 object content’s won’t change).
●	✅ Manually execute your script for every day from 2022-10-15 to 2022-10-21 for every single day (you can do this manually by simply changing the DATE_PARAM value and re-executing your Python file, or by writing a for loop if you are an advanced Python user)

SOLUTION

See file upload
/workspaces/ECBS-5147-Data-Engineering-2-Cloud-Computing/pipeline/Assignement 4 DE2 A Merceron.py
