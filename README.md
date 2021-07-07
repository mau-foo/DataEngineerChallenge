# DataEngineerChallenge

This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

## Tools Used:
- Spark (Scala)
- JupyterHub (IDE)

## How to run or view results

View results

1. Notebook located within the jupyter folder ([Notebook Link](https://github.com/mau-foo/DataEngineerChallenge/blob/master/Jupyter/PayPay%20Challenge.ipynb))
2. Nbviewer if notebook is not rendering ([Notebook Link](https://nbviewer.jupyter.org/github/mau-foo/DataEngineerChallenge/blob/master/Jupyter/PayPay%20Challenge.ipynb))

Run Scala script

1. Install Spark if not already installed
2. Download script folder from repository
3. Either open terminal in folder or navigate to folder from terminal (use cd to navigate to folder)
4. Run this command: spark-shell -i PayPay_Challenge.scala
5. Once script is finished, you can use :q to close the spark shell

Run notebook

1. Install JupyterHub (need to have Python installed)
2. Install Spark
3. Install Scala kernel to run Spark in notebook
4. Run all code in cells in notebook

